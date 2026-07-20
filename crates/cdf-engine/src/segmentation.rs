use std::collections::BTreeMap;

use arrow_array::{
    Array, BinaryViewArray, DictionaryArray, FixedSizeListArray, LargeListArray,
    LargeListViewArray, ListArray, ListViewArray, MapArray, RecordBatch, StringViewArray,
    StructArray, UnionArray,
    types::{
        ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use cdf_kernel::{
    CdfError, CompositePosition, CursorPosition, CursorValue, FileManifest, FilePosition, Result,
    SegmentId, SourcePosition, merge_file_position_evidence,
};
use cdf_memory::MemoryLease;
use cdf_memory::MemorySnapshot;
use serde::{Deserialize, Serialize};

pub const SEGMENTATION_POLICY_VERSION: u16 = 2;
pub const POSITION_ALGEBRA_VERSION: u16 = 1;
pub const SEGMENT_ID_NAMESPACE: &str = "partition-segment-ordinal-v2";

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
    pub fn p3_v2() -> Self {
        Self {
            version: SEGMENTATION_POLICY_VERSION,
            target_rows: 1024 * 1024,
            target_bytes: 32 * 1024 * 1024,
            maximum_rows: 4 * 1024 * 1024,
            maximum_bytes: 64 * 1024 * 1024,
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
    pub logical_bytes: u64,
    pub retained_bytes: u64,
    pub canonical_batch_rows: u32,
    pub canonical_batch_bytes: u64,
    pub(crate) memory_leases: Vec<MemoryLease>,
}

impl CanonicalSegment {
    pub fn into_canonical_batches(self) -> Result<Vec<RecordBatch>> {
        canonicalize_batches(
            self.batches,
            self.canonical_batch_rows,
            self.canonical_batch_bytes,
        )
    }
}

pub struct CanonicalSegmentAssembler {
    policy: CanonicalSegmentationPolicy,
    partition_ordinal: u32,
    next_segment_ordinal: u32,
    batches: Vec<RecordBatch>,
    memory_leases: Vec<MemoryLease>,
    output_position: Option<SourcePosition>,
    rows: u64,
    logical_bytes: u64,
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
            memory_leases: Vec::new(),
            output_position: None,
            rows: 0,
            logical_bytes: 0,
            retained_bytes: 0,
        })
    }

    pub fn push(
        &mut self,
        batch: RecordBatch,
        position: Option<SourcePosition>,
    ) -> Result<Vec<CanonicalSegment>> {
        self.push_inner(batch, position, None)
    }

    pub(crate) fn push_accounted(
        &mut self,
        batch: RecordBatch,
        position: Option<SourcePosition>,
        lease: Option<MemoryLease>,
    ) -> Result<Vec<CanonicalSegment>> {
        self.push_inner(batch, position, lease)
    }

    fn push_inner(
        &mut self,
        mut batch: RecordBatch,
        position: Option<SourcePosition>,
        lease: Option<MemoryLease>,
    ) -> Result<Vec<CanonicalSegment>> {
        let mut emitted = Vec::new();
        let mut joined_position = None;
        let slice_position = position
            .as_ref()
            .filter(|position| position.is_batch_slice_invariant())
            .cloned();
        if self.rows > 0 {
            match (&self.output_position, &position) {
                (Some(left), Some(right)) => match join_positions(left, right)? {
                    PositionJoin::Joined(joined) => joined_position = Some(joined),
                    PositionJoin::Boundary => emitted.push(self.flush()?.unwrap()),
                },
                (None, None) => {}
                _ => emitted.push(self.flush()?.unwrap()),
            }
        }
        if self.rows == 0 {
            self.output_position = position.clone();
        }
        if position.is_some() && batch.num_rows() > 0 {
            let batch_rows = u64::try_from(batch.num_rows())
                .map_err(|_| CdfError::data("canonical segment rows exceed u64"))?;
            let batch_bytes = logical_batch_bytes(&batch)?;
            if self.rows > 0
                && (self.rows.saturating_add(batch_rows) > u64::from(self.policy.target_rows)
                    || self.logical_bytes.saturating_add(batch_bytes) > self.policy.target_bytes)
            {
                emitted.push(self.flush()?.unwrap());
                self.output_position = position.clone();
            } else if let Some(joined) = joined_position {
                self.output_position = Some(joined);
            }
            let oversized = batch_rows > u64::from(self.policy.maximum_rows)
                || batch_bytes > self.policy.maximum_bytes;
            if oversized && slice_position.is_none() {
                // The source position describes the complete input batch and cannot be invented
                // for row slices. Preserve exact authority by emitting one conservative oversized
                // segment rather than rejecting valid data or advancing a fabricated cursor.
                self.push_exact_positioned_batch(batch, batch_bytes, lease.as_ref())?;
                emitted.push(
                    self.flush()?
                        .expect("positioned batch appended one segment"),
                );
                return Ok(emitted);
            }
            if !oversized {
                self.push_chunk(batch, batch_bytes, lease.as_ref())?;
                return Ok(emitted);
            }
        }
        while batch.num_rows() > 0 {
            if self.rows == 0 {
                self.output_position = slice_position.clone();
            }
            let remaining_rows = u64::from(self.policy.target_rows).saturating_sub(self.rows);
            let row_take = usize::try_from(remaining_rows)
                .unwrap_or(usize::MAX)
                .min(batch.num_rows());
            let remaining_bytes = self.policy.target_bytes.saturating_sub(self.logical_bytes);
            let take = largest_prefix_within_bytes(&batch, row_take, remaining_bytes)?;
            if take == 0 {
                if self.rows > 0 {
                    emitted.push(self.flush()?.unwrap());
                    continue;
                }
                let one_row = batch.slice(0, 1);
                let one_row_bytes = logical_batch_bytes(&one_row)?;
                if one_row_bytes > self.policy.maximum_bytes {
                    return Err(CdfError::data(format!(
                        "one canonical row requires {one_row_bytes} logical bytes above the {}-byte policy maximum",
                        self.policy.maximum_bytes
                    )));
                }
                self.push_chunk(one_row, one_row_bytes, lease.as_ref())?;
                batch = batch.slice(1, batch.num_rows() - 1);
                emitted.push(self.flush()?.unwrap());
                continue;
            }
            let chunk = if take == batch.num_rows() {
                let schema = batch.schema();
                std::mem::replace(&mut batch, RecordBatch::new_empty(schema))
            } else {
                let chunk = batch.slice(0, take);
                batch = batch.slice(take, batch.num_rows() - take);
                chunk
            };
            let chunk_bytes = logical_batch_bytes(&chunk)?;
            self.push_chunk(chunk, chunk_bytes, lease.as_ref())?;
            if self.rows >= u64::from(self.policy.target_rows)
                || self.logical_bytes >= self.policy.target_bytes
            {
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
            logical_bytes: std::mem::take(&mut self.logical_bytes),
            retained_bytes: std::mem::take(&mut self.retained_bytes),
            canonical_batch_rows: self.policy.microbatch_maximum_rows,
            canonical_batch_bytes: self.policy.microbatch_maximum_bytes,
            memory_leases: std::mem::take(&mut self.memory_leases),
        }))
    }

    fn push_chunk(
        &mut self,
        chunk: RecordBatch,
        logical_bytes: u64,
        lease: Option<&MemoryLease>,
    ) -> Result<()> {
        self.push_chunk_inner(chunk, logical_bytes, lease, false)
    }

    fn push_exact_positioned_batch(
        &mut self,
        chunk: RecordBatch,
        logical_bytes: u64,
        lease: Option<&MemoryLease>,
    ) -> Result<()> {
        if self.rows != 0 || self.output_position.is_none() {
            return Err(CdfError::internal(
                "exact oversized batch requires an empty positioned segment",
            ));
        }
        self.push_chunk_inner(chunk, logical_bytes, lease, true)
    }

    fn push_chunk_inner(
        &mut self,
        chunk: RecordBatch,
        logical_bytes: u64,
        lease: Option<&MemoryLease>,
        allow_exact_oversize: bool,
    ) -> Result<()> {
        let rows = u64::try_from(chunk.num_rows())
            .map_err(|_| CdfError::data("canonical segment rows exceed u64"))?;
        let retained_bytes = u64::try_from(chunk.get_array_memory_size())
            .map_err(|_| CdfError::data("canonical segment memory exceeds u64"))?;
        self.rows = self
            .rows
            .checked_add(rows)
            .ok_or_else(|| CdfError::data("canonical segment rows overflow"))?;
        self.logical_bytes = self
            .logical_bytes
            .checked_add(logical_bytes)
            .ok_or_else(|| CdfError::data("canonical segment logical bytes overflow"))?;
        if !allow_exact_oversize
            && (self.rows > u64::from(self.policy.maximum_rows)
                || self.logical_bytes > self.policy.maximum_bytes)
        {
            return Err(CdfError::data(
                "canonical segment exceeds the plan row/byte maximum",
            ));
        }
        self.retained_bytes = self.retained_bytes.saturating_add(retained_bytes);
        self.batches.push(chunk);
        if let Some(lease) = lease {
            self.memory_leases.push(lease.clone());
        }
        Ok(())
    }
}

pub(crate) fn canonicalize_batches(
    batches: Vec<RecordBatch>,
    maximum_rows: u32,
    maximum_bytes: u64,
) -> Result<Vec<RecordBatch>> {
    let mut output = Vec::new();
    let mut fragments = Vec::new();
    let mut rows = 0_usize;
    let mut bytes = 0_u64;
    let maximum_rows = usize::try_from(maximum_rows)
        .map_err(|_| CdfError::data("canonical microbatch rows exceed usize"))?;

    for mut batch in batches {
        while batch.num_rows() > 0 {
            let row_capacity = maximum_rows.saturating_sub(rows);
            let byte_capacity = maximum_bytes.saturating_sub(bytes);
            let maximum_take = row_capacity.min(batch.num_rows());
            let take = largest_prefix_within_bytes(&batch, maximum_take, byte_capacity)?;
            if take == 0 {
                if !fragments.is_empty() {
                    output.push(finish_canonical_batch(std::mem::take(&mut fragments))?);
                    rows = 0;
                    bytes = 0;
                    continue;
                }
                let one = batch.slice(0, 1);
                let one_bytes = logical_batch_bytes(&one)?;
                if one_bytes > maximum_bytes {
                    return Err(CdfError::data(format!(
                        "one canonical row requires {one_bytes} logical bytes above the {maximum_bytes}-byte microbatch maximum"
                    )));
                }
                fragments.push(one);
                batch = batch.slice(1, batch.num_rows() - 1);
                output.push(finish_canonical_batch(std::mem::take(&mut fragments))?);
                continue;
            }
            let chunk = if take == batch.num_rows() {
                let schema = batch.schema();
                std::mem::replace(&mut batch, RecordBatch::new_empty(schema))
            } else {
                let chunk = batch.slice(0, take);
                batch = batch.slice(take, batch.num_rows() - take);
                chunk
            };
            rows = rows.saturating_add(take);
            bytes = bytes.saturating_add(logical_batch_bytes(&chunk)?);
            fragments.push(chunk);
            if rows == maximum_rows || bytes == maximum_bytes {
                output.push(finish_canonical_batch(std::mem::take(&mut fragments))?);
                rows = 0;
                bytes = 0;
            }
        }
    }
    if !fragments.is_empty() {
        output.push(finish_canonical_batch(fragments)?);
    }
    Ok(output)
}

fn finish_canonical_batch(mut fragments: Vec<RecordBatch>) -> Result<RecordBatch> {
    if fragments.len() == 1 {
        return Ok(fragments.pop().expect("one canonical fragment"));
    }
    let schema = fragments
        .first()
        .ok_or_else(|| CdfError::internal("canonical microbatch has no fragments"))?
        .schema();
    arrow_select::concat::concat_batches(&schema, &fragments).map_err(CdfError::from)
}

fn logical_batch_bytes(batch: &RecordBatch) -> Result<u64> {
    batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .try_fold(0_u64, |total, (field, column)| {
            let bytes = logical_array_bytes(field, column.as_ref())?;
            total
                .checked_add(bytes)
                .ok_or_else(|| CdfError::data("canonical logical batch bytes overflow"))
        })
}

fn logical_array_bytes(field: &arrow_schema::Field, array: &dyn Array) -> Result<u64> {
    let validity = if field.is_nullable() {
        u64::try_from(array.len())
            .map_err(|_| CdfError::data("canonical nullable bytes exceed u64"))?
    } else {
        0
    };
    let nested = match field.data_type() {
        arrow_schema::DataType::List(item) => {
            let array = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| CdfError::internal("list array/type mismatch"))?;
            let offsets = array.value_offsets();
            let start =
                usize::try_from(offsets[0]).map_err(|_| CdfError::data("negative list offset"))?;
            let end = usize::try_from(offsets[array.len()])
                .map_err(|_| CdfError::data("negative list offset"))?;
            let values = array.values().slice(start, end.saturating_sub(start));
            Some((
                u64::try_from(array.len().saturating_mul(4))
                    .map_err(|_| CdfError::data("list offsets exceed u64"))?,
                logical_array_bytes(item, values.as_ref())?,
            ))
        }
        arrow_schema::DataType::LargeList(item) => {
            let array = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| CdfError::internal("large-list array/type mismatch"))?;
            let offsets = array.value_offsets();
            let start = usize::try_from(offsets[0])
                .map_err(|_| CdfError::data("negative large-list offset"))?;
            let end = usize::try_from(offsets[array.len()])
                .map_err(|_| CdfError::data("negative large-list offset"))?;
            let values = array.values().slice(start, end.saturating_sub(start));
            Some((
                u64::try_from(array.len().saturating_mul(8))
                    .map_err(|_| CdfError::data("large-list offsets exceed u64"))?,
                logical_array_bytes(item, values.as_ref())?,
            ))
        }
        arrow_schema::DataType::FixedSizeList(item, size) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| CdfError::internal("fixed-size-list array/type mismatch"))?;
            let start = usize::try_from(array.value_offset(0))
                .map_err(|_| CdfError::data("negative fixed-size-list offset"))?;
            let value_count = usize::try_from(*size)
                .map_err(|_| CdfError::data("negative fixed-size-list size"))?
                .checked_mul(array.len())
                .ok_or_else(|| CdfError::data("fixed-size-list value count overflow"))?;
            let values = array.values().slice(start, value_count);
            Some((0, logical_array_bytes(item, values.as_ref())?))
        }
        arrow_schema::DataType::Struct(fields) => {
            let array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| CdfError::internal("struct array/type mismatch"))?;
            let children =
                fields
                    .iter()
                    .zip(array.columns())
                    .try_fold(0_u64, |total, (child, values)| {
                        total
                            .checked_add(logical_array_bytes(child, values.as_ref())?)
                            .ok_or_else(|| CdfError::data("struct logical bytes overflow"))
                    })?;
            Some((0, children))
        }
        arrow_schema::DataType::Map(entries, _) => {
            let array = array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| CdfError::internal("map array/type mismatch"))?;
            let offsets = array.value_offsets();
            let start =
                usize::try_from(offsets[0]).map_err(|_| CdfError::data("negative map offset"))?;
            let end = usize::try_from(offsets[array.len()])
                .map_err(|_| CdfError::data("negative map offset"))?;
            let values = array.entries().slice(start, end.saturating_sub(start));
            Some((
                u64::try_from(array.len().saturating_mul(4))
                    .map_err(|_| CdfError::data("map offsets exceed u64"))?,
                logical_array_bytes(entries, &values)?,
            ))
        }
        arrow_schema::DataType::ListView(item) => {
            let array = array
                .as_any()
                .downcast_ref::<ListViewArray>()
                .ok_or_else(|| CdfError::internal("list-view array/type mismatch"))?;
            let children = (0..array.len()).try_fold(0_u64, |total, index| {
                total
                    .checked_add(logical_array_bytes(item, array.value(index).as_ref())?)
                    .ok_or_else(|| CdfError::data("list-view logical bytes overflow"))
            })?;
            Some((
                u64::try_from(array.len().saturating_mul(8))
                    .map_err(|_| CdfError::data("list-view offsets exceed u64"))?,
                children,
            ))
        }
        arrow_schema::DataType::LargeListView(item) => {
            let array = array
                .as_any()
                .downcast_ref::<LargeListViewArray>()
                .ok_or_else(|| CdfError::internal("large-list-view array/type mismatch"))?;
            let children = (0..array.len()).try_fold(0_u64, |total, index| {
                total
                    .checked_add(logical_array_bytes(item, array.value(index).as_ref())?)
                    .ok_or_else(|| CdfError::data("large-list-view logical bytes overflow"))
            })?;
            Some((
                u64::try_from(array.len().saturating_mul(16))
                    .map_err(|_| CdfError::data("large-list-view offsets exceed u64"))?,
                children,
            ))
        }
        arrow_schema::DataType::Utf8View => {
            let array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| CdfError::internal("utf8-view array/type mismatch"))?;
            let values = array.iter().try_fold(0_u64, |total, value| {
                total
                    .checked_add(value.map_or(0, |value| value.len() as u64))
                    .ok_or_else(|| CdfError::data("utf8-view logical bytes overflow"))
            })?;
            Some((
                u64::try_from(array.len().saturating_mul(16))
                    .map_err(|_| CdfError::data("utf8-view descriptors exceed u64"))?,
                values,
            ))
        }
        arrow_schema::DataType::BinaryView => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| CdfError::internal("binary-view array/type mismatch"))?;
            let values = array.iter().try_fold(0_u64, |total, value| {
                total
                    .checked_add(value.map_or(0, |value| value.len() as u64))
                    .ok_or_else(|| CdfError::data("binary-view logical bytes overflow"))
            })?;
            Some((
                u64::try_from(array.len().saturating_mul(16))
                    .map_err(|_| CdfError::data("binary-view descriptors exceed u64"))?,
                values,
            ))
        }
        arrow_schema::DataType::Union(fields, mode) => {
            let array = array
                .as_any()
                .downcast_ref::<UnionArray>()
                .ok_or_else(|| CdfError::internal("union array/type mismatch"))?;
            let children = (0..array.len()).try_fold(0_u64, |total, index| {
                let type_id = array.type_id(index);
                let child_field = fields
                    .iter()
                    .find_map(|(id, field)| (id == type_id).then_some(field))
                    .ok_or_else(|| CdfError::data("union type id has no field"))?;
                total
                    .checked_add(logical_array_bytes(
                        child_field,
                        array.value(index).as_ref(),
                    )?)
                    .ok_or_else(|| CdfError::data("union logical bytes overflow"))
            })?;
            let width = match mode {
                arrow_schema::UnionMode::Sparse => 1,
                arrow_schema::UnionMode::Dense => 5,
            };
            Some((
                u64::try_from(array.len().saturating_mul(width))
                    .map_err(|_| CdfError::data("union selectors exceed u64"))?,
                children,
            ))
        }
        arrow_schema::DataType::Dictionary(key_type, value_type) => {
            let value_field =
                arrow_schema::Field::new("dictionary-value", value_type.as_ref().clone(), true);
            macro_rules! dictionary {
                ($key:ty) => {{
                    let array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<$key>>()
                        .ok_or_else(|| CdfError::internal("dictionary array/type mismatch"))?;
                    Some((
                        u64::try_from(array.len().saturating_mul(std::mem::size_of::<
                            <$key as arrow_array::types::ArrowPrimitiveType>::Native,
                        >()))
                        .map_err(|_| CdfError::data("dictionary keys exceed u64"))?,
                        dictionary_value_bytes(array, &value_field)?,
                    ))
                }};
            }
            match key_type.as_ref() {
                arrow_schema::DataType::Int8 => dictionary!(Int8Type),
                arrow_schema::DataType::Int16 => dictionary!(Int16Type),
                arrow_schema::DataType::Int32 => dictionary!(Int32Type),
                arrow_schema::DataType::Int64 => dictionary!(Int64Type),
                arrow_schema::DataType::UInt8 => dictionary!(UInt8Type),
                arrow_schema::DataType::UInt16 => dictionary!(UInt16Type),
                arrow_schema::DataType::UInt32 => dictionary!(UInt32Type),
                arrow_schema::DataType::UInt64 => dictionary!(UInt64Type),
                other => {
                    return Err(CdfError::data(format!(
                        "unsupported Arrow dictionary key type in canonical segmentation: {other}"
                    )));
                }
            }
        }
        _ => None,
    };
    if let Some((offset_bytes, child_bytes)) = nested {
        return validity
            .checked_add(offset_bytes)
            .and_then(|bytes| bytes.checked_add(child_bytes))
            .ok_or_else(|| CdfError::data("nested canonical logical bytes overflow"));
    }
    let data = array.to_data();
    let mut bytes = data.get_slice_memory_size().map_err(CdfError::from)?;
    if data.nulls().is_some() {
        bytes = bytes.saturating_sub(data.len().div_ceil(8));
    }
    validity
        .checked_add(
            u64::try_from(bytes)
                .map_err(|_| CdfError::data("canonical logical array bytes exceed u64"))?,
        )
        .ok_or_else(|| CdfError::data("canonical logical array bytes overflow"))
}

fn dictionary_value_bytes<K>(
    array: &DictionaryArray<K>,
    value_field: &arrow_schema::Field,
) -> Result<u64>
where
    K: ArrowDictionaryKeyType,
    usize: TryFrom<K::Native>,
{
    let mut value_sizes = vec![None; array.values().len()];
    let mut total = 0_u64;
    for row in 0..array.len() {
        if array.is_null(row) {
            continue;
        }
        let index = usize::try_from(array.keys().value(row))
            .map_err(|_| CdfError::data("dictionary key cannot index values"))?;
        let value_bytes = match value_sizes.get(index).copied().flatten() {
            Some(bytes) => bytes,
            None => {
                let bytes =
                    logical_array_bytes(value_field, array.values().slice(index, 1).as_ref())?;
                let slot = value_sizes
                    .get_mut(index)
                    .ok_or_else(|| CdfError::data("dictionary key is outside values"))?;
                *slot = Some(bytes);
                bytes
            }
        };
        total = total
            .checked_add(value_bytes)
            .ok_or_else(|| CdfError::data("dictionary logical bytes overflow"))?;
    }
    Ok(total)
}

fn largest_prefix_within_bytes(
    batch: &RecordBatch,
    maximum_rows: usize,
    maximum_bytes: u64,
) -> Result<usize> {
    if maximum_rows == 0 || maximum_bytes == 0 {
        return Ok(0);
    }
    if logical_batch_bytes(&batch.slice(0, maximum_rows))? <= maximum_bytes {
        return Ok(maximum_rows);
    }
    let mut low = 0_usize;
    let mut high = maximum_rows;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if logical_batch_bytes(&batch.slice(0, middle))? <= maximum_bytes {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    Ok(low)
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
            Some(existing) => {
                let merged = merge_file_position_evidence(existing, file).map_err(|error| {
                    CdfError::contract(format!(
                        "file-manifest position has conflicting identity for `{}`: {error}",
                        file.path
                    ))
                })?;
                files.insert(file.path.clone(), merged);
            }
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
    use arrow_array::{
        Array, Int64Array, StringArray, StringViewArray,
        builder::{
            Int64Builder, ListBuilder, ListViewBuilder, StringDictionaryBuilder, UnionBuilder,
        },
    };
    use cdf_kernel::{CursorPosition, FileManifest};

    #[test]
    fn ids_depend_only_on_partition_and_segment_ordinals() {
        let policy = CanonicalSegmentationPolicy::p3_v2();
        assert_eq!(
            policy.segment_id(2, 7).unwrap().as_str(),
            "p00000002-s00000007"
        );
        assert_eq!(policy, CanonicalSegmentationPolicy::p3_v2());
    }

    #[test]
    fn adaptive_targets_stay_bounded_and_outside_policy_identity() {
        let policy = CanonicalSegmentationPolicy::p3_v2();
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
        assert_eq!(policy, CanonicalSegmentationPolicy::p3_v2());
    }

    #[test]
    fn typed_position_join_unions_files_advances_numeric_cursor_and_refuses_tokens() {
        let file = |path: &str| FilePosition {
            path: path.to_owned(),
            size_bytes: 1,
            source_generation: None,
            etag: Some(path.to_owned()),
            object_version: None,
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
            ..CanonicalSegmentationPolicy::p3_v2()
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
    fn positioned_oversize_preserves_one_exact_conservative_boundary() {
        let mut assembler = CanonicalSegmentAssembler::new(test_policy(), 0).unwrap();
        let position = SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "id".to_owned(),
            value: CursorValue::U64(5),
        });
        let segments = assembler
            .push(batch(&[1, 2, 3, 4, 5]), Some(position.clone()))
            .unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].row_count, 5);
        assert_eq!(segments[0].output_position.as_ref(), Some(&position));
        assert!(assembler.finish().unwrap().is_empty());
    }

    #[test]
    fn oversized_file_batch_splits_with_terminal_manifest_on_every_segment() {
        let position = SourcePosition::FileManifest(FileManifest {
            version: 1,
            files: vec![FilePosition {
                path: "part.parquet".to_owned(),
                size_bytes: 42,
                source_generation: None,
                etag: Some("etag".to_owned()),
                object_version: None,
                sha256: None,
            }],
        });
        let mut assembler = CanonicalSegmentAssembler::new(test_policy(), 0).unwrap();
        let mut segments = assembler
            .push(batch(&[1, 2, 3, 4, 5]), Some(position.clone()))
            .unwrap();
        segments.extend(assembler.finish().unwrap());

        assert_eq!(segments.len(), 2);
        assert_eq!(segments[0].row_count, 4);
        assert_eq!(segments[1].row_count, 1);
        assert!(
            segments
                .iter()
                .all(|segment| segment.output_position.as_ref() == Some(&position))
        );
    }

    fn byte_test_policy() -> CanonicalSegmentationPolicy {
        CanonicalSegmentationPolicy {
            target_rows: 64,
            target_bytes: 32,
            maximum_rows: 64,
            maximum_bytes: 48,
            microbatch_minimum_rows: 1,
            microbatch_maximum_rows: 64,
            microbatch_minimum_bytes: 1,
            microbatch_maximum_bytes: 48,
            ..CanonicalSegmentationPolicy::p3_v2()
        }
    }

    fn string_batch(values: &[&str]) -> RecordBatch {
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "value",
                arrow_schema::DataType::Utf8,
                false,
            )]));
        RecordBatch::try_new(
            schema,
            vec![std::sync::Arc::new(StringArray::from(values.to_vec()))],
        )
        .unwrap()
    }

    fn assemble_strings(chunks: &[&[&str]]) -> Vec<CanonicalSegment> {
        let mut assembler = CanonicalSegmentAssembler::new(byte_test_policy(), 4).unwrap();
        let mut output = Vec::new();
        for chunk in chunks {
            output.extend(assembler.push(string_batch(chunk), None).unwrap());
        }
        output.extend(assembler.finish().unwrap());
        output
    }

    #[test]
    fn assembler_enforces_byte_target_independent_of_string_rechunking() {
        let values = ["aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"];
        let one = assemble_strings(&[&values]);
        let many = assemble_strings(&[&values[..1], &values[1..3], &values[3..5], &values[5..]]);
        let shape = |segments: &[CanonicalSegment]| {
            segments
                .iter()
                .map(|segment| {
                    (
                        segment.segment_id.as_str().to_owned(),
                        segment.row_count,
                        segment.logical_bytes,
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(shape(&one), shape(&many));
        assert_eq!(
            shape(&one),
            [
                ("p00000004-s00000000".to_owned(), 4, 32),
                ("p00000004-s00000001".to_owned(), 2, 16)
            ]
        );
    }

    #[test]
    fn target_flush_does_not_attach_next_position_to_previous_segment() {
        let cursor = |value| {
            Some(SourcePosition::Cursor(CursorPosition {
                version: 1,
                field: "id".to_owned(),
                value: CursorValue::U64(value),
            }))
        };
        let mut assembler = CanonicalSegmentAssembler::new(test_policy(), 0).unwrap();
        assert!(
            assembler
                .push(batch(&[1, 2, 3]), cursor(3))
                .unwrap()
                .is_empty()
        );
        let emitted = assembler.push(batch(&[4, 5]), cursor(5)).unwrap();
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].output_position, cursor(3));
        assert_eq!(assembler.finish().unwrap()[0].output_position, cursor(5));
    }

    #[test]
    fn nullable_byte_estimate_is_rechunking_additive() {
        let batch = |values: Vec<Option<&str>>| {
            let schema =
                std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                    "value",
                    arrow_schema::DataType::Utf8,
                    true,
                )]));
            RecordBatch::try_new(schema, vec![std::sync::Arc::new(StringArray::from(values))])
                .unwrap()
        };
        let one = batch(vec![Some("aaaa"), None, Some("cccc"), None]);
        let left = batch(vec![Some("aaaa"), None]);
        let right = batch(vec![Some("cccc"), None]);
        assert_eq!(
            logical_batch_bytes(&one).unwrap(),
            logical_batch_bytes(&left).unwrap() + logical_batch_bytes(&right).unwrap()
        );
    }

    #[test]
    fn list_byte_estimate_counts_only_the_logical_slice() {
        let batch = |rows: &[&[i64]]| {
            let mut values = ListBuilder::new(Int64Builder::new());
            for row in rows {
                values.values().append_slice(row);
                values.append(true);
            }
            RecordBatch::try_from_iter([(
                "values",
                std::sync::Arc::new(values.finish()) as arrow_array::ArrayRef,
            )])
            .unwrap()
        };
        let rows: &[&[i64]] = &[&[1, 2], &[3, 4], &[5, 6], &[7, 8]];
        let one = batch(rows);
        let left = batch(&rows[..2]);
        let right = batch(&rows[2..]);
        assert_eq!(
            logical_batch_bytes(&one).unwrap(),
            logical_batch_bytes(&left).unwrap() + logical_batch_bytes(&right).unwrap()
        );
        assert_eq!(
            logical_batch_bytes(&one.slice(0, 1)).unwrap() * 4,
            logical_batch_bytes(&one).unwrap()
        );
    }

    #[test]
    fn dictionary_byte_estimate_is_independent_of_dictionary_chunking() {
        let batch = |values: &[&str]| {
            let mut dictionary = StringDictionaryBuilder::<Int8Type>::new();
            for value in values {
                dictionary.append(*value).unwrap();
            }
            RecordBatch::try_from_iter([(
                "value",
                std::sync::Arc::new(dictionary.finish()) as arrow_array::ArrayRef,
            )])
            .unwrap()
        };
        let values = ["shared-long-value", "x", "shared-long-value", "y"];
        let one = batch(&values);
        let left = batch(&values[..2]);
        let right = batch(&values[2..]);
        assert_eq!(
            logical_batch_bytes(&one).unwrap(),
            logical_batch_bytes(&left).unwrap() + logical_batch_bytes(&right).unwrap()
        );
    }

    #[test]
    fn string_view_byte_estimate_includes_out_of_line_values_and_slices() {
        let batch = |values: &[&str]| {
            RecordBatch::try_from_iter([(
                "value",
                std::sync::Arc::new(StringViewArray::from(values.to_vec()))
                    as arrow_array::ArrayRef,
            )])
            .unwrap()
        };
        let values = [
            "a value longer than twelve bytes",
            "short",
            "another out of line string value",
            "tail",
        ];
        let one = batch(&values);
        let left = batch(&values[..2]);
        let right = batch(&values[2..]);
        assert_eq!(
            logical_batch_bytes(&one).unwrap(),
            logical_batch_bytes(&left).unwrap() + logical_batch_bytes(&right).unwrap()
        );
        assert!(
            logical_batch_bytes(&one.slice(0, 1)).unwrap() < logical_batch_bytes(&one).unwrap()
        );
    }

    #[test]
    fn dense_union_byte_estimate_is_rechunking_additive() {
        use arrow_array::types::Float64Type;

        let batch = |values: &[(i64, f64)]| {
            let mut union = UnionBuilder::new_dense();
            for (integer, float) in values {
                union.append::<Int64Type>("integer", *integer).unwrap();
                union.append::<Float64Type>("float", *float).unwrap();
            }
            RecordBatch::try_from_iter([(
                "value",
                std::sync::Arc::new(union.build().unwrap()) as arrow_array::ArrayRef,
            )])
            .unwrap()
        };
        let values = [(1, 1.5), (2, 2.5), (3, 3.5), (4, 4.5)];
        let one = batch(&values);
        let left = batch(&values[..2]);
        let right = batch(&values[2..]);
        assert_eq!(
            logical_batch_bytes(&one).unwrap(),
            logical_batch_bytes(&left).unwrap() + logical_batch_bytes(&right).unwrap()
        );
    }

    #[test]
    fn list_view_byte_estimate_counts_logical_values_not_backing_capacity() {
        let batch = |rows: &[&[i64]]| {
            let mut values = ListViewBuilder::new(Int64Builder::new());
            for row in rows {
                values.append_value(row.iter().copied().map(Some));
            }
            RecordBatch::try_from_iter([(
                "values",
                std::sync::Arc::new(values.finish()) as arrow_array::ArrayRef,
            )])
            .unwrap()
        };
        let rows: &[&[i64]] = &[&[1, 2], &[3], &[4, 5, 6], &[7]];
        let one = batch(rows);
        let left = batch(&rows[..2]);
        let right = batch(&rows[2..]);
        assert_eq!(
            logical_batch_bytes(&one).unwrap(),
            logical_batch_bytes(&left).unwrap() + logical_batch_bytes(&right).unwrap()
        );
        assert!(
            logical_batch_bytes(&one.slice(0, 1)).unwrap() < logical_batch_bytes(&one).unwrap()
        );
    }

    #[test]
    fn canonical_microbatch_reuses_exact_batches_and_coalesces_only_boundary_fragments() {
        let exact = batch(&[1, 2, 3, 4]);
        let exact_column = exact.column(0).clone();
        let output = canonicalize_batches(vec![exact], 4, 1024).unwrap();
        assert_eq!(output.len(), 1);
        assert!(std::sync::Arc::ptr_eq(output[0].column(0), &exact_column));

        let fragmented =
            canonicalize_batches(vec![batch(&[1]), batch(&[2, 3, 4])], 4, 1024).unwrap();
        assert_eq!(fragmented.len(), 1);
        assert_eq!(
            fragmented[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 2, 3, 4]
        );
    }

    #[test]
    #[ignore = "release-mode A3 fixed-cost benchmark"]
    fn canonical_coalescing_package_benchmark() {
        let chunks = (0..64)
            .map(|chunk| {
                let start = chunk * 1024;
                batch(&(start..start + 1024).map(i64::from).collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        let root = tempfile::tempdir().unwrap();

        let baseline_started = std::time::Instant::now();
        let baseline = cdf_package::PackageBuilder::create(
            root.path().join("baseline"),
            "baseline",
            cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
                .unwrap(),
        )
        .unwrap();
        for (ordinal, chunk) in chunks.iter().enumerate() {
            let package_row_ord_start = u64::try_from(ordinal * 1024).unwrap();
            let canonical = cdf_package_contract::append_package_row_ord(
                vec![chunk.clone()],
                package_row_ord_start,
            )
            .unwrap();
            baseline
                .write_segment(
                    SegmentId::new(format!("baseline-{ordinal:08}")).unwrap(),
                    package_row_ord_start,
                    &canonical,
                )
                .unwrap();
        }
        baseline.finish().unwrap();
        let baseline_ns = baseline_started.elapsed().as_nanos();

        let canonical_started = std::time::Instant::now();
        let mut assembler =
            CanonicalSegmentAssembler::new(CanonicalSegmentationPolicy::p3_v2(), 0).unwrap();
        let mut segments = Vec::new();
        for chunk in &chunks {
            segments.extend(assembler.push(chunk.clone(), None).unwrap());
        }
        segments.extend(assembler.finish().unwrap());
        assert_eq!(segments.len(), 1);
        let canonical = cdf_package::PackageBuilder::create(
            root.path().join("canonical"),
            "canonical",
            cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
                .unwrap(),
        )
        .unwrap();
        let mut package_row_ord_start = 0_u64;
        for segment in segments {
            let row_count = segment.row_count;
            let batches = cdf_package_contract::append_package_row_ord(
                segment.batches,
                package_row_ord_start,
            )
            .unwrap();
            canonical
                .write_segment(segment.segment_id, package_row_ord_start, &batches)
                .unwrap();
            package_row_ord_start += row_count;
        }
        canonical.finish().unwrap();
        let canonical_ns = canonical_started.elapsed().as_nanos();
        let speedup = baseline_ns as f64 / canonical_ns as f64;
        eprintln!(
            "baseline_1024_row_segments_ns={baseline_ns} canonical_64k_segment_ns={canonical_ns} speedup={speedup:.2}"
        );
        assert!(canonical_ns < baseline_ns);
    }
}
