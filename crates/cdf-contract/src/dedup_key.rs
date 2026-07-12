use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, LargeListArray, LargeListViewArray,
    ListArray, ListViewArray, MapArray, StructArray, UInt64Array, UnionArray,
    types::{
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_row::{RowConverter, SortField};
use arrow_schema::DataType;
use arrow_select::take::take;
use cdf_kernel::{CdfError, Result};

pub(crate) fn canonicalize_map_order(arrays: Vec<ArrayRef>) -> Result<Vec<ArrayRef>> {
    arrays
        .into_iter()
        .map(|array| {
            if contains_map(array.data_type()) {
                canonicalize_array(array)
            } else {
                Ok(array)
            }
        })
        .collect()
}

fn contains_map(data_type: &DataType) -> bool {
    match data_type {
        DataType::Map(_, _) => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => contains_map(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_map(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| contains_map(field.data_type())),
        DataType::Dictionary(_, values) => contains_map(values),
        _ => false,
    }
}

fn canonicalize_array(array: ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Map(_, _) => canonicalize_map(array),
        DataType::List(field) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| CdfError::internal("list array/type mismatch"))?;
            let offsets = list.value_offsets();
            let start = usize::try_from(offsets[0])
                .map_err(|_| CdfError::data("list has a negative offset"))?;
            let end = usize::try_from(offsets[list.len()])
                .map_err(|_| CdfError::data("list has a negative offset"))?;
            let normalized = offsets
                .iter()
                .map(|offset| *offset - offsets[0])
                .collect::<Vec<_>>();
            Ok(Arc::new(ListArray::new(
                Arc::clone(field),
                OffsetBuffer::new(ScalarBuffer::from(normalized)),
                canonicalize_array(list.values().slice(start, end.saturating_sub(start)))?,
                list.nulls().cloned(),
            )))
        }
        DataType::LargeList(field) => {
            let list = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| CdfError::internal("large-list array/type mismatch"))?;
            let offsets = list.value_offsets();
            let start = usize::try_from(offsets[0])
                .map_err(|_| CdfError::data("large list has a negative offset"))?;
            let end = usize::try_from(offsets[list.len()])
                .map_err(|_| CdfError::data("large list has a negative offset"))?;
            let normalized = offsets
                .iter()
                .map(|offset| *offset - offsets[0])
                .collect::<Vec<_>>();
            Ok(Arc::new(LargeListArray::new(
                Arc::clone(field),
                OffsetBuffer::new(ScalarBuffer::from(normalized)),
                canonicalize_array(list.values().slice(start, end.saturating_sub(start)))?,
                list.nulls().cloned(),
            )))
        }
        DataType::ListView(field) => {
            let list = array
                .as_any()
                .downcast_ref::<ListViewArray>()
                .ok_or_else(|| CdfError::internal("list-view array/type mismatch"))?;
            let mut indices = Vec::<u64>::new();
            let mut offsets = Vec::<i32>::with_capacity(list.len());
            let mut sizes = Vec::<i32>::with_capacity(list.len());
            for row in 0..list.len() {
                offsets.push(
                    i32::try_from(indices.len())
                        .map_err(|_| CdfError::data("list-view values exceed i32"))?,
                );
                let size = if list.is_null(row) {
                    0
                } else {
                    usize::try_from(list.value_sizes()[row])
                        .map_err(|_| CdfError::data("list-view has a negative size"))?
                };
                sizes.push(
                    i32::try_from(size)
                        .map_err(|_| CdfError::data("list-view size exceeds i32"))?,
                );
                let start = usize::try_from(list.value_offsets()[row])
                    .map_err(|_| CdfError::data("list-view has a negative offset"))?;
                for index in start..start.saturating_add(size) {
                    indices.push(
                        u64::try_from(index)
                            .map_err(|_| CdfError::data("list-view index exceeds u64"))?,
                    );
                }
            }
            let values = take(list.values().as_ref(), &UInt64Array::from(indices), None)
                .map_err(CdfError::from)?;
            Ok(Arc::new(ListViewArray::new(
                Arc::clone(field),
                ScalarBuffer::from(offsets),
                ScalarBuffer::from(sizes),
                canonicalize_array(values)?,
                list.nulls().cloned(),
            )))
        }
        DataType::LargeListView(field) => {
            let list = array
                .as_any()
                .downcast_ref::<LargeListViewArray>()
                .ok_or_else(|| CdfError::internal("large-list-view array/type mismatch"))?;
            let mut indices = Vec::<u64>::new();
            let mut offsets = Vec::<i64>::with_capacity(list.len());
            let mut sizes = Vec::<i64>::with_capacity(list.len());
            for row in 0..list.len() {
                offsets.push(
                    i64::try_from(indices.len())
                        .map_err(|_| CdfError::data("large-list-view values exceed i64"))?,
                );
                let size = if list.is_null(row) {
                    0
                } else {
                    usize::try_from(list.value_sizes()[row])
                        .map_err(|_| CdfError::data("large-list-view has a negative size"))?
                };
                sizes.push(
                    i64::try_from(size)
                        .map_err(|_| CdfError::data("large-list-view size exceeds i64"))?,
                );
                let start = usize::try_from(list.value_offsets()[row])
                    .map_err(|_| CdfError::data("large-list-view has a negative offset"))?;
                for index in start..start.saturating_add(size) {
                    indices.push(
                        u64::try_from(index)
                            .map_err(|_| CdfError::data("large-list-view index exceeds u64"))?,
                    );
                }
            }
            let values = take(list.values().as_ref(), &UInt64Array::from(indices), None)
                .map_err(CdfError::from)?;
            Ok(Arc::new(LargeListViewArray::new(
                Arc::clone(field),
                ScalarBuffer::from(offsets),
                ScalarBuffer::from(sizes),
                canonicalize_array(values)?,
                list.nulls().cloned(),
            )))
        }
        DataType::FixedSizeList(field, size) => {
            let list = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| CdfError::internal("fixed-size-list array/type mismatch"))?;
            let start = usize::try_from(list.value_offset(0))
                .map_err(|_| CdfError::data("fixed-size list has a negative offset"))?;
            let values = usize::try_from(*size)
                .map_err(|_| CdfError::data("fixed-size list has a negative size"))?
                .checked_mul(list.len())
                .ok_or_else(|| CdfError::data("fixed-size list value count overflow"))?;
            Ok(Arc::new(FixedSizeListArray::new(
                Arc::clone(field),
                *size,
                canonicalize_array(list.values().slice(start, values))?,
                list.nulls().cloned(),
            )))
        }
        DataType::Struct(fields) => {
            let structure = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| CdfError::internal("struct array/type mismatch"))?;
            let columns = structure
                .columns()
                .iter()
                .cloned()
                .map(canonicalize_array)
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::new(
                fields.clone(),
                columns,
                structure.nulls().cloned(),
            )))
        }
        DataType::Dictionary(key_type, _) => {
            macro_rules! dictionary {
                ($key:ty) => {{
                    let dictionary = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<$key>>()
                        .ok_or_else(|| CdfError::internal("dictionary array/type mismatch"))?;
                    Ok(Arc::new(DictionaryArray::<$key>::new(
                        dictionary.keys().clone(),
                        canonicalize_array(Arc::clone(dictionary.values()))?,
                    )) as ArrayRef)
                }};
            }
            match key_type.as_ref() {
                DataType::Int8 => dictionary!(Int8Type),
                DataType::Int16 => dictionary!(Int16Type),
                DataType::Int32 => dictionary!(Int32Type),
                DataType::Int64 => dictionary!(Int64Type),
                DataType::UInt8 => dictionary!(UInt8Type),
                DataType::UInt16 => dictionary!(UInt16Type),
                DataType::UInt32 => dictionary!(UInt32Type),
                DataType::UInt64 => dictionary!(UInt64Type),
                other => Err(CdfError::data(format!(
                    "unsupported dictionary key type while canonicalizing dedup maps: {other}"
                ))),
            }
        }
        DataType::Union(fields, _) => {
            let union = array
                .as_any()
                .downcast_ref::<UnionArray>()
                .ok_or_else(|| CdfError::internal("union array/type mismatch"))?;
            let children = fields
                .iter()
                .map(|(type_id, _)| canonicalize_array(Arc::clone(union.child(type_id))))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(
                UnionArray::try_new(
                    fields.clone(),
                    union.type_ids().clone(),
                    union.offsets().cloned(),
                    children,
                )
                .map_err(CdfError::from)?,
            ))
        }
        _ => Ok(array),
    }
}

fn canonicalize_map(array: ArrayRef) -> Result<ArrayRef> {
    let map = array
        .as_any()
        .downcast_ref::<MapArray>()
        .ok_or_else(|| CdfError::internal("map array/type mismatch"))?;
    let DataType::Map(entries_field, ordered) = map.data_type() else {
        return Err(CdfError::internal("map array has non-map type"));
    };
    let source_offsets = map.value_offsets();
    let entry_base = usize::try_from(source_offsets[0])
        .map_err(|_| CdfError::data("map has a negative entry offset"))?;
    let entry_end = usize::try_from(source_offsets[map.len()])
        .map_err(|_| CdfError::data("map has a negative entry offset"))?;
    let canonical_entries = canonicalize_array(Arc::new(
        map.entries()
            .slice(entry_base, entry_end.saturating_sub(entry_base)),
    ))?;
    let entries = canonical_entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| CdfError::internal("map entries are not a struct array"))?;
    let keys = Arc::clone(&entries.columns()[0]);
    let converter = RowConverter::new(vec![SortField::new(keys.data_type().clone())])
        .map_err(CdfError::from)?;
    let encoded = converter
        .convert_columns(&[Arc::clone(&keys)])
        .map_err(CdfError::from)?;

    let mut take_indices = Vec::<u64>::with_capacity(entries.len());
    let mut offsets = Vec::<i32>::with_capacity(map.len() + 1);
    offsets.push(0);
    for row in 0..map.len() {
        let start = usize::try_from(source_offsets[row])
            .map_err(|_| CdfError::data("map has a negative entry offset"))?
            .saturating_sub(entry_base);
        let end = usize::try_from(source_offsets[row + 1])
            .map_err(|_| CdfError::data("map has a negative entry offset"))?
            .saturating_sub(entry_base);
        let mut indices = (start..end).collect::<Vec<_>>();
        if !map.is_null(row) {
            if indices.iter().any(|index| keys.is_null(*index)) {
                return Err(CdfError::data(
                    "map dedup key contains a null map key; quarantine or reject the row before dedup",
                ));
            }
            indices.sort_unstable_by(|left, right| {
                encoded
                    .row(*left)
                    .as_ref()
                    .cmp(encoded.row(*right).as_ref())
            });
            if indices
                .windows(2)
                .any(|pair| encoded.row(pair[0]).as_ref() == encoded.row(pair[1]).as_ref())
            {
                return Err(CdfError::data(
                    "map dedup key contains duplicate logical map keys; quarantine or reject the row before dedup",
                ));
            }
        }
        for index in indices {
            take_indices.push(
                u64::try_from(index).map_err(|_| CdfError::data("map entry index exceeds u64"))?,
            );
        }
        offsets.push(
            i32::try_from(take_indices.len())
                .map_err(|_| CdfError::data("canonical map entries exceed i32"))?,
        );
    }
    let taken = take(entries, &UInt64Array::from(take_indices), None).map_err(CdfError::from)?;
    let sorted_entries = taken
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| CdfError::internal("taken map entries are not a struct array"))?
        .clone();
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
    Ok(Arc::new(MapArray::new(
        Arc::clone(entries_field),
        offsets,
        sorted_entries,
        map.nulls().cloned(),
        *ordered,
    )))
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::Field;

    use super::*;

    fn map(entries: Vec<(&str, Option<i32>)>) -> ArrayRef {
        Arc::new(MapArray::from_vec_of_maps::<StringArray, Int32Array, _, _>(
            vec![Some(entries)],
            false,
        ))
    }

    fn encoded(array: ArrayRef) -> Vec<u8> {
        let converter = RowConverter::new(vec![SortField::new(array.data_type().clone())]).unwrap();
        converter
            .convert_columns(&[array])
            .unwrap()
            .row(0)
            .as_ref()
            .to_vec()
    }

    #[test]
    fn map_order_is_canonicalized_before_row_encoding() {
        let left = canonicalize_map_order(vec![map(vec![("alpha", Some(1)), ("beta", Some(2))])])
            .unwrap()
            .pop()
            .unwrap();
        let right = canonicalize_map_order(vec![map(vec![("beta", Some(2)), ("alpha", Some(1))])])
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(encoded(left), encoded(right));
    }

    #[test]
    fn nested_struct_map_order_is_canonicalized() {
        let structure = |map: ArrayRef| {
            Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("attributes", map.data_type().clone(), false)),
                map,
            )])) as ArrayRef
        };
        let left = canonicalize_map_order(vec![structure(map(vec![
            ("alpha", Some(1)),
            ("beta", Some(2)),
        ]))])
        .unwrap()
        .pop()
        .unwrap();
        let right = canonicalize_map_order(vec![structure(map(vec![
            ("beta", Some(2)),
            ("alpha", Some(1)),
        ]))])
        .unwrap()
        .pop()
        .unwrap();
        assert_eq!(encoded(left), encoded(right));
    }

    #[test]
    fn duplicate_map_keys_fail_before_dedup_encoding() {
        let error = canonicalize_map_order(vec![map(vec![
            ("duplicate", Some(1)),
            ("duplicate", Some(2)),
        ])])
        .unwrap_err();
        assert!(error.message.contains("duplicate logical map keys"));
    }

    #[test]
    fn sliced_map_does_not_validate_unreferenced_entries() {
        let maps = MapArray::from_vec_of_maps::<StringArray, Int32Array, _, _>(
            vec![
                Some(vec![("duplicate", Some(1)), ("duplicate", Some(2))]),
                Some(vec![("kept", Some(3))]),
            ],
            false,
        );
        let sliced = Arc::new(maps.slice(1, 1)) as ArrayRef;
        let canonical = canonicalize_map_order(vec![sliced]).unwrap();
        assert_eq!(canonical[0].len(), 1);
    }
}
