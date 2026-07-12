use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, LargeListArray, LargeListViewArray,
    ListArray, ListViewArray, MapArray, PrimitiveArray, StructArray, UInt64Array, UnionArray,
    types::{
        ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use arrow_buffer::{ArrowNativeType, OffsetBuffer, ScalarBuffer};
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
                    canonicalize_dictionary(dictionary)
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
        DataType::Union(fields, mode) => {
            let union = array
                .as_any()
                .downcast_ref::<UnionArray>()
                .ok_or_else(|| CdfError::internal("union array/type mismatch"))?;
            let mut selected = BTreeMap::<i8, Vec<u64>>::new();
            let mut offsets = (*mode == arrow_schema::UnionMode::Dense)
                .then(|| Vec::<i32>::with_capacity(union.len()));
            if let Some(offsets) = offsets.as_mut() {
                for row in 0..union.len() {
                    let type_id = union.type_id(row);
                    let indices = selected.entry(type_id).or_default();
                    offsets.push(
                        i32::try_from(indices.len())
                            .map_err(|_| CdfError::data("dense union offsets exceed i32"))?,
                    );
                    indices.push(
                        u64::try_from(union.value_offset(row))
                            .map_err(|_| CdfError::data("union value offset exceeds u64"))?,
                    );
                }
            }
            let children = fields
                .iter()
                .map(|(type_id, _)| {
                    let indices = match &offsets {
                        Some(_) => selected.remove(&type_id).unwrap_or_default(),
                        None => (0..union.len())
                            .map(|row| {
                                u64::try_from(union.value_offset(row))
                                    .map_err(|_| CdfError::data("union value offset exceeds u64"))
                            })
                            .collect::<Result<Vec<_>>>()?,
                    };
                    let values = take(
                        union.child(type_id).as_ref(),
                        &UInt64Array::from(indices),
                        None,
                    )
                    .map_err(CdfError::from)?;
                    canonicalize_array(values)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(
                UnionArray::try_new(
                    fields.clone(),
                    union.type_ids().clone(),
                    offsets.map(ScalarBuffer::from),
                    children,
                )
                .map_err(CdfError::from)?,
            ))
        }
        _ => Ok(array),
    }
}

fn canonicalize_dictionary<K>(dictionary: &DictionaryArray<K>) -> Result<ArrayRef>
where
    K: ArrowDictionaryKeyType,
    PrimitiveArray<K>: From<Vec<Option<K::Native>>>,
{
    let mut old_to_new = vec![None::<usize>; dictionary.values().len()];
    let mut selected_values = Vec::<u64>::new();
    let mut keys = Vec::<Option<K::Native>>::with_capacity(dictionary.len());
    for row in 0..dictionary.len() {
        if dictionary.is_null(row) {
            keys.push(None);
            continue;
        }
        let old = dictionary
            .keys()
            .value(row)
            .to_usize()
            .ok_or_else(|| CdfError::data("dictionary key cannot index values"))?;
        let slot = old_to_new
            .get_mut(old)
            .ok_or_else(|| CdfError::data("dictionary key is outside values"))?;
        let new = match *slot {
            Some(new) => new,
            None => {
                let new = selected_values.len();
                selected_values.push(
                    u64::try_from(old)
                        .map_err(|_| CdfError::data("dictionary value index exceeds u64"))?,
                );
                *slot = Some(new);
                new
            }
        };
        keys.push(Some(K::Native::from_usize(new).ok_or_else(|| {
            CdfError::data("canonical dictionary index exceeds key width")
        })?));
    }
    let values = take(
        dictionary.values().as_ref(),
        &UInt64Array::from(selected_values),
        None,
    )
    .map_err(CdfError::from)?;
    Ok(Arc::new(DictionaryArray::<K>::new(
        PrimitiveArray::<K>::from(keys),
        canonicalize_array(values)?,
    )))
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
    let mut selected_indices = Vec::<u64>::new();
    let mut selected_offsets = Vec::<usize>::with_capacity(map.len() + 1);
    selected_offsets.push(0);
    for row in 0..map.len() {
        if !map.is_null(row) {
            let start = usize::try_from(source_offsets[row])
                .map_err(|_| CdfError::data("map has a negative entry offset"))?;
            let end = usize::try_from(source_offsets[row + 1])
                .map_err(|_| CdfError::data("map has a negative entry offset"))?;
            for index in start..end {
                selected_indices.push(
                    u64::try_from(index)
                        .map_err(|_| CdfError::data("map entry index exceeds u64"))?,
                );
            }
        }
        selected_offsets.push(selected_indices.len());
    }
    let selected =
        take(map.entries(), &UInt64Array::from(selected_indices), None).map_err(CdfError::from)?;
    let canonical_entries = canonicalize_array(selected)?;
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
        let start = selected_offsets[row];
        let end = selected_offsets[row + 1];
        let mut indices = (start..end).collect::<Vec<_>>();
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
    use arrow_array::{Int8Array, Int32Array, StringArray};
    use arrow_schema::{Field, UnionFields};

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

    #[test]
    fn dictionary_does_not_validate_unreferenced_map_values() {
        let values = MapArray::from_vec_of_maps::<StringArray, Int32Array, _, _>(
            vec![
                Some(vec![("duplicate", Some(1)), ("duplicate", Some(2))]),
                Some(vec![("kept", Some(3))]),
            ],
            false,
        );
        let dictionary =
            DictionaryArray::<Int8Type>::new(Int8Array::from(vec![1_i8]), Arc::new(values));
        let canonical = canonicalize_map_order(vec![Arc::new(dictionary) as ArrayRef]).unwrap();
        assert_eq!(canonical[0].len(), 1);
    }

    #[test]
    fn dense_union_does_not_validate_unselected_map_child() {
        let invalid_map = map(vec![("duplicate", Some(1)), ("duplicate", Some(2))]);
        let int_field = Arc::new(Field::new("integer", DataType::Int32, false));
        let map_field = Arc::new(Field::new(
            "attributes",
            invalid_map.data_type().clone(),
            false,
        ));
        let fields = UnionFields::try_new(vec![0, 1], vec![map_field, int_field]).unwrap();
        let union = UnionArray::try_new(
            fields,
            ScalarBuffer::from(vec![1_i8]),
            Some(ScalarBuffer::from(vec![0_i32])),
            vec![invalid_map, Arc::new(Int32Array::from(vec![7]))],
        )
        .unwrap();
        let canonical = canonicalize_map_order(vec![Arc::new(union) as ArrayRef]).unwrap();
        assert_eq!(canonical[0].len(), 1);
    }
}
