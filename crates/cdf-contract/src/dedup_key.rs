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

pub(crate) fn encode_typed_rows(arrays: Vec<ArrayRef>, row_count: usize) -> Result<Vec<Vec<u8>>> {
    let arrays = canonicalize_map_order(arrays)?;
    let converter = RowConverter::new(
        arrays
            .iter()
            .map(|array| SortField::new(array.data_type().clone()))
            .collect(),
    )
    .map_err(CdfError::from)?;
    let rows = converter.convert_columns(&arrays).map_err(CdfError::from)?;
    Ok((0..row_count)
        .map(|row| rows.row(row).as_ref().to_vec())
        .collect())
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
    use arrow_array::{
        BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Decimal128Array,
        DurationMicrosecondArray, FixedSizeBinaryArray, Float16Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
        StringArray, StringViewArray, Time64NanosecondArray, TimestampMicrosecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow_schema::{Field, UnionFields};

    use super::*;

    fn maps(rows: Vec<Vec<(&str, Option<i32>)>>) -> MapArray {
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        offsets.push(0_u32);
        for row in rows {
            for (key, value) in row {
                keys.push(key);
                values.push(value);
            }
            offsets.push(u32::try_from(keys.len()).unwrap());
        }
        MapArray::new_from_strings(keys.into_iter(), &Int32Array::from(values), &offsets).unwrap()
    }

    fn map(entries: Vec<(&str, Option<i32>)>) -> ArrayRef {
        Arc::new(maps(vec![entries]))
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
        let maps = maps(vec![
            vec![("duplicate", Some(1)), ("duplicate", Some(2))],
            vec![("kept", Some(3))],
        ]);
        let sliced = Arc::new(maps.slice(1, 1)) as ArrayRef;
        let canonical = canonicalize_map_order(vec![sliced]).unwrap();
        assert_eq!(canonical[0].len(), 1);
    }

    #[test]
    fn dictionary_does_not_validate_unreferenced_map_values() {
        let values = maps(vec![
            vec![("duplicate", Some(1)), ("duplicate", Some(2))],
            vec![("kept", Some(3))],
        ]);
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

    #[test]
    fn scalar_vocabulary_matches_pinned_arrow_row_bytes() {
        let fixed = FixedSizeBinaryArray::try_from_iter(
            [b"aa".as_slice(), b"aa".as_slice(), b"bb".as_slice()].into_iter(),
        )
        .unwrap();
        let decimal = Decimal128Array::from(vec![11_i128, 11, 12])
            .with_precision_and_scale(18, 2)
            .unwrap();
        let arrays = vec![
            Arc::new(BooleanArray::from(vec![true, true, false])) as ArrayRef,
            Arc::new(Int8Array::from(vec![1, 1, 2])),
            Arc::new(Int16Array::from(vec![1, 1, 2])),
            Arc::new(Int32Array::from(vec![1, 1, 2])),
            Arc::new(Int64Array::from(vec![1, 1, 2])),
            Arc::new(UInt8Array::from(vec![1, 1, 2])),
            Arc::new(UInt16Array::from(vec![1, 1, 2])),
            Arc::new(UInt32Array::from(vec![1, 1, 2])),
            Arc::new(UInt64Array::from(vec![1, 1, 2])),
            Arc::new(Float16Array::from(vec![
                half::f16::from_f32(1.5),
                half::f16::from_f32(1.5),
                half::f16::from_f32(2.5),
            ])),
            Arc::new(Float32Array::from(vec![1.5, 1.5, 2.5])),
            Arc::new(Float64Array::from(vec![1.5, 1.5, 2.5])),
            Arc::new(Date32Array::from(vec![1, 1, 2])),
            Arc::new(Time64NanosecondArray::from(vec![1, 1, 2])),
            Arc::new(DurationMicrosecondArray::from(vec![1, 1, 2])),
            Arc::new(TimestampMicrosecondArray::from(vec![1, 1, 2]).with_timezone("UTC")),
            Arc::new(decimal),
            Arc::new(StringArray::from(vec!["a", "a", "b"])),
            Arc::new(LargeStringArray::from(vec!["a", "a", "b"])),
            Arc::new(StringViewArray::from(vec!["a", "a", "b"])),
            Arc::new(BinaryArray::from_vec(vec![b"a", b"a", b"b"])),
            Arc::new(LargeBinaryArray::from_vec(vec![b"a", b"a", b"b"])),
            Arc::new(BinaryViewArray::from_iter_values([
                b"a".as_slice(),
                b"a".as_slice(),
                b"b".as_slice(),
            ])),
            Arc::new(fixed),
        ];
        let reference = RowConverter::new(
            arrays
                .iter()
                .map(|array| SortField::new(array.data_type().clone()))
                .collect(),
        )
        .unwrap()
        .convert_columns(&arrays)
        .unwrap();
        let encoded = encode_typed_rows(arrays, 3).unwrap();
        assert_eq!(encoded[0], reference.row(0).as_ref());
        assert_eq!(encoded[1], reference.row(1).as_ref());
        assert_eq!(encoded[2], reference.row(2).as_ref());
        assert_eq!(encoded[0], encoded[1]);
        assert_ne!(encoded[1], encoded[2]);
    }
}
