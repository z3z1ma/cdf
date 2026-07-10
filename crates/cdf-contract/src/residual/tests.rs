use std::sync::Arc;

use arrow_array::builder::{Int32Builder, MapBuilder, StringBuilder};
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
    Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, DurationMicrosecondArray,
    DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray,
    FixedSizeListArray, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, IntervalDayTimeArray, IntervalMonthDayNanoArray,
    IntervalYearMonthArray, LargeBinaryArray, LargeStringArray, ListArray, ListViewArray,
    NullArray, StringArray, StringViewArray, StructArray, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array, new_null_array,
};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, ScalarBuffer, i256};
use arrow_schema::{DataType, Field};
use half::f16;
use proptest::prelude::*;

use super::*;

fn round_trip(array: ArrayRef) -> Vec<u8> {
    let field = ResidualFieldRef::new(["value"], array.as_ref(), 0).unwrap();
    let bytes = encode_residual_json_v1([field]).unwrap();
    let decoded = decode_residual_json_v1(&bytes).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].path, "/value");
    assert_eq!(decoded[0].array.data_type(), array.data_type());
    assert_eq!(decoded[0].array.to_data(), array.to_data());
    bytes
}

#[test]
fn canonical_golden_orders_fields_and_escapes_original_source_paths() {
    let first = BooleanArray::from(vec![true]);
    let second = Int64Array::from(vec![i64::MAX]);
    let bytes = encode_residual_json_v1([
        ResidualFieldRef::new(["z"], &second, 0).unwrap(),
        ResidualFieldRef::new(["a/b~c"], &first, 0).unwrap(),
    ])
    .unwrap();
    assert_eq!(
        String::from_utf8(bytes.clone()).unwrap(),
        r#"{"v":1,"fields":{"/a~1b~0c":{"arrow_type":{"kind":"boolean"},"encoding":"json","value":true},"/z":{"arrow_type":{"kind":"int","signed":true,"bits":64},"encoding":"base10","value":"9223372036854775807"}}}"#
    );
    assert_eq!(
        encode_residual_json_v1([
            ResidualFieldRef::new(["a/b~c"], &first, 0).unwrap(),
            ResidualFieldRef::new(["z"], &second, 0).unwrap(),
        ])
        .unwrap(),
        bytes
    );
    decode_residual_json_v1(&bytes).unwrap();
}

#[test]
fn scalar_vocabulary_round_trips_exactly() {
    assert_eq!(canonical_float16(f16::from_f32(0.1)), "0.1");
    let decimal256 =
        i256::from_string("123456789012345678901234567890123456789012345678901234567890").unwrap();
    let fixed = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        vec![Some([0_u8, 1, 254, 255].as_slice())].into_iter(),
        4,
    )
    .unwrap();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(NullArray::new(1)),
        Arc::new(BooleanArray::from(vec![true])),
        Arc::new(Int8Array::from(vec![i8::MIN])),
        Arc::new(Int16Array::from(vec![i16::MIN])),
        Arc::new(Int32Array::from(vec![i32::MIN])),
        Arc::new(Int64Array::from(vec![i64::MIN])),
        Arc::new(UInt8Array::from(vec![u8::MAX])),
        Arc::new(UInt16Array::from(vec![u16::MAX])),
        Arc::new(UInt32Array::from(vec![u32::MAX])),
        Arc::new(UInt64Array::from(vec![u64::MAX])),
        Arc::new(Float16Array::from(vec![f16::from_f32(-0.0)])),
        Arc::new(Float32Array::from(vec![-123.5_f32])),
        Arc::new(Float64Array::from(vec![f64::MIN_POSITIVE])),
        Arc::new(
            Decimal32Array::from(vec![-12345])
                .with_precision_and_scale(9, 4)
                .unwrap(),
        ),
        Arc::new(
            Decimal64Array::from(vec![-123456789])
                .with_precision_and_scale(18, 8)
                .unwrap(),
        ),
        Arc::new(
            Decimal128Array::from(vec![-1234567890123456789012345678_i128])
                .with_precision_and_scale(38, 9)
                .unwrap(),
        ),
        Arc::new(
            Decimal256Array::from(vec![decimal256])
                .with_precision_and_scale(76, 12)
                .unwrap(),
        ),
        Arc::new(StringArray::from(vec!["snowman ☃"])),
        Arc::new(LargeStringArray::from(vec!["large"])),
        Arc::new(StringViewArray::from(vec!["view"])),
        Arc::new(BinaryArray::from(vec![&b"\0\xff"[..]])),
        Arc::new(LargeBinaryArray::from(vec![&b"large\0"[..]])),
        Arc::new(fixed),
        Arc::new(BinaryViewArray::from(vec![&b"view\xff"[..]])),
        Arc::new(TimestampSecondArray::from(vec![-1]).with_timezone("UTC")),
        Arc::new(TimestampMillisecondArray::from(vec![i64::MIN + 1])),
        Arc::new(TimestampMicrosecondArray::from(vec![42])),
        Arc::new(TimestampNanosecondArray::from(vec![i64::MAX])),
        Arc::new(Date32Array::from(vec![-1234])),
        Arc::new(Date64Array::from(vec![123456789])),
        Arc::new(Time32SecondArray::from(vec![123])),
        Arc::new(Time32MillisecondArray::from(vec![456])),
        Arc::new(Time64MicrosecondArray::from(vec![789])),
        Arc::new(Time64NanosecondArray::from(vec![987654321])),
        Arc::new(DurationSecondArray::from(vec![-1])),
        Arc::new(DurationMillisecondArray::from(vec![-2])),
        Arc::new(DurationMicrosecondArray::from(vec![-3])),
        Arc::new(DurationNanosecondArray::from(vec![-4])),
        Arc::new(IntervalYearMonthArray::from(vec![-13])),
        Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
            -2, 86_399_999,
        )])),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(-3, 17, -999_999_999),
        ])),
    ];
    for array in arrays {
        round_trip(array);
    }

    for array in [
        Arc::new(Float16Array::from(vec![f16::NAN])) as ArrayRef,
        Arc::new(Float16Array::from(vec![f16::INFINITY])) as ArrayRef,
        Arc::new(Float32Array::from(vec![f32::NEG_INFINITY])) as ArrayRef,
        Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef,
    ] {
        let bytes = round_trip_semantic_float(array.clone());
        assert!(!bytes.is_empty());
    }
}

fn round_trip_semantic_float(array: ArrayRef) -> Vec<u8> {
    let bytes =
        encode_residual_json_v1([ResidualFieldRef::new(["float"], array.as_ref(), 0).unwrap()])
            .unwrap();
    let decoded = decode_residual_json_v1(&bytes).unwrap();
    assert_eq!(decoded[0].array.data_type(), array.data_type());
    match array.data_type() {
        DataType::Float16 => {
            let expected = array
                .as_any()
                .downcast_ref::<Float16Array>()
                .unwrap()
                .value(0);
            let actual = decoded[0]
                .array
                .as_any()
                .downcast_ref::<Float16Array>()
                .unwrap()
                .value(0);
            assert_eq!(expected.is_nan(), actual.is_nan());
            assert_eq!(expected.is_infinite(), actual.is_infinite());
            assert_eq!(expected.is_sign_negative(), actual.is_sign_negative());
        }
        DataType::Float32 => {
            let expected = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0);
            let actual = decoded[0]
                .array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0);
            assert_eq!(expected.is_nan(), actual.is_nan());
            assert_eq!(expected.is_infinite(), actual.is_infinite());
            assert_eq!(expected.is_sign_negative(), actual.is_sign_negative());
        }
        DataType::Float64 => {
            let expected = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
            let actual = decoded[0]
                .array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);
            assert_eq!(expected.is_nan(), actual.is_nan());
            assert_eq!(expected.is_infinite(), actual.is_infinite());
            assert_eq!(expected.is_sign_negative(), actual.is_sign_negative());
        }
        other => panic!("unexpected float type {other}"),
    }
    bytes
}

#[test]
fn nested_list_struct_and_non_string_map_keys_round_trip() {
    let list =
        ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1), None, Some(3)])]);
    round_trip(Arc::new(list));

    let item = Arc::new(Field::new("item", DataType::Int32, true));
    let fixed = FixedSizeListArray::try_new(
        Arc::clone(&item),
        2,
        Arc::new(Int32Array::from(vec![Some(4), Some(5)])),
        None,
    )
    .unwrap();
    round_trip(Arc::new(fixed));

    let view = ListViewArray::try_new(
        item,
        ScalarBuffer::from(vec![0_i32]),
        ScalarBuffer::from(vec![2_i32]),
        Arc::new(Int32Array::from(vec![Some(8), None])),
        None,
    )
    .unwrap();
    round_trip(Arc::new(view));

    let structure = StructArray::from(vec![
        (
            Arc::new(Field::new("z", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![7])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("alpha")])) as ArrayRef,
        ),
    ]);
    round_trip(Arc::new(structure));

    let mut map = MapBuilder::new(None, Int32Builder::new(), StringBuilder::new());
    map.keys().append_value(7);
    map.values().append_value("seven");
    map.keys().append_value(2);
    map.values().append_value("two");
    map.append(true).unwrap();
    round_trip(Arc::new(map.finish()));
}

#[test]
fn null_preserves_recorded_nested_type() {
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    let array = new_null_array(&DataType::List(field), 1);
    round_trip(array);
}

#[test]
fn adversarial_versions_paths_encodings_and_types_fail_closed() {
    let value = Int32Array::from(vec![1]);
    let duplicate = encode_residual_json_v1([
        ResidualFieldRef::new(["same"], &value, 0).unwrap(),
        ResidualFieldRef::new(["same"], &value, 0).unwrap(),
    ])
    .unwrap_err();
    assert!(matches!(duplicate, ResidualCodecError::InvalidPath { .. }));

    let version = br#"{"v":2,"fields":{"/x":{"arrow_type":{"kind":"int","signed":true,"bits":32},"encoding":"base10","value":"1"}}}"#;
    assert_eq!(
        decode_residual_json_v1(version).unwrap_err(),
        ResidualCodecError::UnsupportedVersion { version: 2 }
    );

    let noncanonical = br#"{ "v":1,"fields":{"/x":{"arrow_type":{"kind":"int","signed":true,"bits":32},"encoding":"base10","value":"1"}}}"#;
    assert!(matches!(
        decode_residual_json_v1(noncanonical).unwrap_err(),
        ResidualCodecError::InvalidEnvelope { .. }
    ));

    let bad_path = br#"{"v":1,"fields":{"/bad~2path":{"arrow_type":{"kind":"int","signed":true,"bits":32},"encoding":"base10","value":"1"}}}"#;
    assert!(matches!(
        decode_residual_json_v1(bad_path).unwrap_err(),
        ResidualCodecError::InvalidPath { .. }
    ));

    let wrong_encoding = br#"{"v":1,"fields":{"/x":{"arrow_type":{"kind":"int","signed":true,"bits":32},"encoding":"json","value":"1"}}}"#;
    assert!(matches!(
        decode_residual_json_v1(wrong_encoding).unwrap_err(),
        ResidualCodecError::ExactDecode { .. }
    ));

    let noncanonical_integer = br#"{"v":1,"fields":{"/x":{"arrow_type":{"kind":"int","signed":true,"bits":32},"encoding":"base10","value":"01"}}}"#;
    assert!(matches!(
        decode_residual_json_v1(noncanonical_integer).unwrap_err(),
        ResidualCodecError::ExactDecode { .. }
    ));

    let dictionary_type = DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8));
    let unsupported = new_null_array(&dictionary_type, 1);
    let error =
        encode_residual_json_v1([
            ResidualFieldRef::new(["dictionary"], unsupported.as_ref(), 0).unwrap(),
        ])
        .unwrap_err();
    assert_eq!(error.code(), RESIDUAL_ENCODE_UNSUPPORTED_CODE);
    assert!(matches!(
        error,
        ResidualCodecError::EncodeUnsupported { .. }
    ));
}

proptest! {
    #[test]
    fn signed_integer_property_round_trips(value in any::<i64>()) {
        round_trip(Arc::new(Int64Array::from(vec![value])));
    }

    #[test]
    fn unsigned_integer_property_round_trips(value in any::<u64>()) {
        round_trip(Arc::new(UInt64Array::from(vec![value])));
    }

    #[test]
    fn finite_float_property_round_trips_bits(bits in any::<u64>()) {
        let value = f64::from_bits(bits);
        prop_assume!(value.is_finite());
        let array = Arc::new(Float64Array::from(vec![value])) as ArrayRef;
        let bytes = encode_residual_json_v1([
            ResidualFieldRef::new(["float"], array.as_ref(), 0).unwrap(),
        ]).unwrap();
        let decoded = decode_residual_json_v1(&bytes).unwrap();
        let decoded = decoded[0].array.as_any().downcast_ref::<Float64Array>().unwrap().value(0);
        prop_assert_eq!(decoded.to_bits(), value.to_bits());
    }

    #[test]
    fn finite_float16_property_round_trips_bits(bits in any::<u16>()) {
        let value = f16::from_bits(bits);
        prop_assume!(value.is_finite());
        let array = Arc::new(Float16Array::from(vec![value])) as ArrayRef;
        let bytes = encode_residual_json_v1([
            ResidualFieldRef::new(["float16"], array.as_ref(), 0).unwrap(),
        ]).unwrap();
        let decoded = decode_residual_json_v1(&bytes).unwrap();
        let decoded = decoded[0].array.as_any().downcast_ref::<Float16Array>().unwrap().value(0);
        prop_assert_eq!(decoded.to_bits(), value.to_bits());
    }

    #[test]
    fn decimal_property_round_trips_physical_integer(value in -999_999_999_999_999_999_i128..=999_999_999_999_999_999_i128) {
        let array = Decimal128Array::from(vec![value])
            .with_precision_and_scale(38, 9)
            .unwrap();
        round_trip(Arc::new(array));
    }

    #[test]
    fn list_property_round_trips(values in proptest::collection::vec(proptest::option::of(any::<i32>()), 0..64)) {
        let array = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(values)]);
        round_trip(Arc::new(array));
    }

    #[test]
    fn binary_property_round_trips(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        round_trip(Arc::new(BinaryArray::from(vec![Some(bytes.as_slice())])));
    }

    #[test]
    fn pointer_property_is_canonical(segments in proptest::collection::vec(".*", 1..6)) {
        let pointer = residual_json_pointer(segments.iter().map(String::as_str));
        validate_canonical_pointer(&pointer).unwrap();
    }
}
