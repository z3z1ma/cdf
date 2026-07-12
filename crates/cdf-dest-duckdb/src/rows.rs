use crate::api::*;
use crate::*;

pub(crate) fn cell_value(array: &dyn Array, data_type: &DataType, row: usize) -> Result<CellValue> {
    if array.is_null(row) {
        return Ok(CellValue {
            value: Value::Null,
            key: CellKey::Null,
        });
    }

    macro_rules! primitive {
        ($array_ty:ty, $value:expr, $key:expr) => {{
            let array = array.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
                CdfError::internal(format!("Arrow array downcast failed for {data_type:?}"))
            })?;
            let value = array.value(row);
            Ok(CellValue {
                value: $value(value),
                key: $key(value),
            })
        }};
    }

    match data_type {
        DataType::Boolean => primitive!(BooleanArray, Value::Boolean, CellKey::Bool),
        DataType::Int8 => primitive!(Int8Array, Value::TinyInt, |value| CellKey::I64(
            value as i64
        )),
        DataType::Int16 => primitive!(Int16Array, Value::SmallInt, |value| CellKey::I64(
            value as i64
        )),
        DataType::Int32 => primitive!(Int32Array, Value::Int, |value| CellKey::I64(value as i64)),
        DataType::Int64 => primitive!(Int64Array, Value::BigInt, CellKey::I64),
        DataType::UInt8 => primitive!(UInt8Array, Value::UTinyInt, |value| CellKey::U64(
            value as u64
        )),
        DataType::UInt16 => primitive!(UInt16Array, Value::USmallInt, |value| CellKey::U64(
            value as u64
        )),
        DataType::UInt32 => {
            primitive!(UInt32Array, Value::UInt, |value| CellKey::U64(value as u64))
        }
        DataType::UInt64 => primitive!(UInt64Array, Value::UBigInt, CellKey::U64),
        DataType::Float32 => primitive!(Float32Array, Value::Float, |value: f32| CellKey::F32(
            value.to_bits()
        )),
        DataType::Float64 => primitive!(Float64Array, Value::Double, |value: f64| CellKey::F64(
            value.to_bits()
        )),
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CdfError::internal("Arrow Utf8 downcast failed"))?;
            let value = array.value(row).to_owned();
            Ok(CellValue {
                value: Value::Text(value.clone()),
                key: CellKey::Text(value),
            })
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| CdfError::internal("Arrow LargeUtf8 downcast failed"))?;
            let value = array.value(row).to_owned();
            Ok(CellValue {
                value: Value::Text(value.clone()),
                key: CellKey::Text(value),
            })
        }
        DataType::Binary => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| CdfError::internal("Arrow Binary downcast failed"))?;
            let value = array.value(row).to_vec();
            Ok(CellValue {
                value: Value::Blob(value.clone()),
                key: CellKey::Blob(value),
            })
        }
        DataType::LargeBinary => {
            let array = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| CdfError::internal("Arrow LargeBinary downcast failed"))?;
            let value = array.value(row).to_vec();
            Ok(CellValue {
                value: Value::Blob(value.clone()),
                key: CellKey::Blob(value),
            })
        }
        DataType::Date32 => primitive!(Date32Array, Value::Date32, CellKey::Date32),
        DataType::Time32(TimeUnit::Second) => {
            let array = array
                .as_any()
                .downcast_ref::<Time32SecondArray>()
                .ok_or_else(|| CdfError::internal("Arrow Time32Second downcast failed"))?;
            let value = array.value(row);
            let micros = i64::from(value) * 1_000_000;
            Ok(CellValue {
                value: Value::Time64(DuckTimeUnit::Second, i64::from(value)),
                key: CellKey::TimeMicros(micros),
            })
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let array = array
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .ok_or_else(|| CdfError::internal("Arrow Time32Millisecond downcast failed"))?;
            let value = array.value(row);
            let micros = i64::from(value) * 1_000;
            Ok(CellValue {
                value: Value::Time64(DuckTimeUnit::Millisecond, i64::from(value)),
                key: CellKey::TimeMicros(micros),
            })
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            primitive!(
                Time64MicrosecondArray,
                |value| Value::Time64(DuckTimeUnit::Microsecond, value),
                CellKey::TimeMicros
            )
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let array = array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .ok_or_else(|| CdfError::internal("Arrow Time64Nanosecond downcast failed"))?;
            let value = array.value(row);
            if value % 1_000 != 0 {
                return Err(CdfError::contract(
                    "DuckDB TIME cannot preserve nanosecond values that are not whole microseconds",
                ));
            }
            Ok(CellValue {
                value: Value::Time64(DuckTimeUnit::Microsecond, value / 1_000),
                key: CellKey::TimeMicros(value / 1_000),
            })
        }
        DataType::Timestamp(TimeUnit::Second, None) => {
            primitive!(
                TimestampSecondArray,
                |value| Value::Timestamp(DuckTimeUnit::Second, value),
                |value| CellKey::TimestampMicros(value * 1_000_000)
            )
        }
        DataType::Timestamp(TimeUnit::Millisecond, None) => {
            primitive!(
                TimestampMillisecondArray,
                |value| Value::Timestamp(DuckTimeUnit::Millisecond, value),
                |value| CellKey::TimestampMicros(value * 1_000)
            )
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            primitive!(
                TimestampMicrosecondArray,
                |value| Value::Timestamp(DuckTimeUnit::Microsecond, value),
                CellKey::TimestampMicros
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| CdfError::internal("Arrow TimestampNanosecond downcast failed"))?;
            let value = array.value(row);
            if value % 1_000 != 0 {
                return Err(CdfError::contract(
                    "DuckDB TIMESTAMP cannot preserve nanosecond values that are not whole microseconds",
                ));
            }
            Ok(CellValue {
                value: Value::Timestamp(DuckTimeUnit::Microsecond, value / 1_000),
                key: CellKey::TimestampMicros(value / 1_000),
            })
        }
        DataType::Timestamp(_, Some(_)) => Err(CdfError::contract(
            "DuckDB timezone-aware timestamp commits require a ratified ICU-enabled path",
        )),
        other => Err(CdfError::contract(format!(
            "DuckDB destination does not support Arrow type {other:?}"
        ))),
    }
}
