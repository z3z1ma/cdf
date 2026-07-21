use crate::api::*;
use crate::*;
use arrow_array::{
    BinaryViewArray, Date64Array, Decimal32Array, Decimal64Array, Decimal128Array,
    DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, FixedSizeBinaryArray, IntervalDayTimeArray, IntervalMonthDayNanoArray,
    IntervalYearMonthArray, StringViewArray,
};
use arrow_schema::IntervalUnit;

pub(crate) fn cell_value(array: &dyn Array, data_type: &DataType, row: usize) -> Result<CellValue> {
    if array.is_null(row) {
        return Ok(CellValue { value: Value::Null });
    }

    macro_rules! primitive {
        ($array_ty:ty, $value:expr) => {{
            let array = array.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
                CdfError::internal(format!("Arrow array downcast failed for {data_type:?}"))
            })?;
            let value = array.value(row);
            Ok(CellValue {
                value: $value(value),
            })
        }};
    }

    match data_type {
        DataType::Boolean => primitive!(BooleanArray, Value::Boolean),
        DataType::Int8 => primitive!(Int8Array, Value::TinyInt),
        DataType::Int16 => primitive!(Int16Array, Value::SmallInt),
        DataType::Int32 => primitive!(Int32Array, Value::Int),
        DataType::Int64 => primitive!(Int64Array, Value::BigInt),
        DataType::UInt8 => primitive!(UInt8Array, Value::UTinyInt),
        DataType::UInt16 => primitive!(UInt16Array, Value::USmallInt),
        DataType::UInt32 => primitive!(UInt32Array, Value::UInt),
        DataType::UInt64 => primitive!(UInt64Array, Value::UBigInt),
        DataType::Float32 => primitive!(Float32Array, Value::Float),
        DataType::Float64 => primitive!(Float64Array, Value::Double),
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CdfError::internal("Arrow Utf8 downcast failed"))?;
            let value = array.value(row).to_owned();
            Ok(CellValue {
                value: Value::Text(value),
            })
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| CdfError::internal("Arrow LargeUtf8 downcast failed"))?;
            let value = array.value(row).to_owned();
            Ok(CellValue {
                value: Value::Text(value),
            })
        }
        DataType::Utf8View => {
            let array = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| CdfError::internal("Arrow Utf8View downcast failed"))?;
            Ok(CellValue {
                value: Value::Text(array.value(row).to_owned()),
            })
        }
        DataType::Binary => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| CdfError::internal("Arrow Binary downcast failed"))?;
            let value = array.value(row).to_vec();
            Ok(CellValue {
                value: Value::Blob(value),
            })
        }
        DataType::LargeBinary => {
            let array = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| CdfError::internal("Arrow LargeBinary downcast failed"))?;
            let value = array.value(row).to_vec();
            Ok(CellValue {
                value: Value::Blob(value),
            })
        }
        DataType::BinaryView => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .ok_or_else(|| CdfError::internal("Arrow BinaryView downcast failed"))?;
            Ok(CellValue {
                value: Value::Blob(array.value(row).to_vec()),
            })
        }
        DataType::FixedSizeBinary(_) => {
            let array = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| CdfError::internal("Arrow FixedSizeBinary downcast failed"))?;
            Ok(CellValue {
                value: Value::Blob(array.value(row).to_vec()),
            })
        }
        DataType::Decimal32(_, _) => {
            let array = array
                .as_any()
                .downcast_ref::<Decimal32Array>()
                .ok_or_else(|| CdfError::internal("Arrow Decimal32 downcast failed"))?;
            Ok(CellValue {
                value: Value::Text(array.value_as_string(row)),
            })
        }
        DataType::Decimal64(_, _) => {
            let array = array
                .as_any()
                .downcast_ref::<Decimal64Array>()
                .ok_or_else(|| CdfError::internal("Arrow Decimal64 downcast failed"))?;
            Ok(CellValue {
                value: Value::Text(array.value_as_string(row)),
            })
        }
        DataType::Decimal128(_, _) => {
            let array = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| CdfError::internal("Arrow Decimal128 downcast failed"))?;
            Ok(CellValue {
                value: Value::Text(array.value_as_string(row)),
            })
        }
        DataType::Date32 => primitive!(Date32Array, Value::Date32),
        DataType::Date64 => {
            let array = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| CdfError::internal("Arrow Date64 downcast failed"))?;
            let days = array.value(row).div_euclid(86_400_000);
            Ok(CellValue {
                value: Value::Date32(
                    i32::try_from(days).map_err(|_| {
                        CdfError::data("Arrow Date64 value exceeds DuckDB DATE range")
                    })?,
                ),
            })
        }
        DataType::Time32(TimeUnit::Second) => {
            let array = array
                .as_any()
                .downcast_ref::<Time32SecondArray>()
                .ok_or_else(|| CdfError::internal("Arrow Time32Second downcast failed"))?;
            let value = array.value(row);
            Ok(CellValue {
                value: Value::Time64(DuckTimeUnit::Second, i64::from(value)),
            })
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let array = array
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .ok_or_else(|| CdfError::internal("Arrow Time32Millisecond downcast failed"))?;
            let value = array.value(row);
            Ok(CellValue {
                value: Value::Time64(DuckTimeUnit::Millisecond, i64::from(value)),
            })
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            primitive!(Time64MicrosecondArray, |value| Value::Time64(
                DuckTimeUnit::Microsecond,
                value
            ))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let array = array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .ok_or_else(|| CdfError::internal("Arrow Time64Nanosecond downcast failed"))?;
            let value = array.value(row);
            Ok(CellValue {
                value: Value::Time64(DuckTimeUnit::Nanosecond, value),
            })
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| CdfError::internal("Arrow TimestampSecond downcast failed"))?;
            let value = array.value(row);
            Ok(CellValue {
                value: Value::Timestamp(DuckTimeUnit::Second, value),
            })
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| CdfError::internal("Arrow TimestampMillisecond downcast failed"))?;
            let value = array.value(row);
            Ok(CellValue {
                value: Value::Timestamp(DuckTimeUnit::Millisecond, value),
            })
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            primitive!(TimestampMicrosecondArray, |value| Value::Timestamp(
                DuckTimeUnit::Microsecond,
                value
            ))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            let array = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| CdfError::internal("Arrow TimestampNanosecond downcast failed"))?;
            let value = array.value(row);
            Ok(CellValue {
                value: Value::Timestamp(DuckTimeUnit::Nanosecond, value),
            })
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(timezone)) => {
            Err(CdfError::contract(format!(
                "DuckDB TIMESTAMPTZ cannot preserve nanosecond values for timezone {timezone:?}; use a compiled microsecond coercion before correction"
            )))
        }
        DataType::Duration(TimeUnit::Second) => {
            duration_value::<DurationSecondArray>(array, data_type, row, 1_000_000_000)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            duration_value::<DurationMillisecondArray>(array, data_type, row, 1_000_000)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            duration_value::<DurationMicrosecondArray>(array, data_type, row, 1_000)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            duration_value::<DurationNanosecondArray>(array, data_type, row, 1)
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            let array = array
                .as_any()
                .downcast_ref::<IntervalYearMonthArray>()
                .ok_or_else(|| CdfError::internal("Arrow IntervalYearMonth downcast failed"))?;
            Ok(CellValue {
                value: Value::Interval {
                    months: array.value(row),
                    days: 0,
                    nanos: 0,
                },
            })
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            let array = array
                .as_any()
                .downcast_ref::<IntervalDayTimeArray>()
                .ok_or_else(|| CdfError::internal("Arrow IntervalDayTime downcast failed"))?;
            let value = array.value(row);
            Ok(CellValue {
                value: Value::Interval {
                    months: 0,
                    days: value.days,
                    nanos: i64::from(value.milliseconds) * 1_000_000,
                },
            })
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let array = array
                .as_any()
                .downcast_ref::<IntervalMonthDayNanoArray>()
                .ok_or_else(|| CdfError::internal("Arrow IntervalMonthDayNano downcast failed"))?;
            let value = array.value(row);
            Ok(CellValue {
                value: Value::Interval {
                    months: value.months,
                    days: value.days,
                    nanos: value.nanoseconds,
                },
            })
        }
        other => Err(CdfError::contract(format!(
            "DuckDB destination does not support Arrow type {other:?}"
        ))),
    }
}

fn duration_value<T>(
    array: &dyn Array,
    data_type: &DataType,
    row: usize,
    nanos_per_unit: i64,
) -> Result<CellValue>
where
    T: Array + 'static,
    for<'a> &'a T: DurationArrayValue,
{
    let array = array.as_any().downcast_ref::<T>().ok_or_else(|| {
        CdfError::internal(format!("Arrow duration downcast failed for {data_type:?}"))
    })?;
    let value = DurationArrayValue::value(array, row);
    Ok(CellValue {
        value: Value::Interval {
            months: 0,
            days: 0,
            nanos: value.checked_mul(nanos_per_unit).ok_or_else(|| {
                CdfError::data(format!("Arrow duration {data_type:?} exceeds DuckDB range"))
            })?,
        },
    })
}

trait DurationArrayValue {
    fn value(self, row: usize) -> i64;
}

macro_rules! duration_array_value {
    ($array:ty) => {
        impl DurationArrayValue for &$array {
            fn value(self, row: usize) -> i64 {
                self.value(row)
            }
        }
    };
}

duration_array_value!(DurationSecondArray);
duration_array_value!(DurationMillisecondArray);
duration_array_value!(DurationMicrosecondArray);
duration_array_value!(DurationNanosecondArray);
