use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
    Decimal64Array, Decimal128Array, Decimal256Array, DurationMicrosecondArray,
    DurationMillisecondArray, DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray,
    Float16Array, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    IntervalDayTimeArray, IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray,
    LargeStringArray, StringArray, StringViewArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Schema, TimeUnit};
use cdf_kernel::{CdfError, Result, TypeMapping, TypeMappingFidelity};
use rusqlite::types::Value;

use crate::identifier::SqliteIdentifier;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct SqliteColumn {
    pub(crate) name: SqliteIdentifier,
    pub(crate) sqlite_type: String,
    pub(crate) nullable: bool,
    pub(crate) framework_owned: bool,
}

pub(crate) fn columns_for_schema(schema: &Schema) -> Result<Vec<SqliteColumn>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let framework_owned = cdf_contract::is_framework_variant_field(field.as_ref());
            Ok(SqliteColumn {
                name: if framework_owned {
                    SqliteIdentifier::system(field.name())?
                } else {
                    SqliteIdentifier::user(field.name())?
                },
                sqlite_type: sqlite_type_for_arrow(field.data_type())?.to_owned(),
                nullable: field.is_nullable(),
                framework_owned,
            })
        })
        .collect()
}

pub(crate) fn validate_schema_matches_columns(
    schema: &Schema,
    columns: &[SqliteColumn],
) -> Result<()> {
    if schema.fields().len() != columns.len() {
        return Err(CdfError::data(format!(
            "SQLite destination plan has {} columns but package schema has {} fields",
            columns.len(),
            schema.fields().len()
        )));
    }
    for (field, column) in schema.fields().iter().zip(columns) {
        let expected = sqlite_type_for_arrow(field.data_type())?;
        let framework_owned = cdf_contract::is_framework_variant_field(field.as_ref());
        if field.name() != column.name.as_str()
            || expected != column.sqlite_type
            || (!column.nullable && field.is_nullable())
            || framework_owned != column.framework_owned
        {
            return Err(CdfError::data(format!(
                "SQLite destination plan column {} ({}, nullable={}) does not match package field {} ({expected}, nullable={})",
                column.name,
                column.sqlite_type,
                column.nullable,
                field.name(),
                field.is_nullable()
            )));
        }
    }
    Ok(())
}

pub(crate) fn sqlite_type_for_arrow(data_type: &DataType) -> Result<&'static str> {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(
            arrow_schema::IntervalUnit::YearMonth | arrow_schema::IntervalUnit::DayTime,
        ) => Ok("INTEGER"),
        DataType::UInt64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano) => Ok("TEXT"),
        // IEEE-754 payloads are stored as canonical big-endian bytes. SQLite REAL
        // cannot preserve NaN payloads or signed zero bit-for-bit.
        DataType::Float16 | DataType::Float32 | DataType::Float64 => Ok("BLOB"),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("TEXT"),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => Ok("BLOB"),
        other => Err(CdfError::contract(format!(
            "SQLite destination does not support Arrow type {other:?}; choose a lossless scalar mapping before planning"
        ))),
    }
}

pub(crate) fn sqlite_type_mappings() -> Vec<TypeMapping> {
    let lossless = |arrow_type: &str, destination_type: &str| TypeMapping {
        arrow_type: arrow_type.to_owned(),
        destination_type: destination_type.to_owned(),
        fidelity: TypeMappingFidelity::Lossless,
    };
    let unsupported = |arrow_type: &str| TypeMapping {
        arrow_type: arrow_type.to_owned(),
        destination_type: "unsupported".to_owned(),
        fidelity: TypeMappingFidelity::Unsupported,
    };
    vec![
        unsupported("Null"),
        lossless("Boolean", "INTEGER"),
        lossless("Int8", "INTEGER"),
        lossless("Int16", "INTEGER"),
        lossless("Int32", "INTEGER"),
        lossless("Int64", "INTEGER"),
        lossless("UInt8", "INTEGER"),
        lossless("UInt16", "INTEGER"),
        lossless("UInt32", "INTEGER"),
        lossless("UInt64", "TEXT"),
        lossless("Decimal32(p,s)", "TEXT"),
        lossless("Decimal64(p,s)", "TEXT"),
        lossless("Decimal128(p,s)", "TEXT"),
        lossless("Decimal256(p,s)", "TEXT"),
        lossless("Float16", "BLOB"),
        lossless("Float32", "BLOB"),
        lossless("Float64", "BLOB"),
        lossless("Utf8", "TEXT"),
        lossless("LargeUtf8", "TEXT"),
        lossless("Utf8View", "TEXT"),
        lossless("Binary", "BLOB"),
        lossless("LargeBinary", "BLOB"),
        lossless("BinaryView", "BLOB"),
        lossless("FixedSizeBinary(*)", "BLOB"),
        lossless("Date32", "INTEGER"),
        lossless("Date64", "INTEGER"),
        lossless("Time32(second|millisecond)", "INTEGER"),
        lossless("Time64(Microsecond)", "INTEGER"),
        lossless("Time64(Nanosecond)", "INTEGER"),
        lossless("Timestamp(*,*)", "INTEGER"),
        lossless("Duration", "INTEGER"),
        lossless("Interval(YearMonth|DayTime)", "INTEGER"),
        lossless("Interval(MonthDayNano)", "TEXT"),
        unsupported("Struct"),
        unsupported("List*"),
        unsupported("Map"),
        unsupported("Union"),
        unsupported("Dictionary"),
        unsupported("RunEndEncoded"),
    ]
}

pub(crate) fn sqlite_value(array: &dyn Array, data_type: &DataType, row: usize) -> Result<Value> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }
    macro_rules! integer {
        ($array:ty) => {
            Value::Integer(i64::from(typed::<$array>(array, data_type)?.value(row)))
        };
    }
    let value = match data_type {
        DataType::Boolean => Value::Integer(i64::from(
            typed::<BooleanArray>(array, data_type)?.value(row),
        )),
        DataType::Int8 => integer!(Int8Array),
        DataType::Int16 => integer!(Int16Array),
        DataType::Int32 => integer!(Int32Array),
        DataType::Int64 => Value::Integer(typed::<Int64Array>(array, data_type)?.value(row)),
        DataType::UInt8 => integer!(UInt8Array),
        DataType::UInt16 => integer!(UInt16Array),
        DataType::UInt32 => Value::Integer(i64::from(
            typed::<UInt32Array>(array, data_type)?.value(row),
        )),
        DataType::UInt64 => Value::Text(
            typed::<UInt64Array>(array, data_type)?
                .value(row)
                .to_string(),
        ),
        DataType::Decimal32(_, _) => {
            Value::Text(typed::<Decimal32Array>(array, data_type)?.value_as_string(row))
        }
        DataType::Decimal64(_, _) => {
            Value::Text(typed::<Decimal64Array>(array, data_type)?.value_as_string(row))
        }
        DataType::Decimal128(_, _) => {
            Value::Text(typed::<Decimal128Array>(array, data_type)?.value_as_string(row))
        }
        DataType::Decimal256(_, _) => {
            Value::Text(typed::<Decimal256Array>(array, data_type)?.value_as_string(row))
        }
        DataType::Float16 => Value::Blob(
            typed::<Float16Array>(array, data_type)?
                .value(row)
                .to_bits()
                .to_be_bytes()
                .to_vec(),
        ),
        DataType::Float32 => Value::Blob(
            typed::<Float32Array>(array, data_type)?
                .value(row)
                .to_bits()
                .to_be_bytes()
                .to_vec(),
        ),
        DataType::Float64 => Value::Blob(
            typed::<Float64Array>(array, data_type)?
                .value(row)
                .to_bits()
                .to_be_bytes()
                .to_vec(),
        ),
        DataType::Utf8 => Value::Text(
            typed::<StringArray>(array, data_type)?
                .value(row)
                .to_owned(),
        ),
        DataType::LargeUtf8 => Value::Text(
            typed::<LargeStringArray>(array, data_type)?
                .value(row)
                .to_owned(),
        ),
        DataType::Utf8View => Value::Text(
            typed::<StringViewArray>(array, data_type)?
                .value(row)
                .to_owned(),
        ),
        DataType::Binary => {
            Value::Blob(typed::<BinaryArray>(array, data_type)?.value(row).to_vec())
        }
        DataType::LargeBinary => Value::Blob(
            typed::<LargeBinaryArray>(array, data_type)?
                .value(row)
                .to_vec(),
        ),
        DataType::BinaryView => Value::Blob(
            typed::<BinaryViewArray>(array, data_type)?
                .value(row)
                .to_vec(),
        ),
        DataType::FixedSizeBinary(_) => Value::Blob(
            typed::<FixedSizeBinaryArray>(array, data_type)?
                .value(row)
                .to_vec(),
        ),
        DataType::Date32 => integer!(Date32Array),
        DataType::Date64 => Value::Integer(typed::<Date64Array>(array, data_type)?.value(row)),
        DataType::Time32(TimeUnit::Second) => integer!(Time32SecondArray),
        DataType::Time32(TimeUnit::Millisecond) => integer!(Time32MillisecondArray),
        DataType::Time64(TimeUnit::Microsecond) => {
            Value::Integer(typed::<Time64MicrosecondArray>(array, data_type)?.value(row))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Value::Integer(typed::<Time64NanosecondArray>(array, data_type)?.value(row))
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            Value::Integer(typed::<TimestampSecondArray>(array, data_type)?.value(row))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Value::Integer(typed::<TimestampMillisecondArray>(array, data_type)?.value(row))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Value::Integer(typed::<TimestampMicrosecondArray>(array, data_type)?.value(row))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Value::Integer(typed::<TimestampNanosecondArray>(array, data_type)?.value(row))
        }
        DataType::Duration(TimeUnit::Second) => {
            Value::Integer(typed::<DurationSecondArray>(array, data_type)?.value(row))
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            Value::Integer(typed::<DurationMillisecondArray>(array, data_type)?.value(row))
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Value::Integer(typed::<DurationMicrosecondArray>(array, data_type)?.value(row))
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Value::Integer(typed::<DurationNanosecondArray>(array, data_type)?.value(row))
        }
        DataType::Interval(arrow_schema::IntervalUnit::YearMonth) => Value::Integer(i64::from(
            typed::<IntervalYearMonthArray>(array, data_type)?.value(row),
        )),
        DataType::Interval(arrow_schema::IntervalUnit::DayTime) => {
            let value = typed::<IntervalDayTimeArray>(array, data_type)?.value(row);
            let packed =
                (u64::from(value.days as u32) << 32) | u64::from(value.milliseconds as u32);
            Value::Integer(packed as i64)
        }
        DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano) => {
            let value = typed::<IntervalMonthDayNanoArray>(array, data_type)?.value(row);
            let packed = (u128::from(value.months as u32) << 96)
                | (u128::from(value.days as u32) << 64)
                | u128::from(value.nanoseconds as u64);
            Value::Text((packed as i128).to_string())
        }
        other => {
            return Err(CdfError::contract(format!(
                "SQLite destination cannot encode Arrow type {other:?}"
            )));
        }
    };
    Ok(value)
}

fn typed<'a, T: 'static>(array: &'a dyn Array, data_type: &DataType) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        CdfError::internal(format!(
            "Arrow array for {data_type:?} had an unexpected concrete type"
        ))
    })
}
