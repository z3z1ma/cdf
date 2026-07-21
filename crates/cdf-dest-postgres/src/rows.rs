use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
    Decimal64Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float16Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, StringViewArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Schema, TimeUnit};

use crate::*;

pub(crate) fn validate_schema_matches_plan(
    schema: &Schema,
    columns: &[PostgresColumn],
) -> Result<()> {
    if schema.fields().len() != columns.len() {
        return Err(CdfError::data(format!(
            "Postgres plan has {} column(s) but package schema has {} field(s)",
            columns.len(),
            schema.fields().len()
        )));
    }

    for (field, column) in schema.fields().iter().zip(columns) {
        if field.name() != column.name.as_str() {
            return Err(CdfError::data(format!(
                "Postgres plan column {} does not match package field {}",
                column.name.as_str(),
                field.name()
            )));
        }
        let expected = postgres_type_for_arrow(field.data_type())?;
        if !expected.eq_ignore_ascii_case(&column.data_type) {
            return Err(CdfError::data(format!(
                "Postgres plan column {} has type {} but package field {:?} maps to {}",
                column.name.as_str(),
                column.data_type,
                field.data_type(),
                expected
            )));
        }
        if !column.nullable && field.is_nullable() {
            return Err(CdfError::data(format!(
                "Postgres plan column {} is NOT NULL but package field is nullable",
                column.name.as_str()
            )));
        }
    }

    Ok(())
}

pub fn postgres_columns_for_schema(schema: &Schema) -> Result<Vec<PostgresColumn>> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = postgres_type_for_arrow(field.data_type())?;
            if cdf_contract::is_framework_variant_field(field.as_ref()) {
                PostgresColumn::system(field.name(), &data_type, field.is_nullable())
            } else {
                PostgresColumn::new(field.name(), &data_type, field.is_nullable())
            }
        })
        .collect()
}

pub(crate) fn correction_cell_text(
    array: &dyn Array,
    data_type: &DataType,
    row: usize,
) -> Result<Option<String>> {
    if array.is_null(row) {
        return Ok(None);
    }

    let value = match data_type {
        DataType::Boolean => typed::<BooleanArray>(array, data_type)?
            .value(row)
            .to_string(),
        DataType::Int8 => i16::from(typed::<Int8Array>(array, data_type)?.value(row)).to_string(),
        DataType::Int16 => typed::<Int16Array>(array, data_type)?
            .value(row)
            .to_string(),
        DataType::Int32 => typed::<Int32Array>(array, data_type)?
            .value(row)
            .to_string(),
        DataType::Int64 => typed::<Int64Array>(array, data_type)?
            .value(row)
            .to_string(),
        DataType::UInt8 => i16::from(typed::<UInt8Array>(array, data_type)?.value(row)).to_string(),
        DataType::UInt16 => {
            i32::from(typed::<UInt16Array>(array, data_type)?.value(row)).to_string()
        }
        DataType::UInt32 => {
            i64::from(typed::<UInt32Array>(array, data_type)?.value(row)).to_string()
        }
        DataType::UInt64 => typed::<UInt64Array>(array, data_type)?
            .value(row)
            .to_string(),
        DataType::Decimal32(_, _) => {
            typed::<Decimal32Array>(array, data_type)?.value_as_string(row)
        }
        DataType::Decimal64(_, _) => {
            typed::<Decimal64Array>(array, data_type)?.value_as_string(row)
        }
        DataType::Decimal128(_, _) => {
            typed::<Decimal128Array>(array, data_type)?.value_as_string(row)
        }
        DataType::Decimal256(_, _) => {
            typed::<Decimal256Array>(array, data_type)?.value_as_string(row)
        }
        DataType::Float16 => {
            f32::from(typed::<Float16Array>(array, data_type)?.value(row)).to_string()
        }
        DataType::Float32 => typed::<Float32Array>(array, data_type)?
            .value(row)
            .to_string(),
        DataType::Float64 => typed::<Float64Array>(array, data_type)?
            .value(row)
            .to_string(),
        DataType::Utf8 => typed::<StringArray>(array, data_type)?
            .value(row)
            .to_owned(),
        DataType::LargeUtf8 => typed::<LargeStringArray>(array, data_type)?
            .value(row)
            .to_owned(),
        DataType::Utf8View => typed::<StringViewArray>(array, data_type)?
            .value(row)
            .to_owned(),
        DataType::Binary => bytea_hex(typed::<BinaryArray>(array, data_type)?.value(row)),
        DataType::LargeBinary => bytea_hex(typed::<LargeBinaryArray>(array, data_type)?.value(row)),
        DataType::BinaryView => bytea_hex(typed::<BinaryViewArray>(array, data_type)?.value(row)),
        DataType::FixedSizeBinary(_) => {
            bytea_hex(typed::<FixedSizeBinaryArray>(array, data_type)?.value(row))
        }
        DataType::Date32 => date_string(i64::from(
            typed::<Date32Array>(array, data_type)?.value(row),
        )),
        DataType::Date64 => timestamp_string(
            scaled_micros(
                typed::<Date64Array>(array, data_type)?.value(row),
                1_000,
                "Date64",
            )?,
            false,
        ),
        DataType::Time32(TimeUnit::Second) => time_string(scaled_micros(
            i64::from(typed::<Time32SecondArray>(array, data_type)?.value(row)),
            1_000_000,
            "Time32 second",
        )?),
        DataType::Time32(TimeUnit::Millisecond) => time_string(scaled_micros(
            i64::from(typed::<Time32MillisecondArray>(array, data_type)?.value(row)),
            1_000,
            "Time32 millisecond",
        )?),
        DataType::Time64(TimeUnit::Microsecond) => {
            time_string(typed::<Time64MicrosecondArray>(array, data_type)?.value(row))
        }
        DataType::Time64(TimeUnit::Nanosecond) => time_string(
            typed::<Time64NanosecondArray>(array, data_type)?
                .value(row)
                .div_euclid(1_000),
        ),
        DataType::Timestamp(TimeUnit::Second, timezone) => timestamp_string(
            scaled_micros(
                typed::<TimestampSecondArray>(array, data_type)?.value(row),
                1_000_000,
                "timestamp second",
            )?,
            timezone.is_some(),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => timestamp_string(
            scaled_micros(
                typed::<TimestampMillisecondArray>(array, data_type)?.value(row),
                1_000,
                "timestamp millisecond",
            )?,
            timezone.is_some(),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => timestamp_string(
            typed::<TimestampMicrosecondArray>(array, data_type)?.value(row),
            timezone.is_some(),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, timezone) => timestamp_string(
            typed::<TimestampNanosecondArray>(array, data_type)?
                .value(row)
                .div_euclid(1_000),
            timezone.is_some(),
        ),
        DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::RunEndEncoded(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => serde_json::to_string(
            &cdf_contract::arrow_value_to_canonical_json(array, row).map_err(|error| {
                CdfError::data(format!("encode Postgres JSONB correction value: {error}"))
            })?,
        )
        .map_err(|error| CdfError::data(format!("serialize Postgres JSONB: {error}")))?,
        other => {
            return Err(CdfError::contract(format!(
                "live Postgres execution does not support Arrow type {other:?}"
            )));
        }
    };

    Ok(Some(value))
}

fn typed<'a, T: 'static>(array: &'a dyn Array, data_type: &DataType) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        CdfError::internal(format!(
            "Arrow array for {data_type:?} had unexpected concrete type"
        ))
    })
}

pub fn postgres_type_for_arrow(data_type: &DataType) -> Result<String> {
    let value = match data_type {
        DataType::Boolean => "BOOLEAN".to_owned(),
        DataType::Int8 | DataType::Int16 | DataType::UInt8 => "SMALLINT".to_owned(),
        DataType::Int32 | DataType::UInt16 => "INTEGER".to_owned(),
        DataType::Int64 | DataType::UInt32 => "BIGINT".to_owned(),
        DataType::UInt64 => "NUMERIC(20,0)".to_owned(),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => {
            format!("NUMERIC({precision},{scale})")
        }
        DataType::Float16 | DataType::Float32 => "REAL".to_owned(),
        DataType::Float64 => "DOUBLE PRECISION".to_owned(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "TEXT".to_owned(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BYTEA".to_owned(),
        DataType::Date32 => "DATE".to_owned(),
        DataType::Date64 => "TIMESTAMP".to_owned(),
        DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => "TIME".to_owned(),
        DataType::Timestamp(_, None) => "TIMESTAMP".to_owned(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ".to_owned(),
        DataType::Null
        | DataType::Struct(_)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Map(_, _)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::RunEndEncoded(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => "JSONB".to_owned(),
        other => {
            return Err(CdfError::contract(format!(
                "Postgres destination does not support Arrow type {other:?}"
            )));
        }
    };
    Ok(value)
}

fn scaled_micros(value: i64, factor: i64, label: &str) -> Result<i64> {
    value
        .checked_mul(factor)
        .ok_or_else(|| CdfError::data(format!("Postgres {label} conversion overflowed")))
}

fn bytea_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(2 + bytes.len() * 2);
    output.push_str("\\x");
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn timestamp_string(micros_since_epoch: i64, timezone_aware: bool) -> String {
    let micros_per_day = 86_400_000_000_i64;
    let days = micros_since_epoch.div_euclid(micros_per_day);
    let micros = micros_since_epoch.rem_euclid(micros_per_day);
    let suffix = if timezone_aware { "+00" } else { "" };
    format!("{} {}{}", date_string(days), time_string(micros), suffix)
}

fn date_string(days_since_epoch: i64) -> String {
    let (year, month, day) = civil_from_days(days_since_epoch);
    format!("{year:04}-{month:02}-{day:02}")
}

fn time_string(micros_since_midnight: i64) -> String {
    let micros = micros_since_midnight.rem_euclid(86_400_000_000);
    let hour = micros / 3_600_000_000;
    let minute = (micros % 3_600_000_000) / 60_000_000;
    let second = (micros % 60_000_000) / 1_000_000;
    let fraction = micros % 1_000_000;
    if fraction == 0 {
        format!("{hour:02}:{minute:02}:{second:02}")
    } else {
        format!("{hour:02}:{minute:02}:{second:02}.{fraction:06}")
    }
}

fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use arrow_schema::Field;

    use super::*;

    #[test]
    fn decimal_schema_maps_to_precision_and_scale_numeric() {
        let schema = Schema::new(vec![
            Field::new("amount", DataType::Decimal128(12, 2), true),
            Field::new("wide_amount", DataType::Decimal256(76, 6), true),
        ]);
        let columns = vec![
            PostgresColumn::new("amount", "NUMERIC(12,2)", true).unwrap(),
            PostgresColumn::new("wide_amount", "NUMERIC(76,6)", true).unwrap(),
        ];

        validate_schema_matches_plan(&schema, &columns).unwrap();
    }
}
