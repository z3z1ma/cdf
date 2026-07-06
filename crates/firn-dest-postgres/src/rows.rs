use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Schema, TimeUnit};

use crate::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PostgresStageRow {
    pub(crate) values: Vec<Option<String>>,
    pub(crate) segment_id: String,
    pub(crate) row_index: u64,
}

impl PostgresStageRow {
    pub(crate) fn csv_line(&self, load: &str, loaded_at_ms: i64) -> String {
        let mut fields = self
            .values
            .iter()
            .map(|value| csv_field(value.as_deref()))
            .collect::<Vec<_>>();
        fields.push(csv_field(Some(load)));
        fields.push(csv_field(Some(&self.segment_id)));
        fields.push(csv_field(Some(&self.row_index.to_string())));
        fields.push(csv_field(Some(&loaded_at_ms.to_string())));
        let mut line = fields.join(",");
        line.push('\n');
        line
    }
}

pub(crate) fn validate_schema_matches_plan(
    schema: &Schema,
    columns: &[PostgresColumn],
) -> Result<()> {
    if schema.fields().len() != columns.len() {
        return Err(FirnError::data(format!(
            "Postgres plan has {} column(s) but package schema has {} field(s)",
            columns.len(),
            schema.fields().len()
        )));
    }

    for (field, column) in schema.fields().iter().zip(columns) {
        if field.name() != column.name.as_str() {
            return Err(FirnError::data(format!(
                "Postgres plan column {} does not match package field {}",
                column.name.as_str(),
                field.name()
            )));
        }
        let expected = postgres_type_for_arrow(field.data_type())?;
        if !expected.eq_ignore_ascii_case(&column.data_type) {
            return Err(FirnError::data(format!(
                "Postgres plan column {} has type {} but package field {:?} maps to {}",
                column.name.as_str(),
                column.data_type,
                field.data_type(),
                expected
            )));
        }
        if !column.nullable && field.is_nullable() {
            return Err(FirnError::data(format!(
                "Postgres plan column {} is NOT NULL but package field is nullable",
                column.name.as_str()
            )));
        }
    }

    Ok(())
}

pub(crate) fn batch_row_values(batch: &RecordBatch, row: usize) -> Result<Vec<Option<String>>> {
    batch
        .columns()
        .iter()
        .zip(batch.schema().fields())
        .map(|(array, field)| cell_text(array.as_ref(), field.data_type(), row))
        .collect()
}

fn cell_text(array: &dyn Array, data_type: &DataType, row: usize) -> Result<Option<String>> {
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
        DataType::Decimal128(_, _) => {
            typed::<Decimal128Array>(array, data_type)?.value_as_string(row)
        }
        DataType::Decimal256(_, _) => {
            typed::<Decimal256Array>(array, data_type)?.value_as_string(row)
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
        DataType::Binary => bytea_hex(typed::<BinaryArray>(array, data_type)?.value(row)),
        DataType::LargeBinary => bytea_hex(typed::<LargeBinaryArray>(array, data_type)?.value(row)),
        DataType::Date32 => date_string(i64::from(
            typed::<Date32Array>(array, data_type)?.value(row),
        )),
        DataType::Time64(TimeUnit::Microsecond) => {
            time_string(typed::<Time64MicrosecondArray>(array, data_type)?.value(row))
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => timestamp_string(
            typed::<TimestampMicrosecondArray>(array, data_type)?.value(row),
            timezone.is_some(),
        ),
        other => {
            return Err(FirnError::contract(format!(
                "live Postgres execution does not support Arrow type {other:?}"
            )));
        }
    };

    Ok(Some(value))
}

fn typed<'a, T: 'static>(array: &'a dyn Array, data_type: &DataType) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        FirnError::internal(format!(
            "Arrow array for {data_type:?} had unexpected concrete type"
        ))
    })
}

fn postgres_type_for_arrow(data_type: &DataType) -> Result<String> {
    let value = match data_type {
        DataType::Boolean => "BOOLEAN".to_owned(),
        DataType::Int8 | DataType::Int16 | DataType::UInt8 => "SMALLINT".to_owned(),
        DataType::Int32 | DataType::UInt16 => "INTEGER".to_owned(),
        DataType::Int64 | DataType::UInt32 => "BIGINT".to_owned(),
        DataType::UInt64 => "NUMERIC(20,0)".to_owned(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("NUMERIC({precision},{scale})")
        }
        DataType::Float32 => "REAL".to_owned(),
        DataType::Float64 => "DOUBLE PRECISION".to_owned(),
        DataType::Utf8 | DataType::LargeUtf8 => "TEXT".to_owned(),
        DataType::Binary | DataType::LargeBinary => "BYTEA".to_owned(),
        DataType::Date32 => "DATE".to_owned(),
        DataType::Time64(TimeUnit::Microsecond) => "TIME".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP".to_owned(),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "TIMESTAMPTZ".to_owned(),
        other => {
            return Err(FirnError::contract(format!(
                "live Postgres execution does not support Arrow type {other:?}"
            )));
        }
    };
    Ok(value)
}

fn csv_field(value: Option<&str>) -> String {
    let Some(value) = value else {
        return "\\N".to_owned();
    };
    if value == "\\N" || value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
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
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Decimal128Array};
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

    #[test]
    fn decimal128_rows_preserve_scale_as_numeric_text() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(12, 2),
            true,
        )]));
        let amount: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1234_i128), Some(-5_i128), None])
                .with_precision_and_scale(12, 2)
                .unwrap(),
        );
        let batch = RecordBatch::try_new(schema, vec![amount]).unwrap();

        assert_eq!(
            batch_row_values(&batch, 0).unwrap(),
            vec![Some("12.34".to_owned())]
        );
        assert_eq!(
            batch_row_values(&batch, 1).unwrap(),
            vec![Some("-0.05".to_owned())]
        );
        assert_eq!(batch_row_values(&batch, 2).unwrap(), vec![None]);
    }
}
