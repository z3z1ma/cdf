use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
    Decimal64Array, Decimal128Array, Decimal256Array, FixedSizeBinaryArray, Float16Array,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, StringViewArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_kernel::{CdfError, Result, physical_type, semantic};
use cdf_semantic::{
    POSTGRES_JSON_TEXT_MAPPING_PROFILE, POSTGRES_JSON_TEXT_SEMANTIC,
    POSTGRES_JSONB_TEXT_MAPPING_PROFILE, POSTGRES_JSONB_TEXT_SEMANTIC,
    POSTGRES_NUMERIC_TEXT_MAPPING_PROFILE, POSTGRES_NUMERIC_TEXT_SEMANTIC, SemanticAuthority,
    builtin_catalog,
};

use crate::identifiers::PostgresColumn;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PostgresExactValueText {
    Json,
    Jsonb,
    Numeric,
}

impl PostgresExactValueText {
    fn semantic(self) -> &'static str {
        match self {
            Self::Json => POSTGRES_JSON_TEXT_SEMANTIC,
            Self::Jsonb => POSTGRES_JSONB_TEXT_SEMANTIC,
            Self::Numeric => POSTGRES_NUMERIC_TEXT_SEMANTIC,
        }
    }
}

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
        let (expected, expected_semantic) = postgres_mapping_for_field(field)?;
        if !expected.eq_ignore_ascii_case(&column.data_type) {
            return Err(CdfError::data(format!(
                "Postgres plan column {} has type {} but package field {:?} maps to {}",
                column.name.as_str(),
                column.data_type,
                field.data_type(),
                expected
            )));
        }
        if column.semantic.as_deref() != expected_semantic {
            return Err(CdfError::data(format!(
                "Postgres plan column {} semantic {:?} does not match package field semantic {:?}",
                column.name.as_str(),
                column.semantic,
                expected_semantic
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
            let (data_type, exact_semantic) = postgres_mapping_for_field(field)?;
            let column = if cdf_contract::is_framework_variant_field(field.as_ref()) {
                PostgresColumn::system(field.name(), &data_type, field.is_nullable())
            } else {
                PostgresColumn::new(field.name(), &data_type, field.is_nullable())
            }?;
            Ok(match exact_semantic {
                Some(semantic) => column.with_exact_value_text_semantic(semantic),
                None => column,
            })
        })
        .collect()
}

pub fn postgres_type_for_field(field: &Field) -> Result<String> {
    postgres_mapping_for_field(field).map(|(data_type, _)| data_type)
}

fn postgres_mapping_for_field(field: &Field) -> Result<(String, Option<&'static str>)> {
    let Some(exact) = exact_value_text_kind(field)? else {
        return Ok((postgres_type_for_arrow(field.data_type())?, None));
    };
    let data_type = match exact {
        PostgresExactValueText::Json => "JSON".to_owned(),
        PostgresExactValueText::Jsonb => "JSONB".to_owned(),
        PostgresExactValueText::Numeric => resolve_numeric_target_declaration(field)?,
    };
    Ok((data_type, Some(exact.semantic())))
}

pub(crate) fn exact_value_text_kind(field: &Field) -> Result<Option<PostgresExactValueText>> {
    let catalog = builtin_catalog()?;
    let Some(resolved) = catalog.resolve_field(field, SemanticAuthority::Observed)? else {
        return Ok(None);
    };
    let Some(mapping) = catalog.resolve_destination_mapping(&resolved, field, "postgres")? else {
        return Ok(None);
    };
    let exact = match mapping.mapping_profile.as_str() {
        POSTGRES_JSON_TEXT_MAPPING_PROFILE => PostgresExactValueText::Json,
        POSTGRES_JSONB_TEXT_MAPPING_PROFILE => PostgresExactValueText::Jsonb,
        POSTGRES_NUMERIC_TEXT_MAPPING_PROFILE => PostgresExactValueText::Numeric,
        _ => return Ok(None),
    };
    if field.data_type() != &DataType::Utf8 {
        return Err(exact_field_error(
            field,
            format!(
                "requires Arrow Utf8, not {:?}; preserve the canonical value as Utf8 or remove the exact-value semantic tag",
                field.data_type()
            ),
        ));
    }
    let physical = physical_type(field).ok_or_else(|| {
        exact_field_error(
            field,
            "is missing cdf:physical_type; rediscover the PostgreSQL source schema before replay",
        )
    })?;
    let compatible = match exact {
        PostgresExactValueText::Json => physical.trim().eq_ignore_ascii_case("json"),
        PostgresExactValueText::Jsonb => physical.trim().eq_ignore_ascii_case("jsonb"),
        PostgresExactValueText::Numeric => is_numeric_declaration(physical),
    };
    if !compatible {
        return Err(exact_field_error(
            field,
            format!(
                "is incompatible with cdf:physical_type={physical:?}; rediscover the PostgreSQL source schema or remove the exact-value semantic tag"
            ),
        ));
    }
    Ok(Some(exact))
}

fn resolve_numeric_target_declaration(field: &Field) -> Result<String> {
    let declaration = physical_type(field).expect("exact numeric field checked physical metadata");
    let normalized = declaration.trim().to_ascii_lowercase();
    if normalized == "numeric" {
        return Ok("NUMERIC".to_owned());
    }
    let parameters = normalized
        .strip_prefix("numeric(")
        .and_then(|value| value.strip_suffix(')'))
        .ok_or_else(|| exact_field_error(field, "has an invalid PostgreSQL NUMERIC declaration"))?;
    let (precision, scale) = parameters
        .split_once(',')
        .ok_or_else(|| exact_field_error(field, "has an invalid PostgreSQL NUMERIC declaration"))?;
    if scale.contains(',')
        || precision.is_empty()
        || !precision.bytes().all(|byte| byte.is_ascii_digit())
        || scale.is_empty()
        || !scale
            .strip_prefix('-')
            .unwrap_or(scale)
            .bytes()
            .all(|byte| byte.is_ascii_digit())
    {
        return Err(exact_field_error(
            field,
            "has an invalid PostgreSQL NUMERIC declaration",
        ));
    }
    let precision = precision
        .parse::<u16>()
        .map_err(|_| exact_field_error(field, "has an invalid PostgreSQL NUMERIC precision"))?;
    let scale = scale
        .parse::<i16>()
        .map_err(|_| exact_field_error(field, "has an invalid PostgreSQL NUMERIC scale"))?;
    if !(1..=1_000).contains(&precision) || !(-1_000..=1_000).contains(&scale) {
        return Err(exact_field_error(
            field,
            "exceeds PostgreSQL NUMERIC(precision,scale) declaration bounds",
        ));
    }
    Ok(format!("NUMERIC({precision},{scale})"))
}

fn is_numeric_declaration(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    normalized == "numeric"
        || normalized
            .strip_prefix("numeric(")
            .and_then(|value| value.strip_suffix(')'))
            .is_some()
}

fn exact_field_error(field: &Field, detail: impl std::fmt::Display) -> CdfError {
    CdfError::data(format!(
        "Postgres exact-value field `{}` tagged {:?} {detail}",
        field.name(),
        semantic(field)
    ))
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
    use cdf_kernel::{with_physical_type, with_semantic};

    use super::*;

    fn semantic_field(field: Field, reference: &str) -> Field {
        with_semantic(field, &reference.parse().unwrap())
    }

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

    fn exact_field(name: &str, semantic: &str, physical: &str) -> Field {
        semantic_field(
            with_physical_type(Field::new(name, DataType::Utf8, true), physical),
            semantic,
        )
    }

    #[test]
    fn exact_postgres_text_tags_resolve_native_target_declarations() {
        let schema = Schema::new(vec![
            exact_field("document", POSTGRES_JSON_TEXT_SEMANTIC, "json"),
            exact_field("payload", POSTGRES_JSONB_TEXT_SEMANTIC, "jsonb"),
            exact_field("unbounded", POSTGRES_NUMERIC_TEXT_SEMANTIC, "numeric"),
            exact_field(
                "wide",
                POSTGRES_NUMERIC_TEXT_SEMANTIC,
                "numeric(1000,-1000)",
            ),
        ]);

        let columns = postgres_columns_for_schema(&schema).unwrap();
        assert_eq!(
            columns
                .iter()
                .map(|column| (
                    column.data_type.as_str(),
                    column.semantic.as_deref().unwrap()
                ))
                .collect::<Vec<_>>(),
            vec![
                ("JSON", POSTGRES_JSON_TEXT_SEMANTIC),
                ("JSONB", POSTGRES_JSONB_TEXT_SEMANTIC),
                ("NUMERIC", POSTGRES_NUMERIC_TEXT_SEMANTIC),
                ("NUMERIC(1000,-1000)", POSTGRES_NUMERIC_TEXT_SEMANTIC),
            ]
        );
        validate_schema_matches_plan(&schema, &columns).unwrap();
    }

    #[test]
    fn ordinary_physical_only_remains_text_and_unknown_semantics_fail_closed() {
        let ordinary = Schema::new(vec![
            Field::new("ordinary", DataType::Utf8, true),
            with_physical_type(Field::new("physical", DataType::Utf8, true), "numeric"),
        ]);
        let columns = postgres_columns_for_schema(&ordinary).unwrap();
        assert!(columns.iter().all(|column| column.data_type == "TEXT"));
        assert!(columns.iter().all(|column| column.semantic.is_none()));

        let foreign = semantic_field(
            with_physical_type(Field::new("foreign", DataType::Utf8, true), "numeric"),
            "mongodb.decimal128_text@1",
        );
        let error = postgres_columns_for_schema(&Schema::new(vec![foreign])).unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    }

    #[test]
    fn incomplete_or_incompatible_exact_fields_fail_preflight() {
        let cases = [
            semantic_field(
                Field::new("missing", DataType::Utf8, true),
                POSTGRES_NUMERIC_TEXT_SEMANTIC,
            ),
            exact_field("wrong_physical", POSTGRES_JSONB_TEXT_SEMANTIC, "json"),
            exact_field(
                "invalid_numeric",
                POSTGRES_NUMERIC_TEXT_SEMANTIC,
                "numeric(1001,0)",
            ),
            semantic_field(
                with_physical_type(Field::new("wrong_arrow", DataType::Int64, true), "numeric"),
                POSTGRES_NUMERIC_TEXT_SEMANTIC,
            ),
        ];

        for field in cases {
            let error = postgres_columns_for_schema(&Schema::new(vec![field.clone()])).unwrap_err();
            assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
            assert!(error.message.contains(field.name()), "{error}");
        }
    }

    #[test]
    fn schema_validation_rejects_semantic_reinterpretation_even_when_sql_type_matches() {
        let schema = Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(vec![Field::new("code", DataType::Int64, true)].into()),
            true,
        )]);
        let column = PostgresColumn::new("payload", "JSONB", true)
            .unwrap()
            .with_exact_value_text_semantic(POSTGRES_JSONB_TEXT_SEMANTIC);

        let error = validate_schema_matches_plan(&schema, &[column]).unwrap_err();
        assert!(error.message.contains("semantic"), "{error}");
    }
}
