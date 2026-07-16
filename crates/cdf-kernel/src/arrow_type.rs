use std::sync::Arc;

use arrow_schema::{
    DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE,
    DataType, Field, Fields, TimeUnit,
};

use crate::{CdfError, Result};

/// Parse CDF's canonical textual Arrow type vocabulary.
///
/// This parser belongs to the kernel because declarative resources, code-backed
/// sources, and recorded schema promotion must assign identical meaning to the
/// same type spelling without depending on a compiler frontend.
fn parse_field_data_type(raw: &str) -> std::result::Result<DataType, String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err("type string is empty".to_owned());
    }

    match value.to_ascii_lowercase().as_str() {
        "string" | "utf8" | "json" => return Ok(DataType::Utf8),
        "large_utf8" => return Ok(DataType::LargeUtf8),
        "boolean" => return Ok(DataType::Boolean),
        "int8" => return Ok(DataType::Int8),
        "int16" => return Ok(DataType::Int16),
        "int32" => return Ok(DataType::Int32),
        "int64" => return Ok(DataType::Int64),
        "uint8" => return Ok(DataType::UInt8),
        "uint16" => return Ok(DataType::UInt16),
        "uint32" => return Ok(DataType::UInt32),
        "uint64" | "u_int64" => return Ok(DataType::UInt64),
        "float16" => return Ok(DataType::Float16),
        "float32" => return Ok(DataType::Float32),
        "float64" => return Ok(DataType::Float64),
        "date32" => return Ok(DataType::Date32),
        "date64" => return Ok(DataType::Date64),
        "timestamp_millis" => {
            return Ok(DataType::Timestamp(TimeUnit::Millisecond, None));
        }
        "timestamp_micros" => {
            return Ok(DataType::Timestamp(TimeUnit::Microsecond, None));
        }
        "binary" => return Ok(DataType::Binary),
        "large_binary" => return Ok(DataType::LargeBinary),
        _ => {}
    }

    if let Some(body) = enclosed_body(value, "decimal", '(', ')')? {
        return decimal_type(value, body, DecimalWidth::Decimal128);
    }
    if let Some(body) = enclosed_body(value, "decimal128", '(', ')')? {
        return decimal_type(value, body, DecimalWidth::Decimal128);
    }
    if let Some(body) = enclosed_body(value, "decimal256", '(', ')')? {
        return decimal_type(value, body, DecimalWidth::Decimal256);
    }
    if let Some(body) = enclosed_body(value, "date", '(', ')')? {
        return date_type(body);
    }
    if let Some(body) = enclosed_body(value, "time", '(', ')')? {
        return time_type(body);
    }
    if let Some(body) = enclosed_body(value, "time32", '(', ')')? {
        return Ok(DataType::Time32(time_unit(
            body,
            &[TimeUnit::Second, TimeUnit::Millisecond],
        )?));
    }
    if let Some(body) = enclosed_body(value, "time64", '(', ')')? {
        return Ok(DataType::Time64(time_unit(
            body,
            &[TimeUnit::Microsecond, TimeUnit::Nanosecond],
        )?));
    }
    if let Some(body) = enclosed_body(value, "timestamp", '(', ')')? {
        return timestamp_type(body);
    }
    if let Some(body) = enclosed_body(value, "duration", '(', ')')? {
        return Ok(DataType::Duration(time_unit(body, ALL_TIME_UNITS)?));
    }
    if let Some(body) = enclosed_body(value, "list", '<', '>')? {
        return Ok(DataType::new_list(parse_field_data_type(body)?, true));
    }
    if let Some(body) = enclosed_body(value, "large_list", '<', '>')? {
        return Ok(DataType::new_large_list(parse_field_data_type(body)?, true));
    }
    if let Some(body) = enclosed_body(value, "struct", '<', '>')? {
        return struct_type(body);
    }
    if let Some(body) = enclosed_body(value, "map", '<', '>')? {
        return map_type(body);
    }

    Err("expected an Arrow type string such as `int64`, `timestamp(us, UTC)`, `list<int64>`, or `struct<name: utf8>`".to_owned())
}

pub fn parse_arrow_field_type(raw: &str) -> Result<DataType> {
    parse_field_data_type(raw).map_err(|error| {
        CdfError::contract(format!("invalid Arrow type declaration {raw:?}: {error}"))
    })
}

#[derive(Clone, Copy)]
enum DecimalWidth {
    Decimal128,
    Decimal256,
}

const ALL_TIME_UNITS: &[TimeUnit] = &[
    TimeUnit::Second,
    TimeUnit::Millisecond,
    TimeUnit::Microsecond,
    TimeUnit::Nanosecond,
];

fn decimal_type(
    raw: &str,
    body: &str,
    width: DecimalWidth,
) -> std::result::Result<DataType, String> {
    let args = split_top_level(body, ',')?;
    if args.len() != 2 {
        return Err(format!("{raw} requires precision and scale"));
    }
    let precision = args[0]
        .trim()
        .parse::<u8>()
        .map_err(|_| format!("{raw} precision must be an unsigned integer"))?;
    let scale = args[1]
        .trim()
        .parse::<i8>()
        .map_err(|_| format!("{raw} scale must be an integer"))?;

    let (max_precision, max_scale) = match width {
        DecimalWidth::Decimal128 => (DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE),
        DecimalWidth::Decimal256 => (DECIMAL256_MAX_PRECISION, DECIMAL256_MAX_SCALE),
    };
    if precision == 0 || precision > max_precision {
        return Err(format!(
            "{raw} precision must be between 1 and {max_precision}"
        ));
    }
    if i16::from(scale).abs() > i16::from(max_scale) {
        return Err(format!(
            "{raw} scale must be between -{max_scale} and {max_scale}"
        ));
    }

    Ok(match width {
        DecimalWidth::Decimal128 => DataType::Decimal128(precision, scale),
        DecimalWidth::Decimal256 => DataType::Decimal256(precision, scale),
    })
}

fn date_type(body: &str) -> std::result::Result<DataType, String> {
    match body.trim().to_ascii_lowercase().as_str() {
        "day" | "days" | "d" => Ok(DataType::Date32),
        "ms" | "millisecond" | "milliseconds" => Ok(DataType::Date64),
        other => Err(format!("unsupported date unit `{other}`")),
    }
}

fn time_type(body: &str) -> std::result::Result<DataType, String> {
    match time_unit(body, ALL_TIME_UNITS)? {
        TimeUnit::Second => Ok(DataType::Time32(TimeUnit::Second)),
        TimeUnit::Millisecond => Ok(DataType::Time32(TimeUnit::Millisecond)),
        TimeUnit::Microsecond => Ok(DataType::Time64(TimeUnit::Microsecond)),
        TimeUnit::Nanosecond => Ok(DataType::Time64(TimeUnit::Nanosecond)),
    }
}

fn timestamp_type(body: &str) -> std::result::Result<DataType, String> {
    let args = split_top_level(body, ',')?;
    if !(1..=2).contains(&args.len()) {
        return Err("timestamp requires a unit and optional timezone".to_owned());
    }
    let unit = time_unit(args[0], ALL_TIME_UNITS)?;
    let timezone = args
        .get(1)
        .map(|timezone| trim_quotes(timezone.trim()).to_owned().into());
    Ok(DataType::Timestamp(unit, timezone))
}

fn struct_type(body: &str) -> std::result::Result<DataType, String> {
    let fields = split_top_level(body, ',')?
        .into_iter()
        .map(|field| {
            let (name, field_type) = split_once_top_level(field, ':')?
                .ok_or_else(|| format!("struct field `{field}` must use `name: type`"))?;
            let name = name.trim();
            if name.is_empty() {
                return Err(format!("struct field `{field}` has an empty name"));
            }
            Ok(Field::new(
                name,
                parse_field_data_type(field_type.trim())?,
                true,
            ))
        })
        .collect::<std::result::Result<Vec<_>, String>>()?;
    Ok(DataType::Struct(Fields::from(fields)))
}

fn map_type(body: &str) -> std::result::Result<DataType, String> {
    let args = split_top_level(body, ',')?;
    if args.len() != 2 {
        return Err("map requires key and value types".to_owned());
    }
    let entries = Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", parse_field_data_type(args[0].trim())?, false),
            Field::new("value", parse_field_data_type(args[1].trim())?, true),
        ])),
        false,
    );
    Ok(DataType::Map(Arc::new(entries), false))
}

fn time_unit(value: &str, allowed: &[TimeUnit]) -> std::result::Result<TimeUnit, String> {
    let unit = match value.trim().to_ascii_lowercase().as_str() {
        "s" | "sec" | "second" | "seconds" => TimeUnit::Second,
        "ms" | "millisecond" | "milliseconds" => TimeUnit::Millisecond,
        "us" | "microsecond" | "microseconds" => TimeUnit::Microsecond,
        "ns" | "nanosecond" | "nanoseconds" => TimeUnit::Nanosecond,
        other => return Err(format!("unsupported time unit `{other}`")),
    };
    if allowed.contains(&unit) {
        Ok(unit)
    } else {
        Err(format!(
            "time unit `{}` is not valid in this type",
            value.trim()
        ))
    }
}

fn enclosed_body<'a>(
    value: &'a str,
    prefix: &str,
    open: char,
    close: char,
) -> std::result::Result<Option<&'a str>, String> {
    let Some(after_prefix) = value.strip_prefix(prefix) else {
        return Ok(None);
    };
    let rest = after_prefix.trim_start();
    if !rest.starts_with(open) {
        return Ok(None);
    }

    let mut depth = 0_i32;
    for (index, ch) in rest.char_indices() {
        if ch == open {
            depth += 1;
        } else if ch == close {
            depth -= 1;
            if depth == 0 {
                let trailing = &rest[index + ch.len_utf8()..];
                if trailing.trim().is_empty() {
                    return Ok(Some(&rest[open.len_utf8()..index]));
                }
                return Err(format!("unexpected trailing content `{}`", trailing.trim()));
            }
        }
    }

    Err(format!("missing closing `{close}`"))
}

fn split_top_level(value: &str, delimiter: char) -> std::result::Result<Vec<&str>, String> {
    let mut parts = Vec::new();
    let mut start = 0;
    for index in top_level_delimiter_indices(value, delimiter)? {
        parts.push(&value[start..index]);
        start = index + delimiter.len_utf8();
    }

    parts.push(&value[start..]);
    Ok(parts)
}

fn split_once_top_level(
    value: &str,
    delimiter: char,
) -> std::result::Result<Option<(&str, &str)>, String> {
    let Some(index) = top_level_delimiter_indices(value, delimiter)?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    Ok(Some((
        &value[..index],
        &value[index + delimiter.len_utf8()..],
    )))
}

fn top_level_delimiter_indices(
    value: &str,
    delimiter: char,
) -> std::result::Result<Vec<usize>, String> {
    let mut indices = Vec::new();
    let mut angle_depth = 0_i32;
    let mut paren_depth = 0_i32;

    for (index, ch) in value.char_indices() {
        match ch {
            '<' => angle_depth += 1,
            '>' => {
                angle_depth -= 1;
                if angle_depth < 0 {
                    return Err("unexpected `>`".to_owned());
                }
            }
            '(' => paren_depth += 1,
            ')' => {
                paren_depth -= 1;
                if paren_depth < 0 {
                    return Err("unexpected `)`".to_owned());
                }
            }
            _ if ch == delimiter && angle_depth == 0 && paren_depth == 0 => {
                indices.push(index);
            }
            _ => {}
        }
    }

    if angle_depth != 0 {
        return Err("unbalanced angle brackets".to_owned());
    }
    if paren_depth != 0 {
        return Err("unbalanced parentheses".to_owned());
    }

    Ok(indices)
}

fn trim_quotes(value: &str) -> &str {
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        if (bytes[0] == b'"' && bytes[value.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[value.len() - 1] == b'\'')
        {
            return &value[1..value.len() - 1];
        }
    }
    value
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, TimeUnit};

    use super::parse_arrow_field_type;

    #[test]
    fn parses_scalar_and_nested_arrow_vocabulary() {
        assert_eq!(
            parse_arrow_field_type("timestamp(us, UTC)").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert!(matches!(
            parse_arrow_field_type("struct<id: int64, labels: list<utf8>>").unwrap(),
            DataType::Struct(_)
        ));
    }

    #[test]
    fn rejects_invalid_arrow_vocabulary() {
        assert!(parse_arrow_field_type("decimal(0,9)").is_err());
        assert!(parse_arrow_field_type("list<int64").is_err());
    }
}
