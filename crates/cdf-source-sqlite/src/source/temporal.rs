use arrow_schema::{DataType, Field, TimeUnit};
use cdf_kernel::{CdfError, CursorValue, Result};
use rusqlite::types::{Value, ValueRef};

use super::schema::{SqliteTemporalEncoding, type_mismatch};

#[derive(Clone, Debug, PartialEq)]
pub(super) enum ObservedCursor {
    I64(i64),
    U64(u64),
    DateDays(i64),
    TimestampMicros {
        micros: i64,
        timezone: Option<String>,
    },
}
impl ObservedCursor {
    pub(super) fn greater_than(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::I64(a), Self::I64(b)) | (Self::DateDays(a), Self::DateDays(b)) => a > b,
            (Self::U64(a), Self::U64(b)) => a > b,
            (Self::TimestampMicros { micros: a, .. }, Self::TimestampMicros { micros: b, .. }) => {
                a > b
            }
            _ => false,
        }
    }
    pub(super) fn into_cursor_value(self) -> CursorValue {
        match self {
            Self::I64(value) | Self::DateDays(value) => CursorValue::I64(value),
            Self::U64(value) => CursorValue::U64(value),
            Self::TimestampMicros { micros, timezone } => {
                CursorValue::TimestampMicros { micros, timezone }
            }
        }
    }
}

pub(super) fn observed_cursor(
    field: &Field,
    value: ValueRef<'_>,
    encoding: Option<SqliteTemporalEncoding>,
) -> Result<ObservedCursor> {
    if matches!(value, ValueRef::Null) {
        return Err(CdfError::data(format!(
            "SQLite cursor field `{}` is NULL",
            field.name()
        )));
    }
    Ok(match field.data_type() {
        DataType::Int64 => match value {
            ValueRef::Integer(value) => ObservedCursor::I64(value),
            _ => return type_mismatch(field, value, "SQLite integer"),
        },
        DataType::UInt64 => match value {
            ValueRef::Integer(value) if value >= 0 => ObservedCursor::U64(value as u64),
            _ => return type_mismatch(field, value, "non-negative SQLite integer"),
        },
        DataType::Date32 => {
            ObservedCursor::DateDays(i64::from(decode_date_days(field, value, encoding)?))
        }
        DataType::Timestamp(unit, timezone) => {
            let raw = decode_timestamp(field, value, encoding, *unit)?;
            let micros = timestamp_unit_to_micros(raw, unit)?;
            ObservedCursor::TimestampMicros {
                micros,
                timezone: timezone.as_ref().map(ToString::to_string),
            }
        }
        _ => return Err(CdfError::internal("validated SQLite cursor type changed")),
    })
}

pub(super) fn decode_date_days(
    field: &Field,
    value: ValueRef<'_>,
    encoding: Option<SqliteTemporalEncoding>,
) -> Result<i32> {
    let encoding = encoding.ok_or_else(|| {
        CdfError::contract(format!(
            "SQLite temporal field `{}` has no compiled encoding",
            field.name()
        ))
    })?;
    let days = match (encoding, value) {
        (SqliteTemporalEncoding::Iso8601Text, ValueRef::Text(value)) => {
            parse_date32(std::str::from_utf8(value).map_err(|error| {
                CdfError::data(format!(
                    "SQLite date field `{}` is not UTF-8: {error}",
                    field.name()
                ))
            })?)
            .map(i64::from)
        }
        (encoding, ValueRef::Integer(value)) => sqlite_integer_to_unix_nanos(value, encoding)
            .and_then(|nanos| i64::try_from(nanos.div_euclid(86_400_000_000_000)).ok()),
        _ => None,
    };
    i32::try_from(days.ok_or_else(|| {
        CdfError::data(format!(
            "SQLite date field `{}` is incompatible with encoding {encoding:?}",
            field.name()
        ))
    })?)
    .map_err(|_| {
        CdfError::data(format!(
            "SQLite date field `{}` is out of Date32 range",
            field.name()
        ))
    })
}

pub(super) fn decode_timestamp(
    field: &Field,
    value: ValueRef<'_>,
    encoding: Option<SqliteTemporalEncoding>,
    unit: TimeUnit,
) -> Result<i64> {
    let encoding = encoding.ok_or_else(|| {
        CdfError::contract(format!(
            "SQLite temporal field `{}` has no compiled encoding",
            field.name()
        ))
    })?;
    let nanos = match (encoding, value) {
        (SqliteTemporalEncoding::Iso8601Text, ValueRef::Text(value)) => std::str::from_utf8(value)
            .ok()
            .and_then(parse_rfc3339_micros)
            .and_then(|value| i128::from(value).checked_mul(1_000)),
        (encoding, ValueRef::Integer(value)) => sqlite_integer_to_unix_nanos(value, encoding),
        _ => None,
    }
    .ok_or_else(|| {
        CdfError::data(format!(
            "SQLite timestamp field `{}` is incompatible with encoding {encoding:?}",
            field.name()
        ))
    })?;
    nanos_to_timestamp_unit(nanos, &unit).ok_or_else(|| {
        CdfError::data(format!(
            "SQLite timestamp field `{}` cannot be represented exactly as {unit:?}",
            field.name()
        ))
    })
}

fn sqlite_integer_to_unix_nanos(value: i64, encoding: SqliteTemporalEncoding) -> Option<i128> {
    let scale = match encoding {
        SqliteTemporalEncoding::UnixSeconds => 1_000_000_000_i128,
        SqliteTemporalEncoding::UnixMilliseconds => 1_000_000,
        SqliteTemporalEncoding::UnixMicroseconds => 1_000,
        SqliteTemporalEncoding::UnixNanoseconds => 1,
        SqliteTemporalEncoding::Iso8601Text => return None,
    };
    i128::from(value).checked_mul(scale)
}

fn nanos_to_timestamp_unit(nanos: i128, unit: &TimeUnit) -> Option<i64> {
    let divisor = match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    if nanos % divisor != 0 {
        return None;
    }
    i64::try_from(nanos / divisor).ok()
}

fn timestamp_unit_to_micros(value: i64, unit: &TimeUnit) -> Result<i64> {
    match unit {
        TimeUnit::Second => value.checked_mul(1_000_000),
        TimeUnit::Millisecond => value.checked_mul(1_000),
        TimeUnit::Microsecond => Some(value),
        TimeUnit::Nanosecond if value % 1_000 == 0 => Some(value / 1_000),
        TimeUnit::Nanosecond => None,
    }
    .ok_or_else(|| {
        CdfError::data(
            "SQLite timestamp cursor cannot be represented exactly in checkpoint microseconds",
        )
    })
}

pub(super) fn bind_cursor_value(
    field: &Field,
    value: &CursorValue,
    encoding: Option<SqliteTemporalEncoding>,
) -> Result<Value> {
    match (field.data_type(), value) {
        (DataType::Int64, CursorValue::I64(value)) => Ok(Value::Integer(*value)),
        (DataType::UInt64, CursorValue::U64(value)) => {
            Ok(Value::Integer(i64::try_from(*value).map_err(|_| {
                CdfError::data("SQLite uint64 cursor exceeds SQLite integer range")
            })?))
        }
        (DataType::Date32, CursorValue::I64(days)) => encode_temporal_cursor(
            days.checked_mul(86_400_000_000)
                .ok_or_else(|| CdfError::data("SQLite date cursor overflowed"))?,
            encoding,
            true,
        ),
        (DataType::Timestamp(..), CursorValue::TimestampMicros { micros, .. }) => {
            encode_temporal_cursor(*micros, encoding, false)
        }
        _ => Err(CdfError::data(
            "SQLite start cursor is incompatible with the pinned schema",
        )),
    }
}

pub(super) fn encode_temporal_cursor(
    micros: i64,
    encoding: Option<SqliteTemporalEncoding>,
    date_only: bool,
) -> Result<Value> {
    let encoding = encoding
        .ok_or_else(|| CdfError::contract("SQLite temporal cursor has no compiled encoding"))?;
    let integer = |divisor: i64| {
        if micros % divisor == 0 {
            Ok(Value::Integer(micros / divisor))
        } else {
            Err(CdfError::data(
                "SQLite checkpoint cannot be represented exactly in the configured temporal encoding",
            ))
        }
    };
    match encoding {
        SqliteTemporalEncoding::Iso8601Text if date_only => {
            Ok(Value::Text(format_date_from_days(micros / 86_400_000_000)?))
        }
        SqliteTemporalEncoding::Iso8601Text => Ok(Value::Text(format_rfc3339_micros(micros)?)),
        SqliteTemporalEncoding::UnixSeconds => integer(1_000_000),
        SqliteTemporalEncoding::UnixMilliseconds => integer(1_000),
        SqliteTemporalEncoding::UnixMicroseconds => Ok(Value::Integer(micros)),
        SqliteTemporalEncoding::UnixNanoseconds => {
            Ok(Value::Integer(micros.checked_mul(1_000).ok_or_else(
                || CdfError::data("SQLite nanosecond checkpoint overflowed"),
            )?))
        }
    }
}

fn parse_date32(value: &str) -> Option<i32> {
    let (year, month, day) = parse_date(value)?;
    i32::try_from(days_from_civil(year, month, day)).ok()
}

fn parse_rfc3339_micros(value: &str) -> Option<i64> {
    let (date, rest) = value.split_once('T')?;
    let (year, month, day) = parse_date(date)?;
    let timezone_start = rest.rfind(['Z', '+', '-']).filter(|index| *index >= 8)?;
    let (time, timezone) = rest.split_at(timezone_start);
    let offset = parse_timezone_offset(timezone)?;
    let (clock, fraction) = time.split_once('.').map_or((time, ""), |parts| parts);
    let mut parts = clock.split(':');
    let hour = parts.next()?.parse::<i64>().ok()?;
    let minute = parts.next()?.parse::<i64>().ok()?;
    let second = parts.next()?.parse::<i64>().ok()?;
    if parts.next().is_some() || hour > 23 || minute > 59 || second > 60 {
        return None;
    }
    let micros = parse_fraction_micros(fraction)?;
    days_from_civil(year, month, day)
        .checked_mul(86_400_000_000)?
        .checked_add(hour * 3_600_000_000 + minute * 60_000_000 + second * 1_000_000 + micros)?
        .checked_sub(offset * 1_000_000)
}

fn parse_date(value: &str) -> Option<(i64, u32, u32)> {
    if value.len() != 10 || value.get(4..5)? != "-" || value.get(7..8)? != "-" {
        return None;
    }
    let year = value.get(..4)?.parse().ok()?;
    let month = value.get(5..7)?.parse().ok()?;
    let day = value.get(8..)?.parse().ok()?;
    if !(1..=12).contains(&month) || day == 0 || day > days_in_month(year, month) {
        None
    } else {
        Some((year, month, day))
    }
}

fn parse_timezone_offset(value: &str) -> Option<i64> {
    if value == "Z" {
        return Some(0);
    }
    let sign = match value.get(..1)? {
        "+" => 1,
        "-" => -1,
        _ => return None,
    };
    if value.len() != 6 || value.get(3..4)? != ":" {
        return None;
    }
    let hours = value.get(1..3)?.parse::<i64>().ok()?;
    let minutes = value.get(4..)?.parse::<i64>().ok()?;
    if hours > 23 || minutes > 59 {
        None
    } else {
        Some(sign * (hours * 3_600 + minutes * 60))
    }
}

fn parse_fraction_micros(value: &str) -> Option<i64> {
    if value.len() > 6 || !value.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    format!("{value:0<6}").parse().ok()
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i64::from(month);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(day) - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month as u32, day as u32)
}

fn format_date_from_days(days: i64) -> Result<String> {
    let (year, month, day) = civil_from_days(days);
    if !(0..=9999).contains(&year) {
        return Err(CdfError::data(
            "SQLite date checkpoint is outside ISO-8601 year range",
        ));
    }
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn format_rfc3339_micros(micros: i64) -> Result<String> {
    let days = micros.div_euclid(86_400_000_000);
    let within = micros.rem_euclid(86_400_000_000);
    let date = format_date_from_days(days)?;
    let hour = within / 3_600_000_000;
    let minute = within % 3_600_000_000 / 60_000_000;
    let second = within % 60_000_000 / 1_000_000;
    let fraction = within % 1_000_000;
    Ok(if fraction == 0 {
        format!("{date}T{hour:02}:{minute:02}:{second:02}Z")
    } else {
        format!("{date}T{hour:02}:{minute:02}:{second:02}.{fraction:06}Z")
    })
}
