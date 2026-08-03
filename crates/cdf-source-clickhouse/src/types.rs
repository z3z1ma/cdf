use std::collections::BTreeSet;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_kernel::{CdfError, ResourceDescriptor, Result, SchemaSource, physical_type, source_name};

use crate::identifier::ClickHouseIdentifier;

pub(crate) const CLICKHOUSE_MAXIMUM_SCHEMA_NODES: usize = 4_096;
/// Finite decoder guard independent of `max_block_size`, which ClickHouse may coalesce when
/// serializing small blocks into ArrowStream record batches.
pub(crate) const CLICKHOUSE_MAXIMUM_RECORD_BATCH_ROWS: usize = 1_000_000;
pub(crate) const CLICKHOUSE_MAXIMUM_VARIABLE_ROW_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const CLICKHOUSE_MAXIMUM_TYPE_TEXT_BYTES: usize = 64 * 1024;
pub(crate) const CLICKHOUSE_MAXIMUM_TYPE_NESTING_DEPTH: usize = 64;
pub(crate) const CLICKHOUSE_MAXIMUM_TYPE_STRUCTURAL_TOKENS: usize = 4_096;
pub(crate) const CLICKHOUSE_MAXIMUM_TIMEZONE_BYTES: usize = 128;
pub(crate) const CLICKHOUSE_TARGET_ARROW_BODY_BYTES: u64 =
    crate::memory::CLICKHOUSE_ARROW_BODY_BYTES as u64;
pub(crate) const CLICKHOUSE_CURSOR_CAST_METADATA_KEY: &str = "cdf:clickhouse_cursor_cast";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ClickHouseCursorCast {
    Signed64,
    Unsigned64,
}

impl ClickHouseCursorCast {
    fn metadata_value(self) -> &'static str {
        match self {
            Self::Signed64 => "signed64",
            Self::Unsigned64 => "unsigned64",
        }
    }
}

pub(crate) fn cursor_cast_for_physical_type(physical_type: &str) -> Option<ClickHouseCursorCast> {
    match physical_type {
        "Int8" | "Int16" | "Int32" => Some(ClickHouseCursorCast::Signed64),
        "UInt8" | "UInt16" | "UInt32" => Some(ClickHouseCursorCast::Unsigned64),
        _ => None,
    }
}

pub(crate) fn with_cursor_cast(field: Field, cast: ClickHouseCursorCast) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(
        CLICKHOUSE_CURSOR_CAST_METADATA_KEY.to_owned(),
        cast.metadata_value().to_owned(),
    );
    field.with_metadata(metadata)
}

pub(crate) fn cursor_cast(field: &Field) -> Result<Option<ClickHouseCursorCast>> {
    match field
        .metadata()
        .get(CLICKHOUSE_CURSOR_CAST_METADATA_KEY)
        .map(String::as_str)
    {
        None => Ok(None),
        Some("signed64") => Ok(Some(ClickHouseCursorCast::Signed64)),
        Some("unsigned64") => Ok(Some(ClickHouseCursorCast::Unsigned64)),
        Some(value) => Err(CdfError::data(format!(
            "ClickHouse field `{}` carries unknown cursor cast metadata `{value}`",
            field.name()
        ))),
    }
}

pub(crate) fn validate_clickhouse_type(field: &str, observed: &str) -> Result<()> {
    let observed = observed.trim();
    validate_clickhouse_type_shape(field, observed)?;
    if supported_clickhouse_type(observed) {
        return Ok(());
    }
    if contains_wrapped_normalization_sensitive_type(observed) {
        return Err(CdfError::data(format!(
            "ClickHouse field `{field}` has source type `{observed}` that wraps UUID, Date, or DateTime inside a container that cannot be normalized exactly by the pinned ArrowStream path; cast the complete field to an exact supported type"
        )));
    }
    Err(CdfError::data(format!(
        "ClickHouse field `{field}` has unsupported source type `{observed}` for the pinned official ArrowStream matrix; cast it explicitly to an exact supported scalar, decimal, string/binary, temporal, array, tuple, map, nullable, low-cardinality, enum, IP, or canonical UUID text type"
    )))
}

fn validate_clickhouse_type_shape(field: &str, observed: &str) -> Result<()> {
    if observed.len() > CLICKHOUSE_MAXIMUM_TYPE_TEXT_BYTES {
        return Err(CdfError::data(format!(
            "ClickHouse field `{field}` has a source type declaration exceeding the {CLICKHOUSE_MAXIMUM_TYPE_TEXT_BYTES}-byte parser limit"
        )));
    }

    // This iterative preflight bounds the work and stack depth of the recursive semantic matcher
    // below. Commas inside quoted Enum labels do not create type nodes.
    let mut depth = 0_usize;
    let mut structural_tokens = 1_usize;
    let mut quoted = false;
    let mut escaped = false;
    for character in observed.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && character == '\\' {
            escaped = true;
            continue;
        }
        if character == '\'' {
            quoted = !quoted;
            continue;
        }
        if quoted {
            continue;
        }
        match character {
            '(' => {
                depth = depth.checked_add(1).ok_or_else(|| {
                    CdfError::data(format!(
                        "ClickHouse field `{field}` source type nesting overflowed"
                    ))
                })?;
                structural_tokens = structural_tokens.saturating_add(1);
                if depth > CLICKHOUSE_MAXIMUM_TYPE_NESTING_DEPTH {
                    return Err(CdfError::data(format!(
                        "ClickHouse field `{field}` source type exceeds the {CLICKHOUSE_MAXIMUM_TYPE_NESTING_DEPTH}-level nesting limit"
                    )));
                }
            }
            ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    CdfError::data(format!(
                        "ClickHouse field `{field}` has malformed source type parentheses"
                    ))
                })?;
            }
            ',' => structural_tokens = structural_tokens.saturating_add(1),
            _ => {}
        }
        if structural_tokens > CLICKHOUSE_MAXIMUM_TYPE_STRUCTURAL_TOKENS {
            return Err(CdfError::data(format!(
                "ClickHouse field `{field}` source type exceeds the {CLICKHOUSE_MAXIMUM_TYPE_STRUCTURAL_TOKENS}-token parser limit"
            )));
        }
    }
    if quoted || escaped || depth != 0 {
        return Err(CdfError::data(format!(
            "ClickHouse field `{field}` has malformed quoted or parenthesized source type syntax"
        )));
    }
    Ok(())
}

fn supported_clickhouse_type(value: &str) -> bool {
    supported_clickhouse_type_at_depth(value, false)
}

fn supported_clickhouse_type_at_depth(value: &str, wrapped: bool) -> bool {
    let value = value.trim();
    let Some((base, arguments)) = outer_type(value) else {
        return match value {
            "UUID" | "Date" | "DateTime" => !wrapped,
            _ => matches!(
                value,
                "Bool"
                    | "Int8"
                    | "Int16"
                    | "Int32"
                    | "Int64"
                    | "UInt8"
                    | "UInt16"
                    | "UInt32"
                    | "UInt64"
                    | "Float32"
                    | "Float64"
                    | "String"
                    | "Date32"
                    | "IPv4"
                    | "IPv6"
            ),
        };
    };
    match base {
        "Nullable" | "LowCardinality" | "Array" => {
            supported_clickhouse_type_at_depth(arguments, true)
        }
        "Map" => split_top_level(arguments).is_some_and(|parts| {
            parts.len() == 2
                && parts
                    .iter()
                    .all(|part| supported_clickhouse_type_at_depth(part, true))
        }),
        "Tuple" => split_top_level(arguments).is_some_and(|parts| {
            !parts.is_empty()
                && parts
                    .iter()
                    .all(|part| supported_clickhouse_type_at_depth(strip_tuple_name(part), true))
        }),
        "FixedString" => arguments.parse::<u32>().is_ok_and(|width| width > 0),
        "Decimal" => decimal_arguments(arguments, 76),
        "Decimal32" => decimal_scale(arguments, 9),
        "Decimal64" => decimal_scale(arguments, 18),
        "Decimal128" => decimal_scale(arguments, 38),
        "Decimal256" => decimal_scale(arguments, 76),
        "DateTime" => !wrapped && timezone_argument(arguments),
        "DateTime64" => datetime64_arguments(arguments),
        "Enum8" | "Enum16" => !arguments.trim().is_empty(),
        _ => false,
    }
}

fn contains_wrapped_normalization_sensitive_type(value: &str) -> bool {
    let Some((base, arguments)) = outer_type(value.trim()) else {
        return false;
    };
    match base {
        "Nullable" | "LowCardinality" | "Array" => contains_normalization_sensitive_type(arguments),
        "Map" | "Tuple" => split_top_level(arguments).is_some_and(|parts| {
            parts
                .iter()
                .any(|part| contains_normalization_sensitive_type(strip_tuple_name(part)))
        }),
        _ => false,
    }
}

fn contains_normalization_sensitive_type(value: &str) -> bool {
    let value = value.trim();
    let Some((base, arguments)) = outer_type(value) else {
        return matches!(value, "UUID" | "Date" | "DateTime");
    };
    if base == "DateTime" {
        return true;
    }
    match base {
        "Nullable" | "LowCardinality" | "Array" => contains_normalization_sensitive_type(arguments),
        "Map" | "Tuple" => split_top_level(arguments).is_some_and(|parts| {
            parts
                .iter()
                .any(|part| contains_normalization_sensitive_type(strip_tuple_name(part)))
        }),
        _ => false,
    }
}

fn outer_type(value: &str) -> Option<(&str, &str)> {
    let open = value.find('(')?;
    if !value.ends_with(')') {
        return None;
    }
    let base = value[..open].trim();
    let arguments = &value[open + 1..value.len() - 1];
    let mut depth = 0_i32;
    let mut quoted = false;
    let mut escaped = false;
    for character in arguments.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && character == '\\' {
            escaped = true;
        } else if character == '\'' {
            quoted = !quoted;
        } else if !quoted {
            match character {
                '(' => depth += 1,
                ')' => depth -= 1,
                _ => {}
            }
            if depth < 0 {
                return None;
            }
        }
    }
    (!quoted && depth == 0 && !base.is_empty()).then_some((base, arguments.trim()))
}

fn split_top_level(value: &str) -> Option<Vec<&str>> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0_i32;
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in value.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && character == '\\' {
            escaped = true;
        } else if character == '\'' {
            quoted = !quoted;
        } else if !quoted {
            match character {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    parts.push(value[start..index].trim());
                    start = index + 1;
                }
                _ => {}
            }
            if depth < 0 {
                return None;
            }
        }
    }
    if quoted || depth != 0 {
        return None;
    }
    parts.push(value[start..].trim());
    parts.iter().all(|part| !part.is_empty()).then_some(parts)
}

fn strip_tuple_name(value: &str) -> &str {
    value
        .split_once(char::is_whitespace)
        .map_or(value, |(_, value)| value.trim())
}

fn decimal_arguments(value: &str, maximum_precision: u8) -> bool {
    split_top_level(value).is_some_and(|parts| {
        if parts.len() != 2 {
            return false;
        }
        let precision = parts[0].parse::<u8>().ok();
        let scale = parts[1].parse::<u8>().ok();
        matches!((precision, scale), (Some(precision), Some(scale)) if precision > 0 && precision <= maximum_precision && scale <= precision)
    })
}

fn decimal_scale(value: &str, precision: u8) -> bool {
    value.parse::<u8>().is_ok_and(|scale| scale <= precision)
}

fn timezone_argument(value: &str) -> bool {
    value.is_empty() || timezone_literal(value).is_some()
}

fn datetime64_arguments(value: &str) -> bool {
    split_top_level(value).is_some_and(|parts| {
        matches!(parts.as_slice(), [scale] if scale.parse::<u8>().is_ok_and(|scale| scale <= 9))
            || matches!(parts.as_slice(), [scale, timezone] if scale.parse::<u8>().is_ok_and(|scale| scale <= 9) && timezone_literal(timezone).is_some())
    })
}

fn timezone_literal(value: &str) -> Option<&str> {
    let value = value.strip_prefix('\'')?.strip_suffix('\'')?;
    (!value.is_empty()
        && value.len() <= CLICKHOUSE_MAXIMUM_TIMEZONE_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'/' | b'+' | b'-' | b':' | b'.')
        }))
    .then_some(value)
}

pub(crate) fn datetime_timezone(value: &str) -> Option<Option<&str>> {
    if value == "DateTime" {
        return Some(None);
    }
    let (base, arguments) = outer_type(value)?;
    if base != "DateTime" {
        return None;
    }
    if arguments.is_empty() {
        return Some(None);
    }
    timezone_literal(arguments).map(Some)
}

pub(crate) fn validate_arrow_field(field: &Field) -> Result<()> {
    if supported_arrow_type(field.data_type()) {
        Ok(())
    } else {
        Err(CdfError::data(format!(
            "ClickHouse field `{}` produced unsupported Arrow type {:?}; cast the source field to an exact Arrow-representable type",
            field.name(),
            field.data_type()
        )))
    }
}

fn supported_arrow_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(..)
        | DataType::Decimal256(..)
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
            _,
        ) => true,
        DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _) => {
            supported_arrow_type(field.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .all(|field| supported_arrow_type(field.data_type())),
        DataType::Map(field, _) => supported_arrow_type(field.data_type()),
        DataType::Dictionary(key, value) => {
            matches!(
                key.as_ref(),
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
            ) && supported_arrow_type(value)
        }
        _ => false,
    }
}

pub(crate) fn validate_resource_shape(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    table: &ClickHouseIdentifier,
    stable_key: Option<&ClickHouseIdentifier>,
) -> Result<()> {
    let _ = table;
    if !matches!(
        &descriptor.schema_source,
        SchemaSource::Declared { .. } | SchemaSource::Discovered { .. }
    ) {
        return Err(CdfError::data(
            "ClickHouse execution requires a declared schema hash or pinned discovered schema",
        ));
    }
    if schema.fields().is_empty() {
        return Err(CdfError::data(
            "ClickHouse table execution requires at least one pinned field",
        ));
    }
    let mut names = BTreeSet::new();
    let mut schema_nodes = 0_usize;
    for field in schema.fields() {
        if !names.insert(field.name().to_owned()) {
            return Err(CdfError::contract(format!(
                "ClickHouse schema repeats field `{}`",
                field.name()
            )));
        }
        ClickHouseIdentifier::new(
            source_name(field)
                .unwrap_or_else(|| field.name())
                .to_owned(),
        )?;
        validate_arrow_field(field)?;
        schema_nodes = schema_nodes.saturating_add(arrow_schema_nodes(field.data_type()));
        if physical_type(field) == Some("UUID") && field.data_type() != &DataType::Utf8 {
            return Err(CdfError::data(format!(
                "ClickHouse UUID field `{}` must use CDF Utf8 with preserved UUID physical metadata",
                field.name()
            )));
        }
        if let Some(cast) = cursor_cast(field)? {
            let valid = match cast {
                ClickHouseCursorCast::Signed64 => {
                    matches!(physical_type(field), Some("Int8" | "Int16" | "Int32"))
                        && field.data_type() == &DataType::Int64
                }
                ClickHouseCursorCast::Unsigned64 => {
                    matches!(physical_type(field), Some("UInt8" | "UInt16" | "UInt32"))
                        && field.data_type() == &DataType::UInt64
                }
            };
            if !valid {
                return Err(CdfError::data(format!(
                    "ClickHouse field `{}` has cursor cast metadata that contradicts its physical type {:?} and Arrow type {:?}",
                    field.name(),
                    physical_type(field),
                    field.data_type()
                )));
            }
            if descriptor
                .cursor
                .as_ref()
                .map(|cursor| cursor.field.as_str())
                != Some(field.name())
            {
                return Err(CdfError::data(format!(
                    "ClickHouse field `{}` carries cursor-only widening metadata but is not the configured cursor",
                    field.name()
                )));
            }
        }
    }
    if schema_nodes > CLICKHOUSE_MAXIMUM_SCHEMA_NODES {
        return Err(CdfError::data(format!(
            "ClickHouse schema contains {schema_nodes} Arrow nodes, exceeding the {CLICKHOUSE_MAXIMUM_SCHEMA_NODES}-node bounded-decode envelope"
        )));
    }
    match (&descriptor.cursor, stable_key) {
        (None, Some(_)) => {
            return Err(CdfError::contract(
                "ClickHouse stable_key is valid only for a cursor resource",
            ));
        }
        (Some(_), None) => {
            return Err(CdfError::contract(
                "ClickHouse cursor resources require a stable_key tie-breaker",
            ));
        }
        _ => {}
    }
    if let Some(cursor) = &descriptor.cursor {
        let field = field_by_name(schema, &cursor.field).ok_or_else(|| {
            CdfError::data(format!(
                "ClickHouse cursor field `{}` is missing from the pinned schema",
                cursor.field
            ))
        })?;
        let expected_cast = physical_type(field).and_then(cursor_cast_for_physical_type);
        if cursor_cast(field)? != expected_cast {
            return Err(CdfError::data(format!(
                "ClickHouse cursor field `{}` does not preserve its required narrow-integer widening evidence",
                cursor.field
            )));
        }
        if matches!(
            field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, _)
        ) {
            return Err(CdfError::contract(format!(
                "ClickHouse cursor field `{}` has sub-microsecond precision that the durable cursor cannot represent exactly; use DateTime64 scale 0..=6",
                cursor.field
            )));
        }
        if field.is_nullable()
            || !matches!(
                field.data_type(),
                DataType::Int64
                    | DataType::UInt64
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Timestamp(..)
            )
        {
            return Err(CdfError::contract(format!(
                "ClickHouse cursor field `{}` must be a non-nullable signed/unsigned integer, date, or timestamp",
                cursor.field
            )));
        }
        let stable_key = stable_key.ok_or_else(|| {
            CdfError::internal("validated ClickHouse cursor resource lost its stable-key authority")
        })?;
        if stable_key.as_str() == cursor.field {
            return Err(CdfError::contract(
                "ClickHouse stable_key must differ from the cursor field",
            ));
        }
        let field =
            field_by_source_or_output_name(schema, stable_key.as_str()).ok_or_else(|| {
                CdfError::contract(format!(
                    "ClickHouse stable_key `{stable_key}` is missing from the pinned schema"
                ))
            })?;
        if field.is_nullable() || !stable_key_type(field.data_type()) {
            return Err(CdfError::contract(format!(
                "ClickHouse stable_key `{stable_key}` must be a non-nullable integer, string/binary, or fixed binary field"
            )));
        }
    }
    Ok(())
}

pub(crate) fn projection_has_variable_width(schema: &Schema, projection: &[String]) -> bool {
    projection.iter().any(|name| {
        field_by_name(schema, name)
            .is_none_or(|field| fixed_width_bytes(field.data_type()).is_none())
    })
}

pub(crate) fn bounded_block_rows(
    schema: &Schema,
    projection: &[String],
    configured: u64,
) -> Result<u64> {
    if configured == 0 {
        return Err(CdfError::contract(
            "ClickHouse max_block_rows must be greater than zero",
        ));
    }
    if projection_has_variable_width(schema, projection) {
        return Ok(configured.clamp(
            1,
            CLICKHOUSE_TARGET_ARROW_BODY_BYTES / CLICKHOUSE_MAXIMUM_VARIABLE_ROW_BYTES,
        ));
    }

    let one_row_bytes = fixed_projection_body_bytes(schema, projection, 1)?;
    if one_row_bytes > CLICKHOUSE_TARGET_ARROW_BODY_BYTES {
        return Err(CdfError::data(format!(
            "one ClickHouse Arrow row requires at least {one_row_bytes} fixed-width body bytes beyond the {CLICKHOUSE_TARGET_ARROW_BODY_BYTES}-byte decode ceiling; project fewer columns or cast the oversized field to a bounded representation"
        )));
    }

    let mut lower = 1_u64;
    let mut upper = configured;
    while lower < upper {
        let midpoint = lower + (upper - lower).div_ceil(2);
        if fixed_projection_body_bytes(schema, projection, midpoint)?
            <= CLICKHOUSE_TARGET_ARROW_BODY_BYTES
        {
            lower = midpoint;
        } else {
            upper = midpoint - 1;
        }
    }
    Ok(lower)
}

fn fixed_width_bytes(data_type: &DataType) -> Option<u64> {
    match data_type {
        DataType::Boolean => Some(0),
        DataType::Int8 | DataType::UInt8 => Some(1),
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => Some(2),
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Decimal32(..) => Some(4),
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Decimal64(..) => Some(8),
        DataType::Interval(arrow_schema::IntervalUnit::YearMonth) => Some(4),
        DataType::Interval(arrow_schema::IntervalUnit::DayTime) => Some(8),
        DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano) => Some(16),
        DataType::Decimal128(..) => Some(16),
        DataType::Decimal256(..) => Some(32),
        DataType::FixedSizeBinary(width) if *width > 0 => u64::try_from(*width).ok(),
        // Nested fixed-size layouts carry child validity buffers and per-buffer IPC padding.
        // Route them through the server-enforced variable-row ceiling instead of pretending
        // their encoded footprint is only the sum of child values.
        DataType::FixedSizeList(..) | DataType::Struct(_) => None,
        _ => None,
    }
}

pub(crate) fn fixed_projection_body_bytes(
    schema: &Schema,
    projection: &[String],
    rows: u64,
) -> Result<u64> {
    projection.iter().try_fold(0_u64, |total, name| {
        let field = field_by_name(schema, name).ok_or_else(|| {
            CdfError::internal(format!(
                "validated ClickHouse projection field `{name}` disappeared while sizing its Arrow body"
            ))
        })?;
        let width = fixed_width_bytes(field.data_type()).ok_or_else(|| {
            CdfError::internal(format!(
                "variable-width ClickHouse field `{name}` entered fixed-width Arrow body sizing"
            ))
        })?;
        let validity_bytes = if field.is_nullable() {
            rows.saturating_add(7) / 8
        } else {
            0
        };
        let value_bytes = if matches!(field.data_type(), DataType::Boolean) {
            rows.saturating_add(7) / 8
        } else {
            rows.saturating_mul(width)
        };
        let field_bytes = align_ipc_buffer(validity_bytes)
            .saturating_add(align_ipc_buffer(value_bytes));
        Ok(total.saturating_add(field_bytes))
    })
}

fn align_ipc_buffer(bytes: u64) -> u64 {
    if bytes == 0 {
        return 0;
    }
    bytes.checked_add(63).map_or(u64::MAX, |value| value & !63)
}

fn arrow_schema_nodes(data_type: &DataType) -> usize {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => 1_usize.saturating_add(arrow_schema_nodes(field.data_type())),
        DataType::Dictionary(_, value) => 1_usize.saturating_add(arrow_schema_nodes(value)),
        DataType::Struct(fields) => fields.iter().fold(1_usize, |total, field| {
            total.saturating_add(arrow_schema_nodes(field.data_type()))
        }),
        _ => 1,
    }
}

fn stable_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
    )
}

pub(crate) fn field_by_name<'a>(schema: &'a Schema, name: &str) -> Option<&'a Field> {
    schema
        .fields()
        .iter()
        .map(AsRef::as_ref)
        .find(|field| field.name() == name)
}

pub(crate) fn field_by_source_or_output_name<'a>(
    schema: &'a Schema,
    name: &str,
) -> Option<&'a Field> {
    schema
        .fields()
        .iter()
        .map(AsRef::as_ref)
        .find(|field| field.name() == name || source_name(field) == Some(name))
}
