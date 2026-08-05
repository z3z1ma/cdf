use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use cdf_kernel::{CdfError, Result, TypeMapping, TypeMappingFidelity};
use serde::{Deserialize, Serialize};

use crate::identifier::ClickHouseIdentifier;

pub(crate) const PACKAGE_HASH_COLUMN: &str = "_cdf_package_hash";
pub(crate) const CLICKHOUSE_TYPE_METADATA: &str = "cdf:clickhouse:type";
const MAXIMUM_TYPE_DEPTH: usize = 64;
const MAXIMUM_TYPE_TEXT_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClickHouseColumn {
    pub(crate) name: ClickHouseIdentifier,
    pub(crate) clickhouse_type: String,
    pub(crate) nullable: bool,
    pub(crate) framework_owned: bool,
}

pub(crate) fn columns_for_schema(schema: &Schema) -> Result<Vec<ClickHouseColumn>> {
    cdf_package_contract::validate_logical_output_schema(schema)?;
    if schema.fields().is_empty() {
        return Err(CdfError::contract(
            "ClickHouse destination requires at least one logical column",
        ));
    }
    if schema.fields().len() > 4_096 {
        return Err(CdfError::contract(
            "ClickHouse destination schema exceeds 4,096 columns",
        ));
    }
    schema
        .fields()
        .iter()
        .map(|field| column_for_field(field, 0))
        .collect()
}

fn column_for_field(field: &Field, depth: usize) -> Result<ClickHouseColumn> {
    let framework_owned = cdf_contract::is_framework_variant_field(field);
    let name = if framework_owned {
        ClickHouseIdentifier::framework(field.name().clone())?
    } else {
        ClickHouseIdentifier::user(field.name().clone())?
    };
    let inner = mapped_type(field, depth)?;
    let clickhouse_type = if field.is_nullable() {
        format!("Nullable({inner})")
    } else {
        inner
    };
    Ok(ClickHouseColumn {
        name,
        clickhouse_type,
        nullable: field.is_nullable(),
        framework_owned,
    })
}

fn mapped_type(field: &Field, depth: usize) -> Result<String> {
    if depth > MAXIMUM_TYPE_DEPTH {
        return Err(CdfError::contract(
            "ClickHouse destination type nesting exceeds 64 levels",
        ));
    }
    if let Some(physical) = field.metadata().get(CLICKHOUSE_TYPE_METADATA) {
        validate_declared_type(physical)?;
        if normalized_type(physical).starts_with("Nullable(") {
            return Err(CdfError::contract(format!(
                "ClickHouse type metadata for {} must declare the inner native type; Arrow field nullability owns Nullable(...) wrapping",
                field.name()
            )));
        }
        validate_declared_type_compatibility(field.data_type(), physical)?;
        return Ok(physical.clone());
    }
    match field.data_type() {
        DataType::Boolean => Ok("Bool".to_owned()),
        DataType::Int8 => Ok("Int8".to_owned()),
        DataType::Int16 => Ok("Int16".to_owned()),
        DataType::Int32 => Ok("Int32".to_owned()),
        DataType::Int64 => Ok("Int64".to_owned()),
        DataType::UInt8 => Ok("UInt8".to_owned()),
        DataType::UInt16 => Ok("UInt16".to_owned()),
        DataType::UInt32 => Ok("UInt32".to_owned()),
        DataType::UInt64 => Ok("UInt64".to_owned()),
        DataType::Float32 => Ok("Float32".to_owned()),
        DataType::Float64 => Ok("Float64".to_owned()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Ok("String".to_owned()),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Ok("String".to_owned()),
        DataType::FixedSizeBinary(width) if *width > 0 => Ok(format!("FixedString({width})")),
        DataType::Date32 => Ok("Date32".to_owned()),
        DataType::Timestamp(unit, timezone) => {
            let scale = match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            };
            let timezone = timezone.as_deref().ok_or_else(|| {
                CdfError::contract(format!(
                    "ClickHouse timestamp field {} requires explicit timezone metadata",
                    field.name()
                ))
            })?;
            validate_timezone(timezone)?;
            Ok(format!("DateTime64({scale}, '{timezone}')"))
        }
        DataType::Decimal128(precision, scale) if *precision <= 38 => {
            validate_decimal(*precision, *scale)?;
            Ok(format!("Decimal({precision}, {scale})"))
        }
        DataType::Decimal256(precision, scale) if *precision <= 76 => {
            validate_decimal(*precision, *scale)?;
            Ok(format!("Decimal({precision}, {scale})"))
        }
        DataType::List(child) | DataType::LargeList(child) => {
            Ok(format!("Array({})", nested_type(child, depth + 1)?))
        }
        DataType::FixedSizeList(child, width) if *width > 0 => {
            Ok(format!("Array({})", nested_type(child, depth + 1)?))
        }
        DataType::Struct(fields) => tuple_type(fields, depth + 1),
        DataType::Map(entries, false) => map_type(entries, depth + 1),
        DataType::Dictionary(key, value) if is_integer_dictionary_key(key) => {
            let synthetic = Field::new(field.name(), value.as_ref().clone(), false);
            Ok(format!(
                "LowCardinality({})",
                mapped_type(&synthetic, depth + 1)?
            ))
        }
        unsupported => Err(CdfError::contract(format!(
            "ClickHouse destination field {} has unsupported Arrow type {unsupported:?}; cast it to an exact native ClickHouse mapping before writing",
            field.name()
        ))),
    }
}

fn nested_type(field: &Field, depth: usize) -> Result<String> {
    let inner = mapped_type(field, depth)?;
    if field.is_nullable() {
        Ok(format!("Nullable({inner})"))
    } else {
        Ok(inner)
    }
}

fn tuple_type(fields: &Fields, depth: usize) -> Result<String> {
    if fields.is_empty() {
        return Err(CdfError::contract(
            "ClickHouse destination does not admit empty Arrow structs",
        ));
    }
    let fields = fields
        .iter()
        .map(|field| {
            let name = ClickHouseIdentifier::user(field.name().clone())?;
            Ok(format!("{} {}", name.quoted(), nested_type(field, depth)?))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("Tuple({})", fields.join(", ")))
}

fn map_type(entries: &Field, depth: usize) -> Result<String> {
    let DataType::Struct(fields) = entries.data_type() else {
        return Err(CdfError::contract(
            "ClickHouse Arrow map entries must be a key/value struct",
        ));
    };
    if fields.len() != 2 || fields[0].is_nullable() {
        return Err(CdfError::contract(
            "ClickHouse Arrow maps require exactly one non-null key and one value child",
        ));
    }
    Ok(format!(
        "Map({}, {})",
        nested_type(&fields[0], depth)?,
        nested_type(&fields[1], depth)?
    ))
}

fn validate_decimal(precision: u8, scale: i8) -> Result<()> {
    if precision == 0
        || scale < 0
        || u8::try_from(scale)
            .ok()
            .is_none_or(|scale| scale > precision)
    {
        return Err(CdfError::contract(format!(
            "ClickHouse decimal precision/scale ({precision}, {scale}) is invalid"
        )));
    }
    Ok(())
}

fn validate_timezone(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value.is_ascii()
        || value.bytes().any(|byte| {
            !(byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'_' | b'-' | b'+'))
        })
    {
        return Err(CdfError::contract(
            "ClickHouse timestamp timezone must be a 1..=128-byte IANA-style ASCII name",
        ));
    }
    Ok(())
}

fn validate_declared_type(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAXIMUM_TYPE_TEXT_BYTES
        || value.chars().any(char::is_control)
        || value
            .chars()
            .any(|ch| !(ch.is_ascii_alphanumeric() || "_(), '`.".contains(ch)))
    {
        return Err(CdfError::contract(
            "cdf:clickhouse:type must be bounded control-free ClickHouse type text",
        ));
    }
    let mut depth = 0_usize;
    for character in value.chars() {
        match character {
            '(' => {
                depth = depth.checked_add(1).ok_or_else(|| {
                    CdfError::contract("ClickHouse declared type depth overflowed")
                })?;
                if depth > MAXIMUM_TYPE_DEPTH {
                    return Err(CdfError::contract(
                        "ClickHouse declared type nesting exceeds 64 levels",
                    ));
                }
            }
            ')' => {
                depth = depth.checked_sub(1).ok_or_else(|| {
                    CdfError::contract("ClickHouse declared type has unmatched closing parenthesis")
                })?;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(CdfError::contract(
            "ClickHouse declared type has unmatched opening parenthesis",
        ));
    }
    Ok(())
}

fn validate_declared_type_compatibility(data_type: &DataType, declared: &str) -> Result<()> {
    let base = declared
        .strip_prefix("Nullable(")
        .and_then(|value| value.strip_suffix(')'))
        .unwrap_or(declared);
    let compatible = match data_type {
        DataType::FixedSizeBinary(16) => {
            base == "UUID" || base == "IPv6" || base == "FixedString(16)"
        }
        DataType::FixedSizeBinary(4) => base == "IPv4" || base == "FixedString(4)",
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            base.starts_with("Enum") || base.starts_with("Int")
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            base.starts_with("UInt")
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            base == "String" || base.starts_with("LowCardinality(String")
        }
        _ => false,
    };
    if compatible {
        Ok(())
    } else {
        Err(CdfError::contract(format!(
            "Arrow type {data_type:?} is incompatible with declared ClickHouse type {declared}"
        )))
    }
}

fn is_integer_dictionary_key(data_type: &DataType) -> bool {
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
    )
}

pub(crate) fn physical_columns(logical: &[ClickHouseColumn]) -> Result<Vec<ClickHouseColumn>> {
    let mut columns = logical.to_vec();
    columns.push(ClickHouseColumn {
        name: ClickHouseIdentifier::framework(PACKAGE_HASH_COLUMN)?,
        clickhouse_type: "FixedString(32)".to_owned(),
        nullable: false,
        framework_owned: true,
    });
    columns.push(ClickHouseColumn {
        name: ClickHouseIdentifier::framework(cdf_package_contract::CDF_PACKAGE_ROW_ORD_FIELD)?,
        clickhouse_type: "UInt64".to_owned(),
        nullable: false,
        framework_owned: true,
    });
    Ok(columns)
}

pub(crate) fn type_mappings() -> Vec<TypeMapping> {
    let mapping = |arrow_type: &str, destination_type: &str, fidelity| TypeMapping {
        arrow_type: arrow_type.to_owned(),
        destination_type: destination_type.to_owned(),
        fidelity,
    };
    vec![
        mapping("Boolean", "Bool", TypeMappingFidelity::Lossless),
        mapping("Int8", "Int8", TypeMappingFidelity::Lossless),
        mapping("Int16", "Int16", TypeMappingFidelity::Lossless),
        mapping("Int32", "Int32", TypeMappingFidelity::Lossless),
        mapping("Int64", "Int64", TypeMappingFidelity::Lossless),
        mapping("UInt8", "UInt8", TypeMappingFidelity::Lossless),
        mapping("UInt16", "UInt16", TypeMappingFidelity::Lossless),
        mapping("UInt32", "UInt32", TypeMappingFidelity::Lossless),
        mapping("UInt64", "UInt64", TypeMappingFidelity::Lossless),
        mapping("Float32", "Float32", TypeMappingFidelity::Lossless),
        mapping("Float64", "Float64", TypeMappingFidelity::Lossless),
        mapping("Utf8", "String", TypeMappingFidelity::Lossless),
        mapping("LargeUtf8", "String", TypeMappingFidelity::Lossless),
        mapping("Utf8View", "String", TypeMappingFidelity::Lossless),
        mapping("Binary", "String", TypeMappingFidelity::Lossless),
        mapping("LargeBinary", "String", TypeMappingFidelity::Lossless),
        mapping("BinaryView", "String", TypeMappingFidelity::Lossless),
        mapping(
            "FixedSizeBinary(*)",
            "FixedString/UUID/IP with metadata",
            TypeMappingFidelity::Lossless,
        ),
        mapping("Date32", "Date32", TypeMappingFidelity::Lossless),
        mapping(
            "Timestamp(*,timezone)",
            "DateTime64 with explicit timezone",
            TypeMappingFidelity::Lossless,
        ),
        mapping("Decimal128(p,s)", "Decimal", TypeMappingFidelity::Lossless),
        mapping("Decimal256(p,s)", "Decimal", TypeMappingFidelity::Lossless),
        mapping("List", "Array", TypeMappingFidelity::Lossless),
        mapping("LargeList", "Array", TypeMappingFidelity::Lossless),
        mapping("FixedSizeList", "Array", TypeMappingFidelity::Lossless),
        mapping("Struct", "Tuple", TypeMappingFidelity::Lossless),
        mapping("Map", "Map", TypeMappingFidelity::Lossless),
        mapping(
            "Dictionary",
            "LowCardinality",
            TypeMappingFidelity::Lossless,
        ),
        mapping("Union", "unsupported", TypeMappingFidelity::Unsupported),
        mapping(
            "RunEndEncoded",
            "unsupported",
            TypeMappingFidelity::Unsupported,
        ),
    ]
}

pub(crate) fn normalized_type(value: &str) -> String {
    value
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}
