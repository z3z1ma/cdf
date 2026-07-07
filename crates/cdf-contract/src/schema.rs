use std::collections::BTreeMap;

use arrow_schema::{DataType, Schema, TimeUnit};
use cdf_kernel::source_name;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedSchema {
    pub fields: Vec<ObservedField>,
}

impl ObservedSchema {
    pub fn from_arrow(schema: &Schema) -> Self {
        Self::from_arrow_with_claims(schema, BTreeMap::new())
    }

    pub fn from_arrow_with_claims(
        schema: &Schema,
        source_claims: BTreeMap<String, SourceTypeClaim>,
    ) -> Self {
        let fields = schema
            .fields()
            .iter()
            .map(|field_ref| {
                let field = field_ref.as_ref();
                let source = source_name(field)
                    .unwrap_or_else(|| field.name())
                    .to_owned();
                let metadata = field
                    .metadata()
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                let source_type = source_claims.get(&source).cloned();

                ObservedField {
                    name: field.name().clone(),
                    source_name: source,
                    arrow_type: ArrowType::from(field.data_type()),
                    nullable: field.is_nullable(),
                    metadata,
                    source_type,
                }
            })
            .collect();

        Self { fields }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObservedField {
    pub name: String,
    pub source_name: String,
    pub arrow_type: ArrowType,
    pub nullable: bool,
    pub metadata: BTreeMap<String, String>,
    pub source_type: Option<SourceTypeClaim>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceTypeClaim {
    Decimal { precision: u8, scale: i8 },
    Timestamp { timezone: TimestampZoneClaim },
    Other { name: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimestampZoneClaim {
    Zoned { zone: String },
    Naive,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArrowType {
    Null,
    Boolean,
    Int {
        signed: bool,
        bits: u8,
    },
    Float {
        bits: u8,
    },
    Decimal {
        bits: u16,
        precision: u8,
        scale: i8,
    },
    Timestamp {
        unit: TimeUnitName,
        timezone: Option<String>,
    },
    Utf8,
    Binary,
    Struct,
    List,
    Map,
    Other {
        display: String,
    },
}

impl ArrowType {
    pub(crate) fn is_nested(&self) -> bool {
        matches!(self, Self::Struct | Self::List | Self::Map)
    }

    pub(crate) fn is_float(&self) -> bool {
        matches!(self, Self::Float { .. })
    }
}

impl From<&DataType> for ArrowType {
    fn from(data_type: &DataType) -> Self {
        match data_type {
            DataType::Null => Self::Null,
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int {
                signed: true,
                bits: 8,
            },
            DataType::Int16 => Self::Int {
                signed: true,
                bits: 16,
            },
            DataType::Int32 => Self::Int {
                signed: true,
                bits: 32,
            },
            DataType::Int64 => Self::Int {
                signed: true,
                bits: 64,
            },
            DataType::UInt8 => Self::Int {
                signed: false,
                bits: 8,
            },
            DataType::UInt16 => Self::Int {
                signed: false,
                bits: 16,
            },
            DataType::UInt32 => Self::Int {
                signed: false,
                bits: 32,
            },
            DataType::UInt64 => Self::Int {
                signed: false,
                bits: 64,
            },
            DataType::Float16 => Self::Float { bits: 16 },
            DataType::Float32 => Self::Float { bits: 32 },
            DataType::Float64 => Self::Float { bits: 64 },
            DataType::Decimal32(precision, scale) => Self::Decimal {
                bits: 32,
                precision: *precision,
                scale: *scale,
            },
            DataType::Decimal64(precision, scale) => Self::Decimal {
                bits: 64,
                precision: *precision,
                scale: *scale,
            },
            DataType::Decimal128(precision, scale) => Self::Decimal {
                bits: 128,
                precision: *precision,
                scale: *scale,
            },
            DataType::Decimal256(precision, scale) => Self::Decimal {
                bits: 256,
                precision: *precision,
                scale: *scale,
            },
            DataType::Timestamp(unit, timezone) => Self::Timestamp {
                unit: TimeUnitName::from(unit),
                timezone: timezone.as_ref().map(ToString::to_string),
            },
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Self::Utf8,
            DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_) => Self::Binary,
            DataType::Struct(_) => Self::Struct,
            DataType::List(_)
            | DataType::LargeList(_)
            | DataType::FixedSizeList(_, _)
            | DataType::ListView(_)
            | DataType::LargeListView(_) => Self::List,
            DataType::Map(_, _) => Self::Map,
            other => Self::Other {
                display: other.to_string(),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnitName {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl From<&TimeUnit> for TimeUnitName {
    fn from(unit: &TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}
