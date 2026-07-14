use std::{collections::BTreeMap, sync::Arc};

use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode};
use serde::{Deserialize, Serialize};

use crate::{CdfError, Result};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct CanonicalArrowSchema {
    pub fields: Vec<CanonicalArrowField>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct CanonicalArrowField {
    pub name: String,
    pub data_type: CanonicalArrowType,
    pub nullable: bool,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[non_exhaustive]
pub enum CanonicalArrowType {
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
        unit: CanonicalArrowTimeUnit,
        timezone: Option<String>,
    },
    Date {
        unit: CanonicalArrowDateUnit,
    },
    Time {
        unit: CanonicalArrowTimeUnit,
        bits: u8,
    },
    Duration {
        unit: CanonicalArrowTimeUnit,
    },
    Interval {
        unit: CanonicalArrowIntervalUnit,
    },
    Binary {
        offset_width: u8,
    },
    FixedSizeBinary {
        byte_width: i32,
    },
    BinaryView,
    Utf8 {
        offset_width: u8,
    },
    Utf8View,
    List {
        field: Box<CanonicalArrowField>,
        offset_width: u8,
        view: bool,
    },
    FixedSizeList {
        field: Box<CanonicalArrowField>,
        length: i32,
    },
    Struct {
        fields: Vec<CanonicalArrowField>,
    },
    Map {
        field: Box<CanonicalArrowField>,
        sorted: bool,
    },
    Union {
        fields: Vec<CanonicalArrowUnionField>,
        mode: CanonicalArrowUnionMode,
    },
    Dictionary {
        key: Box<CanonicalArrowType>,
        value: Box<CanonicalArrowType>,
    },
    RunEndEncoded {
        run_ends: Box<CanonicalArrowField>,
        values: Box<CanonicalArrowField>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CanonicalArrowUnionField {
    pub type_id: i8,
    pub field: CanonicalArrowField,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CanonicalArrowUnionMode {
    Sparse,
    Dense,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CanonicalArrowTimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CanonicalArrowDateUnit {
    Day,
    Millisecond,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum CanonicalArrowIntervalUnit {
    YearMonth,
    DayTime,
    MonthDayNano,
}

impl CanonicalArrowField {
    pub fn from_arrow(field: &Field) -> Result<Self> {
        Ok(Self {
            name: field.name().clone(),
            data_type: CanonicalArrowType::from_arrow(field.data_type())?,
            nullable: field.is_nullable(),
            metadata: field
                .metadata()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
    }

    pub fn to_arrow(&self) -> Result<Field> {
        Ok(
            Field::new(&self.name, self.data_type.to_arrow()?, self.nullable)
                .with_metadata(self.metadata.clone().into_iter().collect()),
        )
    }
}

impl CanonicalArrowSchema {
    pub fn from_arrow(schema: &Schema) -> Result<Self> {
        Ok(Self {
            fields: schema
                .fields()
                .iter()
                .map(|field| CanonicalArrowField::from_arrow(field))
                .collect::<Result<Vec<_>>>()?,
            metadata: schema
                .metadata()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        })
    }

    pub fn to_arrow(&self) -> Result<Schema> {
        Ok(Schema::new_with_metadata(
            self.fields
                .iter()
                .map(CanonicalArrowField::to_arrow)
                .collect::<Result<Vec<_>>>()?,
            self.metadata.clone().into_iter().collect(),
        ))
    }
}

impl CanonicalArrowType {
    pub fn from_arrow(data_type: &DataType) -> Result<Self> {
        Ok(match data_type {
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
            DataType::Decimal32(p, s) => Self::Decimal {
                bits: 32,
                precision: *p,
                scale: *s,
            },
            DataType::Decimal64(p, s) => Self::Decimal {
                bits: 64,
                precision: *p,
                scale: *s,
            },
            DataType::Decimal128(p, s) => Self::Decimal {
                bits: 128,
                precision: *p,
                scale: *s,
            },
            DataType::Decimal256(p, s) => Self::Decimal {
                bits: 256,
                precision: *p,
                scale: *s,
            },
            DataType::Timestamp(unit, timezone) => Self::Timestamp {
                unit: CanonicalArrowTimeUnit::from_arrow(unit),
                timezone: timezone.as_deref().map(str::to_owned),
            },
            DataType::Date32 => Self::Date {
                unit: CanonicalArrowDateUnit::Day,
            },
            DataType::Date64 => Self::Date {
                unit: CanonicalArrowDateUnit::Millisecond,
            },
            DataType::Time32(unit) => Self::Time {
                unit: CanonicalArrowTimeUnit::from_arrow(unit),
                bits: 32,
            },
            DataType::Time64(unit) => Self::Time {
                unit: CanonicalArrowTimeUnit::from_arrow(unit),
                bits: 64,
            },
            DataType::Duration(unit) => Self::Duration {
                unit: CanonicalArrowTimeUnit::from_arrow(unit),
            },
            DataType::Interval(unit) => Self::Interval {
                unit: CanonicalArrowIntervalUnit::from_arrow(unit),
            },
            DataType::Binary => Self::Binary { offset_width: 32 },
            DataType::LargeBinary => Self::Binary { offset_width: 64 },
            DataType::FixedSizeBinary(byte_width) => Self::FixedSizeBinary {
                byte_width: *byte_width,
            },
            DataType::BinaryView => Self::BinaryView,
            DataType::Utf8 => Self::Utf8 { offset_width: 32 },
            DataType::LargeUtf8 => Self::Utf8 { offset_width: 64 },
            DataType::Utf8View => Self::Utf8View,
            DataType::List(field) => Self::List {
                field: Box::new(CanonicalArrowField::from_arrow(field)?),
                offset_width: 32,
                view: false,
            },
            DataType::LargeList(field) => Self::List {
                field: Box::new(CanonicalArrowField::from_arrow(field)?),
                offset_width: 64,
                view: false,
            },
            DataType::ListView(field) => Self::List {
                field: Box::new(CanonicalArrowField::from_arrow(field)?),
                offset_width: 32,
                view: true,
            },
            DataType::LargeListView(field) => Self::List {
                field: Box::new(CanonicalArrowField::from_arrow(field)?),
                offset_width: 64,
                view: true,
            },
            DataType::FixedSizeList(field, length) => Self::FixedSizeList {
                field: Box::new(CanonicalArrowField::from_arrow(field)?),
                length: *length,
            },
            DataType::Struct(fields) => {
                let mut names = BTreeMap::new();
                for field in fields {
                    if names.insert(field.name(), ()).is_some() {
                        return Err(CdfError::data(format!(
                            "canonical Arrow struct has duplicate field name {:?}",
                            field.name()
                        )));
                    }
                }
                Self::Struct {
                    fields: fields
                        .iter()
                        .map(|field| CanonicalArrowField::from_arrow(field))
                        .collect::<Result<Vec<_>>>()?,
                }
            }
            DataType::Map(field, sorted) => Self::Map {
                field: Box::new(CanonicalArrowField::from_arrow(field)?),
                sorted: *sorted,
            },
            DataType::Union(fields, mode) => Self::Union {
                fields: fields
                    .iter()
                    .map(|(type_id, field)| {
                        Ok(CanonicalArrowUnionField {
                            type_id,
                            field: CanonicalArrowField::from_arrow(field)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                mode: match mode {
                    UnionMode::Sparse => CanonicalArrowUnionMode::Sparse,
                    UnionMode::Dense => CanonicalArrowUnionMode::Dense,
                },
            },
            DataType::Dictionary(key, value) => Self::Dictionary {
                key: Box::new(Self::from_arrow(key)?),
                value: Box::new(Self::from_arrow(value)?),
            },
            DataType::RunEndEncoded(run_ends, values) => Self::RunEndEncoded {
                run_ends: Box::new(CanonicalArrowField::from_arrow(run_ends)?),
                values: Box::new(CanonicalArrowField::from_arrow(values)?),
            },
        })
    }

    pub fn to_arrow(&self) -> Result<DataType> {
        Ok(match self {
            Self::Null => DataType::Null,
            Self::Boolean => DataType::Boolean,
            Self::Int {
                signed: true,
                bits: 8,
            } => DataType::Int8,
            Self::Int {
                signed: true,
                bits: 16,
            } => DataType::Int16,
            Self::Int {
                signed: true,
                bits: 32,
            } => DataType::Int32,
            Self::Int {
                signed: true,
                bits: 64,
            } => DataType::Int64,
            Self::Int {
                signed: false,
                bits: 8,
            } => DataType::UInt8,
            Self::Int {
                signed: false,
                bits: 16,
            } => DataType::UInt16,
            Self::Int {
                signed: false,
                bits: 32,
            } => DataType::UInt32,
            Self::Int {
                signed: false,
                bits: 64,
            } => DataType::UInt64,
            Self::Int { signed, bits } => {
                return Err(CdfError::data(format!(
                    "invalid canonical integer signed={signed} bits={bits}"
                )));
            }
            Self::Float { bits: 16 } => DataType::Float16,
            Self::Float { bits: 32 } => DataType::Float32,
            Self::Float { bits: 64 } => DataType::Float64,
            Self::Float { bits } => {
                return Err(CdfError::data(format!(
                    "invalid canonical float bits={bits}"
                )));
            }
            Self::Decimal {
                bits: 32,
                precision,
                scale,
            } => DataType::Decimal32(*precision, *scale),
            Self::Decimal {
                bits: 64,
                precision,
                scale,
            } => DataType::Decimal64(*precision, *scale),
            Self::Decimal {
                bits: 128,
                precision,
                scale,
            } => DataType::Decimal128(*precision, *scale),
            Self::Decimal {
                bits: 256,
                precision,
                scale,
            } => DataType::Decimal256(*precision, *scale),
            Self::Decimal { bits, .. } => {
                return Err(CdfError::data(format!(
                    "invalid canonical decimal bits={bits}"
                )));
            }
            Self::Timestamp { unit, timezone } => {
                DataType::Timestamp(unit.to_arrow(), timezone.clone().map(Into::into))
            }
            Self::Date {
                unit: CanonicalArrowDateUnit::Day,
            } => DataType::Date32,
            Self::Date {
                unit: CanonicalArrowDateUnit::Millisecond,
            } => DataType::Date64,
            Self::Time { unit, bits: 32 } => DataType::Time32(unit.to_arrow()),
            Self::Time { unit, bits: 64 } => DataType::Time64(unit.to_arrow()),
            Self::Time { bits, .. } => {
                return Err(CdfError::data(format!(
                    "invalid canonical time bits={bits}"
                )));
            }
            Self::Duration { unit } => DataType::Duration(unit.to_arrow()),
            Self::Interval { unit } => DataType::Interval(unit.to_arrow()),
            Self::Binary { offset_width: 32 } => DataType::Binary,
            Self::Binary { offset_width: 64 } => DataType::LargeBinary,
            Self::Binary { offset_width } => {
                return Err(CdfError::data(format!(
                    "invalid canonical binary offset width={offset_width}"
                )));
            }
            Self::FixedSizeBinary { byte_width } => DataType::FixedSizeBinary(*byte_width),
            Self::BinaryView => DataType::BinaryView,
            Self::Utf8 { offset_width: 32 } => DataType::Utf8,
            Self::Utf8 { offset_width: 64 } => DataType::LargeUtf8,
            Self::Utf8 { offset_width } => {
                return Err(CdfError::data(format!(
                    "invalid canonical utf8 offset width={offset_width}"
                )));
            }
            Self::Utf8View => DataType::Utf8View,
            Self::List {
                field,
                offset_width: 32,
                view: false,
            } => DataType::List(Arc::new(field.to_arrow()?)),
            Self::List {
                field,
                offset_width: 64,
                view: false,
            } => DataType::LargeList(Arc::new(field.to_arrow()?)),
            Self::List {
                field,
                offset_width: 32,
                view: true,
            } => DataType::ListView(Arc::new(field.to_arrow()?)),
            Self::List {
                field,
                offset_width: 64,
                view: true,
            } => DataType::LargeListView(Arc::new(field.to_arrow()?)),
            Self::List {
                offset_width, view, ..
            } => {
                return Err(CdfError::data(format!(
                    "invalid canonical list offset width={offset_width} view={view}"
                )));
            }
            Self::FixedSizeList { field, length } if *length >= 0 => {
                DataType::FixedSizeList(Arc::new(field.to_arrow()?), *length)
            }
            Self::FixedSizeList { length, .. } => {
                return Err(CdfError::data(format!(
                    "invalid fixed-size list length={length}"
                )));
            }
            Self::Struct { fields } => DataType::Struct(
                fields
                    .iter()
                    .map(CanonicalArrowField::to_arrow)
                    .collect::<Result<Vec<_>>>()?
                    .into(),
            ),
            Self::Map { field, sorted } => DataType::Map(Arc::new(field.to_arrow()?), *sorted),
            Self::Union { fields, mode } => DataType::Union(
                UnionFields::try_new(
                    fields.iter().map(|field| field.type_id),
                    fields
                        .iter()
                        .map(|field| field.field.to_arrow())
                        .collect::<Result<Vec<_>>>()?,
                )
                .map_err(CdfError::from)?,
                match mode {
                    CanonicalArrowUnionMode::Sparse => UnionMode::Sparse,
                    CanonicalArrowUnionMode::Dense => UnionMode::Dense,
                },
            ),
            Self::Dictionary { key, value } => {
                DataType::Dictionary(Box::new(key.to_arrow()?), Box::new(value.to_arrow()?))
            }
            Self::RunEndEncoded { run_ends, values } => DataType::RunEndEncoded(
                Arc::new(run_ends.to_arrow()?),
                Arc::new(values.to_arrow()?),
            ),
        })
    }
}

impl CanonicalArrowTimeUnit {
    fn from_arrow(unit: &TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::Second,
            TimeUnit::Millisecond => Self::Millisecond,
            TimeUnit::Microsecond => Self::Microsecond,
            TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
    fn to_arrow(&self) -> TimeUnit {
        match self {
            Self::Second => TimeUnit::Second,
            Self::Millisecond => TimeUnit::Millisecond,
            Self::Microsecond => TimeUnit::Microsecond,
            Self::Nanosecond => TimeUnit::Nanosecond,
        }
    }
}

impl CanonicalArrowIntervalUnit {
    fn from_arrow(unit: &IntervalUnit) -> Self {
        match unit {
            IntervalUnit::YearMonth => Self::YearMonth,
            IntervalUnit::DayTime => Self::DayTime,
            IntervalUnit::MonthDayNano => Self::MonthDayNano,
        }
    }
    fn to_arrow(&self) -> IntervalUnit {
        match self {
            Self::YearMonth => IntervalUnit::YearMonth,
            Self::DayTime => IntervalUnit::DayTime,
            Self::MonthDayNano => IntervalUnit::MonthDayNano,
        }
    }
}
