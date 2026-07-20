use std::cmp::Ordering;

use arrow_arith::aggregate::{
    max, max_binary, max_binary_view, max_boolean, max_fixed_size_binary, max_string,
    max_string_view, min, min_binary, min_binary_view, min_boolean, min_fixed_size_binary,
    min_string, min_string_view,
};
use arrow_array::{
    Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array, Decimal32Array,
    Decimal64Array, Decimal128Array, DurationMicrosecondArray, DurationMillisecondArray,
    DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, StringViewArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit, UnionMode};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de::MapAccess, de::Visitor};

use crate::{
    CanonicalArrowDateUnit, CanonicalArrowIntervalUnit, CanonicalArrowTimeUnit,
    CanonicalArrowUnionMode, CdfError, Result,
};

pub const STATISTICS_MODEL_VERSION: u16 = 1;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchStats {
    pub columns: Box<[ColumnStats]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnStats {
    pub field_path: Box<[Box<str>]>,
    pub data_type: StatisticsArrowType,
    pub row_count: u64,
    pub null_count: u64,
    pub minimum: Option<TypedScalar>,
    pub maximum: Option<TypedScalar>,
    pub completeness: StatisticsCompleteness,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum StatisticsCompleteness {
    Complete,
    Incomplete { reason: IncompleteStatisticsReason },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncompleteStatisticsReason {
    UnsupportedType,
    NanObserved,
    NonFiniteObserved,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum TypedScalar {
    Boolean(bool),
    Signed(i64),
    Unsigned(u64),
    Float32Bits(u32),
    Float64Bits(u64),
    Decimal32(i32),
    Decimal64(i64),
    Decimal128(i128),
    Utf8(Box<str>),
    Binary(Box<[u8]>),
}

/// Statistics-owned canonical Arrow declaration. Nested collections use boxed slices and
/// metadata uses sorted boxed entries, making every owned allocation observable without relying
/// on allocator-private `BTreeMap` node layouts or spare `Vec`/`String` capacity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StatisticsArrowType {
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
        timezone: Option<Box<str>>,
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
        field: Box<StatisticsArrowField>,
        offset_width: u8,
        view: bool,
    },
    FixedSizeList {
        field: Box<StatisticsArrowField>,
        length: i32,
    },
    Struct {
        fields: Box<[StatisticsArrowField]>,
    },
    Map {
        field: Box<StatisticsArrowField>,
        sorted: bool,
    },
    Union {
        fields: Box<[StatisticsArrowUnionField]>,
        mode: CanonicalArrowUnionMode,
    },
    Dictionary {
        key: Box<StatisticsArrowType>,
        value: Box<StatisticsArrowType>,
    },
    RunEndEncoded {
        run_ends: Box<StatisticsArrowField>,
        values: Box<StatisticsArrowField>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StatisticsArrowField {
    pub name: Box<str>,
    pub data_type: StatisticsArrowType,
    pub nullable: bool,
    #[serde(with = "boxed_metadata")]
    pub metadata: StatisticsMetadata,
}

pub type StatisticsMetadata = Box<[(Box<str>, Box<str>)]>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatisticsArrowUnionField {
    pub type_id: i8,
    pub field: StatisticsArrowField,
}

mod boxed_metadata {
    use super::*;
    use serde::ser::SerializeMap;

    pub fn serialize<S>(
        entries: &[(Box<str>, Box<str>)],
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(entries.len()))?;
        for (key, value) in entries.iter() {
            map.serialize_entry(key, value)?;
        }
        map.end()
    }

    pub fn deserialize<'de, D>(deserializer: D) -> std::result::Result<StatisticsMetadata, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MetadataVisitor;
        impl<'de> Visitor<'de> for MetadataVisitor {
            type Value = StatisticsMetadata;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a metadata object with unique sorted keys")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut entries = Vec::with_capacity(map.size_hint().unwrap_or(0));
                while let Some((key, value)) = map.next_entry::<Box<str>, Box<str>>()? {
                    entries.push((key, value));
                }
                entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
                if entries.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                    return Err(serde::de::Error::custom("duplicate metadata key"));
                }
                Ok(entries.into_boxed_slice())
            }
        }
        deserializer.deserialize_map(MetadataVisitor)
    }
}

impl StatisticsArrowField {
    fn from_arrow(field: &Field) -> Result<Self> {
        let mut metadata = field
            .metadata()
            .iter()
            .map(|(key, value)| (key.as_str().into(), value.as_str().into()))
            .collect::<Vec<_>>();
        metadata.sort_unstable_by(|left: &(Box<str>, Box<str>), right| left.0.cmp(&right.0));
        Ok(Self {
            name: field.name().as_str().into(),
            data_type: StatisticsArrowType::from_arrow(field.data_type())?,
            nullable: field.is_nullable(),
            metadata: metadata.into_boxed_slice(),
        })
    }
}

impl StatisticsArrowType {
    /// Converts one Arrow data type into the canonical statistics declaration used by package
    /// evidence. Consumers use this to validate recorded field evidence without importing an
    /// engine-specific type system.
    pub fn from_arrow_data_type(data_type: &DataType) -> Result<Self> {
        Self::from_arrow(data_type)
    }

    /// Validates persisted bounds against the canonical Arrow type and completeness contract.
    /// Package readers and future pruning adapters share this authority so corrupt evidence can
    /// never become permission to skip data.
    pub fn validate_bounds(
        &self,
        row_count: u64,
        null_count: u64,
        completeness: &StatisticsCompleteness,
        minimum: Option<&TypedScalar>,
        maximum: Option<&TypedScalar>,
    ) -> Result<()> {
        if null_count > row_count {
            return Err(CdfError::data("statistics null count exceeds row count"));
        }
        if matches!(completeness, StatisticsCompleteness::Incomplete { .. }) {
            if minimum.is_some() || maximum.is_some() {
                return Err(CdfError::data(
                    "incomplete statistics cannot carry minimum or maximum bounds",
                ));
            }
            return Ok(());
        }

        let non_null_count = row_count - null_count;
        if non_null_count == 0 {
            if minimum.is_some() || maximum.is_some() {
                return Err(CdfError::data(
                    "all-null statistics cannot carry minimum or maximum bounds",
                ));
            }
            return Ok(());
        }
        let minimum = minimum.ok_or_else(|| {
            CdfError::data("complete non-null statistics require a minimum bound")
        })?;
        let maximum = maximum.ok_or_else(|| {
            CdfError::data("complete non-null statistics require a maximum bound")
        })?;
        if !self.accepts_scalar(minimum) || !self.accepts_scalar(maximum) {
            return Err(CdfError::data(
                "statistics bound scalar kind does not match its Arrow type",
            ));
        }
        let ordering = scalar_cmp(minimum, maximum).ok_or_else(|| {
            CdfError::data("statistics minimum and maximum scalar kinds do not match")
        })?;
        if ordering == Ordering::Greater {
            return Err(CdfError::data(
                "statistics minimum bound exceeds its maximum bound",
            ));
        }
        Ok(())
    }

    fn accepts_scalar(&self, scalar: &TypedScalar) -> bool {
        matches!(
            (self, scalar),
            (Self::Boolean, TypedScalar::Boolean(_))
                | (
                    Self::Int { signed: true, .. }
                        | Self::Timestamp { .. }
                        | Self::Date { .. }
                        | Self::Time { .. }
                        | Self::Duration { .. },
                    TypedScalar::Signed(_)
                )
                | (Self::Int { signed: false, .. }, TypedScalar::Unsigned(_))
                | (Self::Float { bits: 32 }, TypedScalar::Float32Bits(_))
                | (Self::Float { bits: 64 }, TypedScalar::Float64Bits(_))
                | (Self::Decimal { bits: 32, .. }, TypedScalar::Decimal32(_))
                | (Self::Decimal { bits: 64, .. }, TypedScalar::Decimal64(_))
                | (Self::Decimal { bits: 128, .. }, TypedScalar::Decimal128(_))
                | (Self::Utf8 { .. } | Self::Utf8View, TypedScalar::Utf8(_))
                | (
                    Self::Binary { .. } | Self::FixedSizeBinary { .. } | Self::BinaryView,
                    TypedScalar::Binary(_)
                )
        )
    }

    fn from_arrow(data_type: &DataType) -> Result<Self> {
        let time_unit = |unit: &TimeUnit| match unit {
            TimeUnit::Second => CanonicalArrowTimeUnit::Second,
            TimeUnit::Millisecond => CanonicalArrowTimeUnit::Millisecond,
            TimeUnit::Microsecond => CanonicalArrowTimeUnit::Microsecond,
            TimeUnit::Nanosecond => CanonicalArrowTimeUnit::Nanosecond,
        };
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
                unit: time_unit(unit),
                timezone: timezone.as_deref().map(Into::into),
            },
            DataType::Date32 => Self::Date {
                unit: CanonicalArrowDateUnit::Day,
            },
            DataType::Date64 => Self::Date {
                unit: CanonicalArrowDateUnit::Millisecond,
            },
            DataType::Time32(unit) => Self::Time {
                unit: time_unit(unit),
                bits: 32,
            },
            DataType::Time64(unit) => Self::Time {
                unit: time_unit(unit),
                bits: 64,
            },
            DataType::Duration(unit) => Self::Duration {
                unit: time_unit(unit),
            },
            DataType::Interval(unit) => Self::Interval {
                unit: match unit {
                    IntervalUnit::YearMonth => CanonicalArrowIntervalUnit::YearMonth,
                    IntervalUnit::DayTime => CanonicalArrowIntervalUnit::DayTime,
                    IntervalUnit::MonthDayNano => CanonicalArrowIntervalUnit::MonthDayNano,
                },
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
                field: Box::new(StatisticsArrowField::from_arrow(field)?),
                offset_width: 32,
                view: false,
            },
            DataType::LargeList(field) => Self::List {
                field: Box::new(StatisticsArrowField::from_arrow(field)?),
                offset_width: 64,
                view: false,
            },
            DataType::ListView(field) => Self::List {
                field: Box::new(StatisticsArrowField::from_arrow(field)?),
                offset_width: 32,
                view: true,
            },
            DataType::LargeListView(field) => Self::List {
                field: Box::new(StatisticsArrowField::from_arrow(field)?),
                offset_width: 64,
                view: true,
            },
            DataType::FixedSizeList(field, length) => Self::FixedSizeList {
                field: Box::new(StatisticsArrowField::from_arrow(field)?),
                length: *length,
            },
            DataType::Struct(fields) => {
                for (index, field) in fields.iter().enumerate() {
                    if fields
                        .iter()
                        .take(index)
                        .any(|prior| prior.name() == field.name())
                    {
                        return Err(CdfError::data(format!(
                            "canonical Arrow struct has duplicate field name {:?}",
                            field.name()
                        )));
                    }
                }
                Self::Struct {
                    fields: fields
                        .iter()
                        .map(|field| StatisticsArrowField::from_arrow(field))
                        .collect::<Result<Vec<_>>>()?
                        .into_boxed_slice(),
                }
            }
            DataType::Map(field, sorted) => Self::Map {
                field: Box::new(StatisticsArrowField::from_arrow(field)?),
                sorted: *sorted,
            },
            DataType::Union(fields, mode) => Self::Union {
                fields: fields
                    .iter()
                    .map(|(type_id, field)| {
                        Ok(StatisticsArrowUnionField {
                            type_id,
                            field: StatisticsArrowField::from_arrow(field)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into_boxed_slice(),
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
                run_ends: Box::new(StatisticsArrowField::from_arrow(run_ends)?),
                values: Box::new(StatisticsArrowField::from_arrow(values)?),
            },
        })
    }
}

impl BatchStats {
    pub fn compute(batch: &RecordBatch) -> Result<Self> {
        let row_count = u64::try_from(batch.num_rows())
            .map_err(|_| CdfError::data("batch row count exceeds u64"))?;
        let columns = batch
            .schema()
            .fields()
            .iter()
            .zip(batch.columns())
            .map(|(field, array)| compute_column(field.name(), field.data_type(), array, row_count))
            .collect::<Result<Vec<_>>>()?
            .into_boxed_slice();
        Ok(Self { columns })
    }

    pub fn merge(&mut self, other: &Self) -> Result<()> {
        if self.columns.is_empty() {
            self.columns = other.columns.clone();
            return Ok(());
        }
        if self.columns.len() != other.columns.len() {
            return Err(CdfError::data(
                "statistics column count changed within a segment",
            ));
        }
        for (left, right) in self.columns.iter_mut().zip(&other.columns) {
            left.merge(right)?;
        }
        Ok(())
    }

    pub fn merge_owned(&mut self, other: Self) -> Result<()> {
        if self.columns.is_empty() {
            self.columns = other.columns;
            return Ok(());
        }
        if self.columns.len() != other.columns.len() {
            return Err(CdfError::data(
                "statistics column count changed within a segment",
            ));
        }
        for (left, right) in self.columns.iter_mut().zip(other.columns.into_vec()) {
            left.merge_owned(right)?;
        }
        Ok(())
    }

    /// Logical retained bytes owned by this statistics value, including its schema/type and
    /// field-path allocations. This is the quantity the engine's package statistics lease owns.
    pub fn retained_bytes(&self) -> Result<u64> {
        let mut bytes = size_bytes::<Self>()?;
        bytes = checked_add(
            bytes,
            checked_mul(
                usize_bytes(self.columns.len())?,
                size_bytes::<ColumnStats>()?,
                "statistics column storage overflow",
            )?,
            "statistics retained bytes overflow",
        )?;
        for column in &self.columns {
            bytes = checked_add(
                bytes,
                column.heap_retained_bytes()?,
                "statistics retained bytes overflow",
            )?;
        }
        Ok(bytes.max(1))
    }

    /// Conservative pre-computation reservation for one batch. The bound includes duplicated
    /// variable-width extrema plus canonical type/path storage, so callers can reserve before
    /// `compute` allocates any retained statistics value.
    pub fn computation_reservation_bytes(batch: &RecordBatch) -> Result<u64> {
        let array_bytes = u64::try_from(batch.get_array_memory_size())
            .map_err(|_| CdfError::data("statistics input memory exceeds u64"))?;
        let mut schema_bytes = size_bytes::<Self>()?;
        for field in batch.schema().fields() {
            schema_bytes = checked_add(
                schema_bytes,
                field_statistics_upper_bound(field.as_ref())?,
                "statistics schema reservation overflow",
            )?;
        }
        checked_add(
            checked_mul(
                array_bytes.max(1),
                2,
                "statistics value reservation overflow",
            )?,
            checked_mul(
                schema_bytes.max(1),
                2,
                "statistics schema reservation overflow",
            )?,
            "statistics computation reservation overflow",
        )
    }
}

impl ColumnStats {
    fn merge(&mut self, other: &Self) -> Result<()> {
        if self.field_path != other.field_path || self.data_type != other.data_type {
            return Err(CdfError::data(
                "statistics field path or Arrow type changed within a segment",
            ));
        }
        self.row_count = self
            .row_count
            .checked_add(other.row_count)
            .ok_or_else(|| CdfError::data("statistics row count overflow"))?;
        self.null_count = self
            .null_count
            .checked_add(other.null_count)
            .ok_or_else(|| CdfError::data("statistics null count overflow"))?;
        if let StatisticsCompleteness::Incomplete { reason: left } = &mut self.completeness {
            if let StatisticsCompleteness::Incomplete { reason: right } = &other.completeness {
                *left = left.clone().max(right.clone());
            }
            return Ok(());
        }
        if let StatisticsCompleteness::Incomplete { reason } = &other.completeness {
            self.completeness = StatisticsCompleteness::Incomplete {
                reason: reason.clone(),
            };
            self.minimum = None;
            self.maximum = None;
            return Ok(());
        }
        self.minimum = combine_bound(self.minimum.take(), other.minimum.clone(), Ordering::Less)?;
        self.maximum = combine_bound(
            self.maximum.take(),
            other.maximum.clone(),
            Ordering::Greater,
        )?;
        Ok(())
    }

    fn merge_owned(&mut self, mut other: Self) -> Result<()> {
        if self.field_path != other.field_path || self.data_type != other.data_type {
            return Err(CdfError::data(
                "statistics field path or Arrow type changed within a segment",
            ));
        }
        self.row_count = self
            .row_count
            .checked_add(other.row_count)
            .ok_or_else(|| CdfError::data("statistics row count overflow"))?;
        self.null_count = self
            .null_count
            .checked_add(other.null_count)
            .ok_or_else(|| CdfError::data("statistics null count overflow"))?;
        if let StatisticsCompleteness::Incomplete { reason: left } = &mut self.completeness {
            if let StatisticsCompleteness::Incomplete { reason: right } = other.completeness {
                *left = left.clone().max(right);
            }
            return Ok(());
        }
        if let StatisticsCompleteness::Incomplete { reason } = other.completeness {
            self.completeness = StatisticsCompleteness::Incomplete { reason };
            self.minimum = None;
            self.maximum = None;
            return Ok(());
        }
        self.minimum =
            combine_bound_owned(self.minimum.take(), other.minimum.take(), Ordering::Less)?;
        self.maximum =
            combine_bound_owned(self.maximum.take(), other.maximum.take(), Ordering::Greater)?;
        Ok(())
    }

    fn heap_retained_bytes(&self) -> Result<u64> {
        let mut bytes = checked_mul(
            usize_bytes(self.field_path.len())?,
            size_bytes::<Box<str>>()?,
            "statistics field-path storage overflow",
        )?;
        for component in &self.field_path {
            bytes = checked_add(
                bytes,
                usize_bytes(component.len())?,
                "statistics field-path storage overflow",
            )?;
        }
        bytes = checked_add(
            bytes,
            canonical_type_heap_bytes(&self.data_type)?,
            "statistics type storage overflow",
        )?;
        for scalar in [self.minimum.as_ref(), self.maximum.as_ref()]
            .into_iter()
            .flatten()
        {
            bytes = checked_add(
                bytes,
                scalar_heap_bytes(scalar)?,
                "statistics scalar storage overflow",
            )?;
        }
        Ok(bytes)
    }
}

fn combine_bound_owned(
    left: Option<TypedScalar>,
    right: Option<TypedScalar>,
    desired: Ordering,
) -> Result<Option<TypedScalar>> {
    match (left, right) {
        (None, value) | (value, None) => Ok(value),
        (Some(left), Some(right)) => {
            let ordering = scalar_cmp(&left, &right).ok_or_else(|| {
                CdfError::data("statistics scalar encoding changed within a typed column")
            })?;
            Ok(Some(if ordering == desired { left } else { right }))
        }
    }
}

fn scalar_heap_bytes(value: &TypedScalar) -> Result<u64> {
    match value {
        TypedScalar::Utf8(value) => usize_bytes(value.len()),
        TypedScalar::Binary(value) => usize_bytes(value.len()),
        _ => Ok(0),
    }
}

fn canonical_type_heap_bytes(value: &StatisticsArrowType) -> Result<u64> {
    use StatisticsArrowType::{
        Dictionary, FixedSizeList, List, Map, RunEndEncoded, Struct, Timestamp, Union,
    };
    match value {
        Timestamp {
            timezone: Some(timezone),
            ..
        } => usize_bytes(timezone.len()),
        List { field, .. } | FixedSizeList { field, .. } | Map { field, .. } => checked_add(
            size_bytes::<StatisticsArrowField>()?,
            canonical_field_heap_bytes(field)?,
            "statistics nested type storage overflow",
        ),
        Struct { fields } => canonical_fields_heap_bytes(fields),
        Union { fields, .. } => {
            let mut bytes = checked_mul(
                usize_bytes(fields.len())?,
                size_bytes::<StatisticsArrowUnionField>()?,
                "statistics union type storage overflow",
            )?;
            for field in fields {
                bytes = checked_add(
                    bytes,
                    canonical_field_heap_bytes(&field.field)?,
                    "statistics union type storage overflow",
                )?;
            }
            Ok(bytes)
        }
        Dictionary { key, value } => {
            let boxes = checked_mul(
                size_bytes::<StatisticsArrowType>()?,
                2,
                "statistics dictionary type storage overflow",
            )?;
            checked_add(
                boxes,
                checked_add(
                    canonical_type_heap_bytes(key)?,
                    canonical_type_heap_bytes(value)?,
                    "statistics dictionary type storage overflow",
                )?,
                "statistics dictionary type storage overflow",
            )
        }
        RunEndEncoded { run_ends, values } => {
            let boxes = checked_mul(
                size_bytes::<StatisticsArrowField>()?,
                2,
                "statistics run-end type storage overflow",
            )?;
            checked_add(
                boxes,
                checked_add(
                    canonical_field_heap_bytes(run_ends)?,
                    canonical_field_heap_bytes(values)?,
                    "statistics run-end type storage overflow",
                )?,
                "statistics run-end type storage overflow",
            )
        }
        _ => Ok(0),
    }
}

fn canonical_fields_heap_bytes(fields: &[StatisticsArrowField]) -> Result<u64> {
    let mut bytes = checked_mul(
        usize_bytes(fields.len())?,
        size_bytes::<StatisticsArrowField>()?,
        "statistics nested field storage overflow",
    )?;
    for field in fields {
        bytes = checked_add(
            bytes,
            canonical_field_heap_bytes(field)?,
            "statistics nested field storage overflow",
        )?;
    }
    Ok(bytes)
}

fn canonical_field_heap_bytes(field: &StatisticsArrowField) -> Result<u64> {
    let mut bytes = usize_bytes(field.name.len())?;
    for (key, value) in &field.metadata {
        bytes = checked_add(
            bytes,
            checked_add(
                checked_add(
                    size_bytes::<(Box<str>, Box<str>)>()?,
                    usize_bytes(key.len())?,
                    "statistics metadata storage overflow",
                )?,
                usize_bytes(value.len())?,
                "statistics metadata storage overflow",
            )?,
            "statistics metadata storage overflow",
        )?;
    }
    checked_add(
        bytes,
        canonical_type_heap_bytes(&field.data_type)?,
        "statistics nested field storage overflow",
    )
}

fn field_statistics_upper_bound(field: &Field) -> Result<u64> {
    let name_bytes = usize_bytes(field.name().len())?;
    let nested = arrow_type_description_bytes(field.data_type())?;
    checked_add(
        size_bytes::<ColumnStats>()?,
        checked_add(
            size_bytes::<Box<str>>()?,
            checked_add(name_bytes, nested, "statistics field reservation overflow")?,
            "statistics field reservation overflow",
        )?,
        "statistics field reservation overflow",
    )
}

fn arrow_field_description_bytes(field: &Field) -> Result<u64> {
    let metadata = checked_mul(
        usize_bytes(field.metadata().len())?,
        size_bytes::<(Box<str>, Box<str>)>()?,
        "statistics metadata entry storage overflow",
    )?;
    let metadata = field
        .metadata()
        .iter()
        .try_fold(metadata, |total, (key, value)| {
            checked_add(
                total,
                checked_add(
                    usize_bytes(key.len())?,
                    usize_bytes(value.len())?,
                    "statistics metadata payload overflow",
                )?,
                "statistics metadata payload overflow",
            )
        })?;
    checked_add(
        size_bytes::<StatisticsArrowField>()?,
        checked_add(
            usize_bytes(field.name().len())?,
            checked_add(
                metadata,
                arrow_type_description_bytes(field.data_type())?,
                "statistics nested field storage overflow",
            )?,
            "statistics nested field storage overflow",
        )?,
        "statistics nested field storage overflow",
    )
}

fn arrow_type_description_bytes(data_type: &DataType) -> Result<u64> {
    match data_type {
        DataType::Timestamp(_, Some(timezone)) => usize_bytes(timezone.len()),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => arrow_field_description_bytes(field),
        DataType::Struct(fields) => fields.iter().try_fold(0_u64, |total, field| {
            checked_add(
                total,
                arrow_field_description_bytes(field)?,
                "statistics nested schema reservation overflow",
            )
        }),
        DataType::Union(fields, _) => fields.iter().try_fold(0_u64, |total, (_, field)| {
            checked_add(
                total,
                checked_add(
                    size_bytes::<StatisticsArrowUnionField>()?,
                    arrow_field_description_bytes(field)?,
                    "statistics union field storage overflow",
                )?,
                "statistics union schema reservation overflow",
            )
        }),
        DataType::Dictionary(key, value) => checked_add(
            checked_mul(
                size_bytes::<StatisticsArrowType>()?,
                2,
                "statistics dictionary schema reservation overflow",
            )?,
            checked_add(
                arrow_type_description_bytes(key)?,
                arrow_type_description_bytes(value)?,
                "statistics dictionary schema reservation overflow",
            )?,
            "statistics dictionary schema reservation overflow",
        ),
        DataType::RunEndEncoded(run_ends, values) => checked_add(
            arrow_field_description_bytes(run_ends)?,
            arrow_field_description_bytes(values)?,
            "statistics run-end schema reservation overflow",
        ),
        _ => Ok(0),
    }
}

fn size_bytes<T>() -> Result<u64> {
    usize_bytes(std::mem::size_of::<T>())
}

fn usize_bytes(value: usize) -> Result<u64> {
    u64::try_from(value).map_err(|_| CdfError::data("statistics retained size exceeds u64"))
}

fn checked_add(left: u64, right: u64, message: &'static str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| CdfError::data(message))
}

fn checked_mul(left: u64, right: u64, message: &'static str) -> Result<u64> {
    left.checked_mul(right)
        .ok_or_else(|| CdfError::data(message))
}

fn combine_bound(
    left: Option<TypedScalar>,
    right: Option<TypedScalar>,
    desired: Ordering,
) -> Result<Option<TypedScalar>> {
    match (left, right) {
        (None, value) | (value, None) => Ok(value),
        (Some(left), Some(right)) => {
            let ordering = scalar_cmp(&left, &right).ok_or_else(|| {
                CdfError::data("statistics scalar encoding changed within a typed column")
            })?;
            Ok(Some(if ordering == desired { left } else { right }))
        }
    }
}

fn scalar_cmp(left: &TypedScalar, right: &TypedScalar) -> Option<Ordering> {
    Some(match (left, right) {
        (TypedScalar::Boolean(a), TypedScalar::Boolean(b)) => a.cmp(b),
        (TypedScalar::Signed(a), TypedScalar::Signed(b)) => a.cmp(b),
        (TypedScalar::Unsigned(a), TypedScalar::Unsigned(b)) => a.cmp(b),
        (TypedScalar::Float32Bits(a), TypedScalar::Float32Bits(b)) => {
            f32::from_bits(*a).total_cmp(&f32::from_bits(*b))
        }
        (TypedScalar::Float64Bits(a), TypedScalar::Float64Bits(b)) => {
            f64::from_bits(*a).total_cmp(&f64::from_bits(*b))
        }
        (TypedScalar::Decimal32(a), TypedScalar::Decimal32(b)) => a.cmp(b),
        (TypedScalar::Decimal64(a), TypedScalar::Decimal64(b)) => a.cmp(b),
        (TypedScalar::Decimal128(a), TypedScalar::Decimal128(b)) => a.cmp(b),
        (TypedScalar::Utf8(a), TypedScalar::Utf8(b)) => a.cmp(b),
        (TypedScalar::Binary(a), TypedScalar::Binary(b)) => a.cmp(b),
        _ => return None,
    })
}

macro_rules! primitive_bounds {
    ($array:expr, $array_ty:ty, $wrap:expr) => {{
        let values = $array.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
            CdfError::internal("Arrow statistics array did not match its declared type")
        })?;
        (min(values).map($wrap), max(values).map($wrap))
    }};
}

fn compute_column(
    name: &str,
    data_type: &DataType,
    array: &arrow_array::ArrayRef,
    row_count: u64,
) -> Result<ColumnStats> {
    let null_count = u64::try_from(array.null_count())
        .map_err(|_| CdfError::data("column null count exceeds u64"))?;
    let canonical_type = StatisticsArrowType::from_arrow(data_type)?;
    let complete = |minimum, maximum| ColumnStats {
        field_path: vec![Box::<str>::from(name)].into_boxed_slice(),
        data_type: canonical_type.clone(),
        row_count,
        null_count,
        minimum,
        maximum,
        completeness: StatisticsCompleteness::Complete,
    };
    let incomplete = |reason| ColumnStats {
        field_path: vec![Box::<str>::from(name)].into_boxed_slice(),
        data_type: canonical_type.clone(),
        row_count,
        null_count,
        minimum: None,
        maximum: None,
        completeness: StatisticsCompleteness::Incomplete { reason },
    };

    let bounds = match data_type {
        DataType::Boolean => {
            let values = downcast::<BooleanArray>(array)?;
            (
                min_boolean(values).map(TypedScalar::Boolean),
                max_boolean(values).map(TypedScalar::Boolean),
            )
        }
        DataType::Int8 => {
            primitive_bounds!(array, Int8Array, |v| TypedScalar::Signed(i64::from(v)))
        }
        DataType::Int16 => {
            primitive_bounds!(array, Int16Array, |v| TypedScalar::Signed(i64::from(v)))
        }
        DataType::Int32 => {
            primitive_bounds!(array, Int32Array, |v| TypedScalar::Signed(i64::from(v)))
        }
        DataType::Int64 => primitive_bounds!(array, Int64Array, TypedScalar::Signed),
        DataType::UInt8 => {
            primitive_bounds!(array, UInt8Array, |v| TypedScalar::Unsigned(u64::from(v)))
        }
        DataType::UInt16 => {
            primitive_bounds!(array, UInt16Array, |v| TypedScalar::Unsigned(u64::from(v)))
        }
        DataType::UInt32 => {
            primitive_bounds!(array, UInt32Array, |v| TypedScalar::Unsigned(u64::from(v)))
        }
        DataType::UInt64 => primitive_bounds!(array, UInt64Array, TypedScalar::Unsigned),
        DataType::Float32 => {
            let values = downcast::<Float32Array>(array)?;
            if values.iter().flatten().any(f32::is_nan) {
                return Ok(incomplete(IncompleteStatisticsReason::NanObserved));
            }
            if values.iter().flatten().any(|value| !value.is_finite()) {
                return Ok(incomplete(IncompleteStatisticsReason::NonFiniteObserved));
            }
            let minimum = values.iter().flatten().min_by(f32::total_cmp);
            let maximum = values.iter().flatten().max_by(f32::total_cmp);
            (
                minimum.map(|value| TypedScalar::Float32Bits(value.to_bits())),
                maximum.map(|value| TypedScalar::Float32Bits(value.to_bits())),
            )
        }
        DataType::Float64 => {
            let values = downcast::<Float64Array>(array)?;
            if values.iter().flatten().any(f64::is_nan) {
                return Ok(incomplete(IncompleteStatisticsReason::NanObserved));
            }
            if values.iter().flatten().any(|value| !value.is_finite()) {
                return Ok(incomplete(IncompleteStatisticsReason::NonFiniteObserved));
            }
            let minimum = values.iter().flatten().min_by(f64::total_cmp);
            let maximum = values.iter().flatten().max_by(f64::total_cmp);
            (
                minimum.map(|value| TypedScalar::Float64Bits(value.to_bits())),
                maximum.map(|value| TypedScalar::Float64Bits(value.to_bits())),
            )
        }
        DataType::Decimal32(_, _) => {
            primitive_bounds!(array, Decimal32Array, TypedScalar::Decimal32)
        }
        DataType::Decimal64(_, _) => {
            primitive_bounds!(array, Decimal64Array, TypedScalar::Decimal64)
        }
        DataType::Decimal128(_, _) => {
            primitive_bounds!(array, Decimal128Array, TypedScalar::Decimal128)
        }
        DataType::Date32 => {
            primitive_bounds!(array, Date32Array, |v| TypedScalar::Signed(i64::from(v)))
        }
        DataType::Date64 => primitive_bounds!(array, Date64Array, TypedScalar::Signed),
        DataType::Time32(arrow_schema::TimeUnit::Second) => {
            primitive_bounds!(array, Time32SecondArray, |v| TypedScalar::Signed(
                i64::from(v)
            ))
        }
        DataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
            primitive_bounds!(array, Time32MillisecondArray, |v| TypedScalar::Signed(
                i64::from(v)
            ))
        }
        DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
            primitive_bounds!(array, Time64MicrosecondArray, TypedScalar::Signed)
        }
        DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
            primitive_bounds!(array, Time64NanosecondArray, TypedScalar::Signed)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
            primitive_bounds!(array, TimestampSecondArray, TypedScalar::Signed)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
            primitive_bounds!(array, TimestampMillisecondArray, TypedScalar::Signed)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            primitive_bounds!(array, TimestampMicrosecondArray, TypedScalar::Signed)
        }
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
            primitive_bounds!(array, TimestampNanosecondArray, TypedScalar::Signed)
        }
        DataType::Duration(arrow_schema::TimeUnit::Second) => {
            primitive_bounds!(array, DurationSecondArray, TypedScalar::Signed)
        }
        DataType::Duration(arrow_schema::TimeUnit::Millisecond) => {
            primitive_bounds!(array, DurationMillisecondArray, TypedScalar::Signed)
        }
        DataType::Duration(arrow_schema::TimeUnit::Microsecond) => {
            primitive_bounds!(array, DurationMicrosecondArray, TypedScalar::Signed)
        }
        DataType::Duration(arrow_schema::TimeUnit::Nanosecond) => {
            primitive_bounds!(array, DurationNanosecondArray, TypedScalar::Signed)
        }
        DataType::Utf8 => {
            let values = downcast::<StringArray>(array)?;
            (
                min_string(values).map(|v| TypedScalar::Utf8(v.into())),
                max_string(values).map(|v| TypedScalar::Utf8(v.into())),
            )
        }
        DataType::LargeUtf8 => {
            let values = downcast::<LargeStringArray>(array)?;
            (
                min_string(values).map(|v| TypedScalar::Utf8(v.into())),
                max_string(values).map(|v| TypedScalar::Utf8(v.into())),
            )
        }
        DataType::Utf8View => {
            let values = downcast::<StringViewArray>(array)?;
            (
                min_string_view(values).map(|v| TypedScalar::Utf8(v.into())),
                max_string_view(values).map(|v| TypedScalar::Utf8(v.into())),
            )
        }
        DataType::Binary => {
            let values = downcast::<BinaryArray>(array)?;
            (
                min_binary(values).map(|v| TypedScalar::Binary(v.into())),
                max_binary(values).map(|v| TypedScalar::Binary(v.into())),
            )
        }
        DataType::LargeBinary => {
            let values = downcast::<LargeBinaryArray>(array)?;
            (
                min_binary(values).map(|v| TypedScalar::Binary(v.into())),
                max_binary(values).map(|v| TypedScalar::Binary(v.into())),
            )
        }
        DataType::BinaryView => {
            let values = downcast::<BinaryViewArray>(array)?;
            (
                min_binary_view(values).map(|v| TypedScalar::Binary(v.into())),
                max_binary_view(values).map(|v| TypedScalar::Binary(v.into())),
            )
        }
        DataType::FixedSizeBinary(_) => {
            let values = downcast::<FixedSizeBinaryArray>(array)?;
            (
                min_fixed_size_binary(values).map(|v| TypedScalar::Binary(v.into())),
                max_fixed_size_binary(values).map(|v| TypedScalar::Binary(v.into())),
            )
        }
        _ => return Ok(incomplete(IncompleteStatisticsReason::UnsupportedType)),
    };
    Ok(complete(bounds.0, bounds.1))
}

fn downcast<T: 'static>(array: &arrow_array::ArrayRef) -> Result<&T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| CdfError::internal("Arrow statistics array did not match its declared type"))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{
        ArrayRef, Decimal128Array, Float64Array, Int64Array, ListArray, RecordBatch, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::*;

    #[test]
    fn typed_statistics_preserve_numeric_decimal_and_temporal_semantics() {
        let decimal = Decimal128Array::from(vec![Some(-101_i128), None, Some(99)])
            .with_precision_and_scale(20, 4)
            .unwrap();
        let timestamp = TimestampMicrosecondArray::from(vec![Some(-5_i64), Some(9), None])
            .with_timezone("America/Phoenix");
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("integer", DataType::Int64, true),
                Field::new("decimal", DataType::Decimal128(20, 4), true),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, Some("America/Phoenix".into())),
                    true,
                ),
                Field::new("text", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![Some(10), Some(-2), None])) as ArrayRef,
                Arc::new(decimal),
                Arc::new(timestamp),
                Arc::new(StringArray::from(vec![Some("10"), Some("2"), None])),
            ],
        )
        .unwrap();

        let stats = BatchStats::compute(&batch).unwrap();
        assert_eq!(stats.columns[0].minimum, Some(TypedScalar::Signed(-2)));
        assert_eq!(stats.columns[0].maximum, Some(TypedScalar::Signed(10)));
        assert_eq!(
            stats.columns[1].minimum,
            Some(TypedScalar::Decimal128(-101))
        );
        assert_eq!(stats.columns[2].maximum, Some(TypedScalar::Signed(9)));
        assert_eq!(
            stats.columns[3].minimum,
            Some(TypedScalar::Utf8("10".into()))
        );
        assert_eq!(stats.columns[0].row_count, 3);
        assert_eq!(stats.columns[0].null_count, 1);

        let encoded = serde_json::to_vec(&stats).unwrap();
        assert_eq!(
            serde_json::from_slice::<BatchStats>(&encoded).unwrap(),
            stats
        );
    }

    #[test]
    fn merge_is_deterministic_and_completeness_is_monotone() {
        let batch = |values: Vec<Option<i64>>| {
            RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(Int64Array::from(values)) as ArrayRef,
            )])
            .unwrap()
        };
        let a = BatchStats::compute(&batch(vec![Some(8), None])).unwrap();
        let b = BatchStats::compute(&batch(vec![Some(-3), Some(12)])).unwrap();
        let mut left = a.clone();
        left.merge(&b).unwrap();
        let mut right = b;
        right.merge(&a).unwrap();
        assert_eq!(left, right);
        assert_eq!(left.columns[0].minimum, Some(TypedScalar::Signed(-3)));
        assert_eq!(left.columns[0].maximum, Some(TypedScalar::Signed(12)));
        assert_eq!(left.columns[0].row_count, 4);

        let floats = RecordBatch::try_from_iter(vec![(
            "value",
            Arc::new(Float64Array::from(vec![1.0, f64::NAN])) as ArrayRef,
        )])
        .unwrap();
        let incomplete = BatchStats::compute(&floats).unwrap();
        assert!(matches!(
            incomplete.columns[0].completeness,
            StatisticsCompleteness::Incomplete {
                reason: IncompleteStatisticsReason::NanObserved
            }
        ));
    }

    #[test]
    fn nested_values_are_explicitly_incomplete() {
        let list = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            None,
        ]);
        let batch =
            RecordBatch::try_from_iter(vec![("nested", Arc::new(list) as ArrayRef)]).unwrap();
        let stats = BatchStats::compute(&batch).unwrap();
        assert!(matches!(
            stats.columns[0].completeness,
            StatisticsCompleteness::Incomplete {
                reason: IncompleteStatisticsReason::UnsupportedType
            }
        ));
    }

    #[test]
    fn persisted_bounds_reject_type_completeness_and_order_corruption() {
        let signed = StatisticsArrowType::Int {
            signed: true,
            bits: 64,
        };
        signed
            .validate_bounds(
                3,
                1,
                &StatisticsCompleteness::Complete,
                Some(&TypedScalar::Signed(-2)),
                Some(&TypedScalar::Signed(10)),
            )
            .unwrap();

        for (minimum, maximum, expected) in [
            (
                Some(TypedScalar::Unsigned(1)),
                Some(TypedScalar::Unsigned(2)),
                "scalar kind",
            ),
            (
                Some(TypedScalar::Signed(10)),
                Some(TypedScalar::Signed(-2)),
                "exceeds",
            ),
        ] {
            let error = signed
                .validate_bounds(
                    3,
                    1,
                    &StatisticsCompleteness::Complete,
                    minimum.as_ref(),
                    maximum.as_ref(),
                )
                .unwrap_err();
            assert!(error.message.contains(expected), "{error}");
        }

        let error = signed
            .validate_bounds(
                3,
                1,
                &StatisticsCompleteness::Incomplete {
                    reason: IncompleteStatisticsReason::UnsupportedType,
                },
                Some(&TypedScalar::Signed(-2)),
                None,
            )
            .unwrap_err();
        assert!(error.message.contains("incomplete statistics"), "{error}");

        let error = signed
            .validate_bounds(
                3,
                3,
                &StatisticsCompleteness::Complete,
                Some(&TypedScalar::Signed(-2)),
                Some(&TypedScalar::Signed(10)),
            )
            .unwrap_err();
        assert!(error.message.contains("all-null statistics"), "{error}");
    }

    #[test]
    fn nested_metadata_and_dictionary_storage_are_canonical_and_accounted() {
        let metadata = HashMap::from([
            ("zeta".to_owned(), "last".to_owned()),
            ("alpha".to_owned(), "first".to_owned()),
        ]);
        let field = Field::new("nested", DataType::Utf8, true).with_metadata(metadata);
        let canonical = StatisticsArrowField::from_arrow(&field).unwrap();
        assert_eq!(canonical.metadata[0].0.as_ref(), "alpha");
        assert_eq!(canonical.metadata[1].0.as_ref(), "zeta");
        assert_eq!(
            serde_json::from_slice::<StatisticsArrowField>(
                &serde_json::to_vec(&canonical).unwrap()
            )
            .unwrap(),
            canonical
        );

        let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let expected_boxes = 2 * std::mem::size_of::<StatisticsArrowType>() as u64;
        assert_eq!(
            arrow_type_description_bytes(&dictionary).unwrap(),
            expected_boxes
        );
        assert_eq!(
            canonical_type_heap_bytes(&StatisticsArrowType::from_arrow(&dictionary).unwrap())
                .unwrap(),
            expected_boxes
        );
    }
}
