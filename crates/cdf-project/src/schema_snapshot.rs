use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use arrow_schema::{
    DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use cdf_contract::FieldCoercionDecision;
use cdf_kernel::{
    CanonicalArrowType, CdfError, DiscoveryManifestReference, ResourceId, Result, SchemaHash,
    SchemaSnapshotReference, discovery_manifest_from_metadata, insert_discovery_manifest_metadata,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SCHEMA_SNAPSHOT_ARTIFACT_VERSION: u16 = 4;
pub const SCHEMA_SNAPSHOT_PROMOTION_AUTHORITY_VERSION: u16 = 1;
pub const SCHEMA_SNAPSHOT_DIR: &str = ".cdf/schemas";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshotArtifact {
    pub version: u16,
    pub resource_id: String,
    pub schema_hash: SchemaHash,
    pub path: String,
    pub schema: SchemaSnapshotSchema,
    pub metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion_authority: Option<SchemaSnapshotPromotionAuthority>,
    pub hash_input: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshotHashInput {
    pub version: u16,
    pub resource_id: String,
    pub schema: SchemaSnapshotSchema,
    pub metadata: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery_manifest: Option<DiscoveryManifestReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub promotion_authority: Option<SchemaSnapshotPromotionAuthority>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaSnapshotPromotionAuthority {
    pub version: u16,
    pub resource_id: String,
    pub old_snapshot: SchemaSnapshotReference,
    pub proposed_schema: SchemaSnapshotSchema,
    pub fresh_discovery_schema_hash: Option<String>,
    pub fresh_discovery_manifest_hash: Option<String>,
    pub fresh_discovery_file_coverage: Option<crate::DiscoveryFileCoverage>,
    pub fresh_discovery_content_identity: BTreeMap<String, String>,
    pub normalizer_version: String,
    pub contract_policy_hash: String,
    pub validation_program_hash: Option<String>,
    pub selected_paths: Vec<SchemaSnapshotPromotionPathAuthority>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaSnapshotPromotionPathAuthority {
    pub path: String,
    pub source_name: String,
    pub output_field: String,
    pub selected_arrow_type: CanonicalArrowType,
    pub coercion_verdicts: Vec<SchemaSnapshotPromotionCoercionAuthority>,
    pub observed_count: u64,
    pub address_value_digest: String,
    pub packages: Vec<String>,
    pub associations: Vec<SchemaSnapshotPromotionTargetAssociationAuthority>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaSnapshotPromotionCoercionAuthority {
    pub observed_type: CanonicalArrowType,
    pub selected_type: CanonicalArrowType,
    pub decision: FieldCoercionDecision,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaSnapshotPromotionTargetAssociationAuthority {
    pub package_hash: String,
    pub destination: String,
    pub target: String,
    pub recorded_receipt_ids: Vec<String>,
    pub availability: SchemaSnapshotPromotionEvidenceAvailability,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaSnapshotPromotionEvidenceAvailability {
    RetainedPackage,
    DestinationReadback,
    TombstoneOnly,
    Missing,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshotSchema {
    pub fields: Vec<SchemaSnapshotField>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshotField {
    pub name: String,
    pub data_type: SchemaSnapshotDataType,
    pub nullable: bool,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshotUnionField {
    pub type_id: i8,
    pub field: SchemaSnapshotField,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchemaSnapshotDataType {
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
        unit: SchemaSnapshotTimeUnit,
        timezone: Option<String>,
    },
    Date {
        unit: SchemaSnapshotDateUnit,
    },
    Time {
        unit: SchemaSnapshotTimeUnit,
        bits: u8,
    },
    Duration {
        unit: SchemaSnapshotTimeUnit,
    },
    Interval {
        unit: SchemaSnapshotIntervalUnit,
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
        field: Box<SchemaSnapshotField>,
        offset_width: u8,
        view: bool,
    },
    FixedSizeList {
        field: Box<SchemaSnapshotField>,
        length: i32,
    },
    Struct {
        fields: Vec<SchemaSnapshotField>,
    },
    Union {
        mode: SchemaSnapshotUnionMode,
        fields: Vec<SchemaSnapshotUnionField>,
    },
    Dictionary {
        key_type: Box<SchemaSnapshotDataType>,
        value_type: Box<SchemaSnapshotDataType>,
    },
    Map {
        field: Box<SchemaSnapshotField>,
        sorted: bool,
    },
    RunEndEncoded {
        run_ends: Box<SchemaSnapshotField>,
        values: Box<SchemaSnapshotField>,
    },
    Other {
        display: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaSnapshotTimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaSnapshotDateUnit {
    Day,
    Millisecond,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaSnapshotIntervalUnit {
    YearMonth,
    DayTime,
    MonthDayNano,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaSnapshotUnionMode {
    Sparse,
    Dense,
}

impl SchemaSnapshotSchema {
    pub fn from_arrow(schema: &Schema) -> Self {
        Self {
            fields: schema
                .fields()
                .iter()
                .map(|field| SchemaSnapshotField::from_arrow(field.as_ref()))
                .collect(),
            metadata: metadata_map(&schema.metadata),
        }
    }

    pub fn to_arrow(&self) -> Result<Schema> {
        Ok(Schema::new_with_metadata(
            self.fields
                .iter()
                .map(SchemaSnapshotField::to_arrow)
                .collect::<Result<Vec<_>>>()?,
            self.metadata.clone().into_iter().collect(),
        ))
    }
}

impl SchemaSnapshotField {
    fn from_arrow(field: &Field) -> Self {
        Self {
            name: field.name().clone(),
            data_type: SchemaSnapshotDataType::from_arrow(field.data_type()),
            nullable: field.is_nullable(),
            metadata: metadata_map(field.metadata()),
        }
    }

    fn to_arrow(&self) -> Result<Field> {
        Ok(
            Field::new(&self.name, self.data_type.to_arrow()?, self.nullable)
                .with_metadata(self.metadata.clone().into_iter().collect()),
        )
    }
}

impl SchemaSnapshotDataType {
    fn from_arrow(data_type: &DataType) -> Self {
        Self::primitive_from_arrow(data_type)
            .or_else(|| Self::decimal_from_arrow(data_type))
            .or_else(|| Self::temporal_from_arrow(data_type))
            .or_else(|| Self::binary_from_arrow(data_type))
            .or_else(|| Self::text_from_arrow(data_type))
            .unwrap_or_else(|| Self::nested_from_arrow(data_type))
    }

    fn to_arrow(&self) -> Result<DataType> {
        match self {
            Self::Null => Ok(DataType::Null),
            Self::Boolean => Ok(DataType::Boolean),
            Self::Int { signed, bits } => integer_data_type(*signed, *bits),
            Self::Float { bits } => match bits {
                16 => Ok(DataType::Float16),
                32 => Ok(DataType::Float32),
                64 => Ok(DataType::Float64),
                _ => Err(snapshot_type_error(format!("float{bits}"))),
            },
            Self::Decimal {
                bits,
                precision,
                scale,
            } => match bits {
                32 => Ok(DataType::Decimal32(*precision, *scale)),
                64 => Ok(DataType::Decimal64(*precision, *scale)),
                128 => Ok(DataType::Decimal128(*precision, *scale)),
                256 => Ok(DataType::Decimal256(*precision, *scale)),
                _ => Err(snapshot_type_error(format!("decimal{bits}"))),
            },
            Self::Timestamp { unit, timezone } => Ok(DataType::Timestamp(
                unit.to_arrow(),
                timezone.as_deref().map(Into::into),
            )),
            Self::Date { unit } => match unit {
                SchemaSnapshotDateUnit::Day => Ok(DataType::Date32),
                SchemaSnapshotDateUnit::Millisecond => Ok(DataType::Date64),
            },
            Self::Time { unit, bits } => match (bits, unit) {
                (32, SchemaSnapshotTimeUnit::Second | SchemaSnapshotTimeUnit::Millisecond) => {
                    Ok(DataType::Time32(unit.to_arrow()))
                }
                (64, SchemaSnapshotTimeUnit::Microsecond | SchemaSnapshotTimeUnit::Nanosecond) => {
                    Ok(DataType::Time64(unit.to_arrow()))
                }
                _ => Err(snapshot_type_error(format!("time{bits}({unit:?})"))),
            },
            Self::Duration { unit } => Ok(DataType::Duration(unit.to_arrow())),
            Self::Interval { unit } => Ok(DataType::Interval(unit.to_arrow())),
            Self::Binary { offset_width: 32 } => Ok(DataType::Binary),
            Self::Binary { offset_width: 64 } => Ok(DataType::LargeBinary),
            Self::Binary { offset_width } => Err(snapshot_type_error(format!(
                "binary(offset={offset_width})"
            ))),
            Self::FixedSizeBinary { byte_width } => Ok(DataType::FixedSizeBinary(*byte_width)),
            Self::BinaryView => Ok(DataType::BinaryView),
            Self::Utf8 { offset_width: 32 } => Ok(DataType::Utf8),
            Self::Utf8 { offset_width: 64 } => Ok(DataType::LargeUtf8),
            Self::Utf8 { offset_width } => {
                Err(snapshot_type_error(format!("utf8(offset={offset_width})")))
            }
            Self::Utf8View => Ok(DataType::Utf8View),
            Self::List {
                field,
                offset_width: 32,
                view: false,
            } => Ok(DataType::List(field.to_arrow()?.into())),
            Self::List {
                field,
                offset_width: 64,
                view: false,
            } => Ok(DataType::LargeList(field.to_arrow()?.into())),
            Self::List {
                field,
                offset_width: 32,
                view: true,
            } => Ok(DataType::ListView(field.to_arrow()?.into())),
            Self::List {
                field,
                offset_width: 64,
                view: true,
            } => Ok(DataType::LargeListView(field.to_arrow()?.into())),
            Self::List {
                offset_width, view, ..
            } => Err(snapshot_type_error(format!(
                "list(offset={offset_width}, view={view})"
            ))),
            Self::FixedSizeList { field, length } => {
                Ok(DataType::FixedSizeList(field.to_arrow()?.into(), *length))
            }
            Self::Struct { fields } => Ok(DataType::Struct(Fields::from(
                fields
                    .iter()
                    .map(SchemaSnapshotField::to_arrow)
                    .collect::<Result<Vec<_>>>()?,
            ))),
            Self::Union { mode, fields } => {
                let union_fields = UnionFields::try_new(
                    fields.iter().map(|field| field.type_id),
                    fields
                        .iter()
                        .map(|field| field.field.to_arrow())
                        .collect::<Result<Vec<_>>>()?,
                )
                .map_err(|error| {
                    CdfError::data(format!("invalid schema snapshot union: {error}"))
                })?;
                Ok(DataType::Union(union_fields, mode.to_arrow()))
            }
            Self::Dictionary {
                key_type,
                value_type,
            } => Ok(DataType::Dictionary(
                Box::new(key_type.to_arrow()?),
                Box::new(value_type.to_arrow()?),
            )),
            Self::Map { field, sorted } => Ok(DataType::Map(field.to_arrow()?.into(), *sorted)),
            Self::RunEndEncoded { run_ends, values } => Ok(DataType::RunEndEncoded(
                run_ends.to_arrow()?.into(),
                values.to_arrow()?.into(),
            )),
            Self::Other { display } => Err(CdfError::data(format!(
                "schema snapshot type `{display}` cannot be reconstructed as an Arrow data type"
            ))),
        }
    }

    fn primitive_from_arrow(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Null => Some(Self::Null),
            DataType::Boolean => Some(Self::Boolean),
            DataType::Int8 => Some(Self::Int {
                signed: true,
                bits: 8,
            }),
            DataType::Int16 => Some(Self::Int {
                signed: true,
                bits: 16,
            }),
            DataType::Int32 => Some(Self::Int {
                signed: true,
                bits: 32,
            }),
            DataType::Int64 => Some(Self::Int {
                signed: true,
                bits: 64,
            }),
            DataType::UInt8 => Some(Self::Int {
                signed: false,
                bits: 8,
            }),
            DataType::UInt16 => Some(Self::Int {
                signed: false,
                bits: 16,
            }),
            DataType::UInt32 => Some(Self::Int {
                signed: false,
                bits: 32,
            }),
            DataType::UInt64 => Some(Self::Int {
                signed: false,
                bits: 64,
            }),
            DataType::Float16 => Some(Self::Float { bits: 16 }),
            DataType::Float32 => Some(Self::Float { bits: 32 }),
            DataType::Float64 => Some(Self::Float { bits: 64 }),
            _ => None,
        }
    }

    fn decimal_from_arrow(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Decimal32(precision, scale) => Some(Self::Decimal {
                bits: 32,
                precision: *precision,
                scale: *scale,
            }),
            DataType::Decimal64(precision, scale) => Some(Self::Decimal {
                bits: 64,
                precision: *precision,
                scale: *scale,
            }),
            DataType::Decimal128(precision, scale) => Some(Self::Decimal {
                bits: 128,
                precision: *precision,
                scale: *scale,
            }),
            DataType::Decimal256(precision, scale) => Some(Self::Decimal {
                bits: 256,
                precision: *precision,
                scale: *scale,
            }),
            _ => None,
        }
    }

    fn temporal_from_arrow(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Timestamp(unit, timezone) => Some(Self::Timestamp {
                unit: SchemaSnapshotTimeUnit::from_arrow(unit),
                timezone: timezone.as_ref().map(ToString::to_string),
            }),
            DataType::Date32 => Some(Self::Date {
                unit: SchemaSnapshotDateUnit::Day,
            }),
            DataType::Date64 => Some(Self::Date {
                unit: SchemaSnapshotDateUnit::Millisecond,
            }),
            DataType::Time32(unit) => Some(Self::Time {
                unit: SchemaSnapshotTimeUnit::from_arrow(unit),
                bits: 32,
            }),
            DataType::Time64(unit) => Some(Self::Time {
                unit: SchemaSnapshotTimeUnit::from_arrow(unit),
                bits: 64,
            }),
            DataType::Duration(unit) => Some(Self::Duration {
                unit: SchemaSnapshotTimeUnit::from_arrow(unit),
            }),
            DataType::Interval(unit) => Some(Self::Interval {
                unit: SchemaSnapshotIntervalUnit::from_arrow(unit),
            }),
            _ => None,
        }
    }

    fn binary_from_arrow(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Binary => Some(Self::Binary { offset_width: 32 }),
            DataType::LargeBinary => Some(Self::Binary { offset_width: 64 }),
            DataType::FixedSizeBinary(byte_width) => Some(Self::FixedSizeBinary {
                byte_width: *byte_width,
            }),
            DataType::BinaryView => Some(Self::BinaryView),
            _ => None,
        }
    }

    fn text_from_arrow(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Utf8 => Some(Self::Utf8 { offset_width: 32 }),
            DataType::LargeUtf8 => Some(Self::Utf8 { offset_width: 64 }),
            DataType::Utf8View => Some(Self::Utf8View),
            _ => None,
        }
    }

    fn nested_from_arrow(data_type: &DataType) -> Self {
        match data_type {
            DataType::List(field) => Self::List {
                field: Box::new(SchemaSnapshotField::from_arrow(field.as_ref())),
                offset_width: 32,
                view: false,
            },
            DataType::ListView(field) => Self::List {
                field: Box::new(SchemaSnapshotField::from_arrow(field.as_ref())),
                offset_width: 32,
                view: true,
            },
            DataType::LargeList(field) => Self::List {
                field: Box::new(SchemaSnapshotField::from_arrow(field.as_ref())),
                offset_width: 64,
                view: false,
            },
            DataType::LargeListView(field) => Self::List {
                field: Box::new(SchemaSnapshotField::from_arrow(field.as_ref())),
                offset_width: 64,
                view: true,
            },
            DataType::FixedSizeList(field, length) => Self::FixedSizeList {
                field: Box::new(SchemaSnapshotField::from_arrow(field.as_ref())),
                length: *length,
            },
            DataType::Struct(fields) => Self::Struct {
                fields: fields
                    .iter()
                    .map(|field| SchemaSnapshotField::from_arrow(field.as_ref()))
                    .collect(),
            },
            DataType::Union(fields, mode) => Self::Union {
                mode: SchemaSnapshotUnionMode::from_arrow(mode),
                fields: fields
                    .iter()
                    .map(|(type_id, field)| SchemaSnapshotUnionField {
                        type_id,
                        field: SchemaSnapshotField::from_arrow(field.as_ref()),
                    })
                    .collect(),
            },
            DataType::Dictionary(key_type, value_type) => Self::Dictionary {
                key_type: Box::new(Self::from_arrow(key_type)),
                value_type: Box::new(Self::from_arrow(value_type)),
            },
            DataType::Map(field, sorted) => Self::Map {
                field: Box::new(SchemaSnapshotField::from_arrow(field.as_ref())),
                sorted: *sorted,
            },
            DataType::RunEndEncoded(run_ends, values) => Self::RunEndEncoded {
                run_ends: Box::new(SchemaSnapshotField::from_arrow(run_ends.as_ref())),
                values: Box::new(SchemaSnapshotField::from_arrow(values.as_ref())),
            },
            _ => Self::Other {
                display: data_type.to_string(),
            },
        }
    }
}

impl SchemaSnapshotTimeUnit {
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

impl SchemaSnapshotIntervalUnit {
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

impl SchemaSnapshotUnionMode {
    fn from_arrow(mode: &UnionMode) -> Self {
        match mode {
            UnionMode::Sparse => Self::Sparse,
            UnionMode::Dense => Self::Dense,
        }
    }

    fn to_arrow(&self) -> UnionMode {
        match self {
            Self::Sparse => UnionMode::Sparse,
            Self::Dense => UnionMode::Dense,
        }
    }
}

fn integer_data_type(signed: bool, bits: u8) -> Result<DataType> {
    match (signed, bits) {
        (true, 8) => Ok(DataType::Int8),
        (true, 16) => Ok(DataType::Int16),
        (true, 32) => Ok(DataType::Int32),
        (true, 64) => Ok(DataType::Int64),
        (false, 8) => Ok(DataType::UInt8),
        (false, 16) => Ok(DataType::UInt16),
        (false, 32) => Ok(DataType::UInt32),
        (false, 64) => Ok(DataType::UInt64),
        _ => Err(snapshot_type_error(format!(
            "{}int{bits}",
            if signed { "" } else { "u" }
        ))),
    }
}

fn snapshot_type_error(data_type: String) -> CdfError {
    CdfError::data(format!(
        "schema snapshot contains unsupported Arrow type encoding `{data_type}`"
    ))
}

fn metadata_map(metadata: &std::collections::HashMap<String, String>) -> BTreeMap<String, String> {
    metadata
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

impl SchemaSnapshotPromotionAuthority {
    fn validate_for_artifact(
        &self,
        resource_id: &ResourceId,
        proposed_schema: &SchemaSnapshotSchema,
    ) -> Result<()> {
        if self.version != SCHEMA_SNAPSHOT_PROMOTION_AUTHORITY_VERSION {
            return Err(CdfError::data(format!(
                "schema snapshot promotion authority uses unsupported version {}; expected {}",
                self.version, SCHEMA_SNAPSHOT_PROMOTION_AUTHORITY_VERSION
            )));
        }
        if self.resource_id != resource_id.as_str() {
            return Err(CdfError::data(format!(
                "schema snapshot promotion authority belongs to resource {:?}, not {:?}",
                self.resource_id,
                resource_id.as_str()
            )));
        }
        if &self.proposed_schema != proposed_schema {
            return Err(CdfError::data(
                "schema snapshot promotion authority proposed_schema does not match the artifact schema",
            ));
        }
        let expected_old_path =
            schema_snapshot_relative_path(resource_id, &self.old_snapshot.schema_hash)?;
        if self.old_snapshot.path != expected_old_path {
            return Err(CdfError::data(format!(
                "schema snapshot promotion authority old snapshot path {} does not match {}",
                self.old_snapshot.path, expected_old_path
            )));
        }
        if self.normalizer_version.trim().is_empty()
            || self.contract_policy_hash.trim().is_empty()
            || self.selected_paths.is_empty()
        {
            return Err(CdfError::data(
                "schema snapshot promotion authority requires a normalizer version, contract policy hash, and at least one selected path",
            ));
        }
        if self.fresh_discovery_manifest_hash.is_some()
            && (self.fresh_discovery_schema_hash.is_none()
                || self.fresh_discovery_file_coverage.is_none())
        {
            return Err(CdfError::data(
                "schema snapshot promotion authority manifest hash requires fresh schema hash and coverage",
            ));
        }

        let mut previous_path = None::<&str>;
        let mut output_fields = BTreeMap::new();
        for field in &proposed_schema.fields {
            if output_fields.insert(field.name.as_str(), field).is_some() {
                return Err(CdfError::data(format!(
                    "schema snapshot promotion artifact schema contains duplicate field {:?}",
                    field.name
                )));
            }
        }
        let mut seen_outputs = BTreeMap::new();
        for selected in &self.selected_paths {
            if previous_path.is_some_and(|previous| previous >= selected.path.as_str()) {
                return Err(CdfError::data(
                    "schema snapshot promotion authority selected paths must be unique and sorted",
                ));
            }
            previous_path = Some(&selected.path);
            if selected.source_name.trim().is_empty()
                || selected.output_field.trim().is_empty()
                || selected.observed_count == 0
                || selected.coercion_verdicts.is_empty()
                || selected.packages.is_empty()
            {
                return Err(CdfError::data(format!(
                    "schema snapshot promotion authority path {:?} has incomplete typed evidence",
                    selected.path
                )));
            }
            let expected_path =
                cdf_contract::residual_json_pointer([selected.source_name.as_str()]);
            if selected.path != expected_path {
                return Err(CdfError::data(format!(
                    "schema snapshot promotion authority path {:?} is not the top-level source identifier {:?}",
                    selected.path, selected.source_name
                )));
            }
            if seen_outputs
                .insert(selected.output_field.as_str(), selected.path.as_str())
                .is_some()
            {
                return Err(CdfError::data(format!(
                    "schema snapshot promotion authority reuses output field {:?}",
                    selected.output_field
                )));
            }
            let Some(field) = output_fields.get(selected.output_field.as_str()) else {
                return Err(CdfError::data(format!(
                    "schema snapshot promotion authority output field {:?} is absent from the proposed schema",
                    selected.output_field
                )));
            };
            if field.metadata.get("cdf:source_name") != Some(&selected.source_name)
                || field.metadata.get("cdf:promoted_path") != Some(&selected.path)
                || field.data_type
                    != SchemaSnapshotDataType::from_arrow(&selected.selected_arrow_type.to_arrow()?)
            {
                return Err(CdfError::data(format!(
                    "schema snapshot promotion authority path {:?} does not match proposed field {:?} type and provenance metadata",
                    selected.path, selected.output_field
                )));
            }
            if !is_sorted_unique(&selected.packages) {
                return Err(CdfError::data(format!(
                    "schema snapshot promotion authority path {:?} packages must be unique and sorted",
                    selected.path
                )));
            }
            for verdict in &selected.coercion_verdicts {
                if verdict.selected_type != selected.selected_arrow_type {
                    return Err(CdfError::data(format!(
                        "schema snapshot promotion authority path {:?} has a coercion verdict for a different selected type",
                        selected.path
                    )));
                }
            }
            let mut previous_association = None::<(&str, &str, &str)>;
            for association in &selected.associations {
                let key = (
                    association.package_hash.as_str(),
                    association.destination.as_str(),
                    association.target.as_str(),
                );
                if previous_association.is_some_and(|previous| previous >= key)
                    || !selected.packages.contains(&association.package_hash)
                    || association.destination.trim().is_empty()
                    || association.target.trim().is_empty()
                    || association.recorded_receipt_ids.is_empty()
                    || !is_sorted_unique(&association.recorded_receipt_ids)
                {
                    return Err(CdfError::data(format!(
                        "schema snapshot promotion authority path {:?} has an invalid package/target association",
                        selected.path
                    )));
                }
                previous_association = Some(key);
            }
        }
        Ok(())
    }
}

fn is_sorted_unique(values: &[String]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

impl SchemaSnapshotArtifact {
    pub fn new(
        resource_id: &ResourceId,
        schema: &Schema,
        metadata: BTreeMap<String, String>,
    ) -> Result<Self> {
        let schema = SchemaSnapshotSchema::from_arrow(schema);
        let hash_input = SchemaSnapshotHashInput {
            version: SCHEMA_SNAPSHOT_ARTIFACT_VERSION,
            resource_id: resource_id.as_str().to_owned(),
            schema: schema.clone(),
            metadata: metadata.clone(),
            discovery_manifest: None,
            promotion_authority: None,
        };
        let hash_input = canonical_json_value(&hash_input)?;
        let schema_hash = schema_hash_for_canonical_value(&hash_input)?;
        let path = schema_snapshot_relative_path(resource_id, &schema_hash)?;
        Ok(Self {
            version: SCHEMA_SNAPSHOT_ARTIFACT_VERSION,
            resource_id: resource_id.as_str().to_owned(),
            schema_hash,
            path,
            schema,
            metadata,
            promotion_authority: None,
            hash_input,
        })
    }

    pub fn new_with_discovery_manifest(
        resource_id: &ResourceId,
        schema: &Schema,
        mut metadata: BTreeMap<String, String>,
        discovery_manifest: DiscoveryManifestReference,
    ) -> Result<Self> {
        insert_discovery_manifest_metadata(&mut metadata, &discovery_manifest)?;
        let schema = SchemaSnapshotSchema::from_arrow(schema);
        let hash_input = SchemaSnapshotHashInput {
            version: SCHEMA_SNAPSHOT_ARTIFACT_VERSION,
            resource_id: resource_id.as_str().to_owned(),
            schema: schema.clone(),
            metadata: metadata.clone(),
            discovery_manifest: Some(discovery_manifest.clone()),
            promotion_authority: None,
        };
        let hash_input = canonical_json_value(&hash_input)?;
        let schema_hash = schema_hash_for_canonical_value(&hash_input)?;
        let path = schema_snapshot_relative_path(resource_id, &schema_hash)?;
        Ok(Self {
            version: SCHEMA_SNAPSHOT_ARTIFACT_VERSION,
            resource_id: resource_id.as_str().to_owned(),
            schema_hash,
            path,
            schema,
            metadata,
            promotion_authority: None,
            hash_input,
        })
    }

    pub fn new_with_promotion(
        resource_id: &ResourceId,
        schema: &Schema,
        promotion_authority: SchemaSnapshotPromotionAuthority,
    ) -> Result<Self> {
        let schema = SchemaSnapshotSchema::from_arrow(schema);
        promotion_authority.validate_for_artifact(resource_id, &schema)?;
        let metadata = promotion_snapshot_metadata(&promotion_authority);
        let hash_input = SchemaSnapshotHashInput {
            version: SCHEMA_SNAPSHOT_ARTIFACT_VERSION,
            resource_id: resource_id.as_str().to_owned(),
            schema: schema.clone(),
            metadata: metadata.clone(),
            discovery_manifest: None,
            promotion_authority: Some(promotion_authority.clone()),
        };
        let hash_input = canonical_json_value(&hash_input)?;
        let schema_hash = schema_hash_for_canonical_value(&hash_input)?;
        let path = schema_snapshot_relative_path(resource_id, &schema_hash)?;
        Ok(Self {
            version: SCHEMA_SNAPSHOT_ARTIFACT_VERSION,
            resource_id: resource_id.as_str().to_owned(),
            schema_hash,
            path,
            schema,
            metadata,
            promotion_authority: Some(promotion_authority),
            hash_input,
        })
    }

    pub fn reference(&self) -> SchemaSnapshotReference {
        SchemaSnapshotReference {
            schema_hash: self.schema_hash.clone(),
            path: self.path.clone(),
            metadata: self.metadata.clone(),
        }
    }

    pub fn discovery_manifest_reference(&self) -> Result<Option<DiscoveryManifestReference>> {
        discovery_manifest_from_metadata(&self.metadata)
    }

    pub fn normalizer_version(&self) -> Option<&str> {
        self.promotion_authority
            .as_ref()
            .map(|authority| authority.normalizer_version.as_str())
            .or_else(|| self.metadata.get("cdf:normalizer").map(String::as_str))
    }

    pub fn validate_hash_input(&self) -> Result<()> {
        if self.version != SCHEMA_SNAPSHOT_ARTIFACT_VERSION {
            return Err(CdfError::data(format!(
                "schema snapshot uses unsupported artifact version {}; expected {}",
                self.version, SCHEMA_SNAPSHOT_ARTIFACT_VERSION
            )));
        }
        let discovery_manifest = self.discovery_manifest_reference()?;
        if discovery_manifest.is_some() && self.promotion_authority.is_some() {
            return Err(CdfError::data(
                "schema snapshot cannot combine discovery-manifest and promotion authority",
            ));
        }
        if let Some(promotion_authority) = &self.promotion_authority {
            let resource_id = ResourceId::new(self.resource_id.clone())?;
            promotion_authority.validate_for_artifact(&resource_id, &self.schema)?;
            if self.metadata != promotion_snapshot_metadata(promotion_authority) {
                return Err(CdfError::data(
                    "promoted schema snapshot metadata must contain only the normalizer derived from typed promotion authority",
                ));
            }
        }
        let expected_input = canonical_json_value(&SchemaSnapshotHashInput {
            version: self.version,
            resource_id: self.resource_id.clone(),
            schema: self.schema.clone(),
            metadata: self.metadata.clone(),
            discovery_manifest,
            promotion_authority: self.promotion_authority.clone(),
        })?;
        if self.hash_input != expected_input {
            return Err(CdfError::data(
                "schema snapshot hash_input does not match artifact schema and metadata",
            ));
        }
        let expected_hash = schema_hash_for_canonical_value(&expected_input)?;
        if self.schema_hash != expected_hash {
            return Err(CdfError::data(format!(
                "schema snapshot hash {} does not match deterministic hash {}",
                self.schema_hash, expected_hash
            )));
        }
        let resource_id = ResourceId::new(self.resource_id.clone())?;
        let expected_path = schema_snapshot_relative_path(&resource_id, &self.schema_hash)?;
        if self.path != expected_path {
            return Err(CdfError::data(format!(
                "schema snapshot path {} does not match deterministic path {}",
                self.path, expected_path
            )));
        }
        Ok(())
    }
}

fn promotion_snapshot_metadata(
    authority: &SchemaSnapshotPromotionAuthority,
) -> BTreeMap<String, String> {
    BTreeMap::from([(
        "cdf:normalizer".to_owned(),
        authority.normalizer_version.clone(),
    )])
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaSnapshotStore {
    project_root: PathBuf,
}

impl SchemaSnapshotStore {
    pub fn new(project_root: impl AsRef<Path>) -> Self {
        Self {
            project_root: project_root.as_ref().to_path_buf(),
        }
    }

    pub fn artifact_path(&self, reference: &SchemaSnapshotReference) -> Result<PathBuf> {
        validate_snapshot_reference_path(&reference.path)?;
        Ok(self.project_root.join(&reference.path))
    }

    pub fn write(&self, artifact: &SchemaSnapshotArtifact) -> Result<PathBuf> {
        artifact.validate_hash_input()?;
        self.validate_discovery_manifest(artifact)?;
        let path = self.project_root.join(&artifact.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| CdfError::data(format!("create {}: {error}", parent.display())))?;
        }
        fs::write(&path, canonical_json_bytes(artifact)?)
            .map_err(|error| CdfError::data(format!("write {}: {error}", path.display())))?;
        Ok(path)
    }

    pub fn write_if_changed(&self, artifact: &SchemaSnapshotArtifact) -> Result<bool> {
        artifact.validate_hash_input()?;
        self.validate_discovery_manifest(artifact)?;
        let path = self.project_root.join(&artifact.path);
        let encoded = canonical_json_bytes(artifact)?;
        if fs::read(&path).ok().as_deref() == Some(encoded.as_slice()) {
            return Ok(false);
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| CdfError::data(format!("create {}: {error}", parent.display())))?;
        }
        fs::write(&path, encoded)
            .map_err(|error| CdfError::data(format!("write {}: {error}", path.display())))?;
        Ok(true)
    }

    pub fn read(&self, reference: &SchemaSnapshotReference) -> Result<SchemaSnapshotArtifact> {
        let path = self.artifact_path(reference)?;
        let bytes = fs::read(&path)
            .map_err(|error| CdfError::data(format!("read {}: {error}", path.display())))?;
        let artifact = serde_json::from_slice::<SchemaSnapshotArtifact>(&bytes)
            .map_err(|error| CdfError::data(format!("parse {}: {error}", path.display())))?;
        if artifact.version != SCHEMA_SNAPSHOT_ARTIFACT_VERSION {
            return Err(CdfError::data(format!(
                "schema snapshot {} uses unsupported artifact version {}; expected {}",
                path.display(),
                artifact.version,
                SCHEMA_SNAPSHOT_ARTIFACT_VERSION
            )));
        }
        artifact.validate_hash_input()?;
        if artifact.schema_hash != reference.schema_hash {
            return Err(CdfError::data(format!(
                "schema snapshot {} contains hash {} but lock/reference expected {}",
                path.display(),
                artifact.schema_hash,
                reference.schema_hash
            )));
        }
        if artifact.reference() != *reference {
            return Err(CdfError::data(format!(
                "schema snapshot {} does not match its locked path and metadata reference",
                path.display()
            )));
        }
        if let Some(manifest) = reference.discovery_manifest()? {
            let manifest_artifact =
                crate::DiscoveryManifestStore::new(&self.project_root).read(&manifest)?;
            if manifest_artifact.resource_id != artifact.resource_id {
                return Err(CdfError::data(format!(
                    "discovery manifest {} belongs to resource {} but schema snapshot belongs to {}",
                    manifest.path, manifest_artifact.resource_id, artifact.resource_id
                )));
            }
        }
        Ok(artifact)
    }

    /// Hydrates a snapshot, validates its content-addressed reference and linked
    /// discovery manifest, and returns the authority token accepted by schema
    /// discovery refresh options.
    pub fn read_with_verified_baseline(
        &self,
        reference: &SchemaSnapshotReference,
    ) -> Result<(SchemaSnapshotArtifact, crate::VerifiedSchemaBaseline)> {
        let artifact = self.read(reference)?;
        let baseline_observation_schema_hashes = match reference.discovery_manifest()? {
            Some(manifest) => crate::DiscoveryManifestStore::new(&self.project_root)
                .read(&manifest)?
                .candidates
                .into_iter()
                .filter_map(|candidate| candidate.physical_schema_hash)
                .collect(),
            None => std::collections::BTreeSet::new(),
        };
        let baseline = crate::VerifiedSchemaBaseline::from_hydrated_snapshot(
            ResourceId::new(artifact.resource_id.clone())?,
            reference.clone(),
            Arc::new(artifact.schema.to_arrow()?),
            baseline_observation_schema_hashes,
        );
        Ok((artifact, baseline))
    }

    fn validate_discovery_manifest(&self, artifact: &SchemaSnapshotArtifact) -> Result<()> {
        let Some(reference) = artifact.discovery_manifest_reference()? else {
            return Ok(());
        };
        let manifest = crate::DiscoveryManifestStore::new(&self.project_root).read(&reference)?;
        if manifest.resource_id != artifact.resource_id {
            return Err(CdfError::data(format!(
                "discovery manifest {} belongs to resource {} but schema snapshot belongs to {}",
                reference.path, manifest.resource_id, artifact.resource_id
            )));
        }
        Ok(())
    }
}

pub fn schema_snapshot_relative_path(
    resource_id: &ResourceId,
    schema_hash: &SchemaHash,
) -> Result<String> {
    ensure_single_path_component(resource_id.as_str(), "resource id")?;
    ensure_single_path_component(schema_hash.as_str(), "schema hash")?;
    Ok(format!(
        "{SCHEMA_SNAPSHOT_DIR}/{}@{}.json",
        resource_id, schema_hash
    ))
}

fn schema_hash_for_canonical_value(value: &serde_json::Value) -> Result<SchemaHash> {
    let bytes = serde_json::to_vec(value).map_err(|error| CdfError::internal(error.to_string()))?;
    SchemaHash::new(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

fn canonical_json_bytes(value: &impl Serialize) -> Result<Vec<u8>> {
    let value = canonical_json_value(value)?;
    serde_json::to_vec_pretty(&value).map_err(|error| CdfError::internal(error.to_string()))
}

fn canonical_json_value(value: &impl Serialize) -> Result<serde_json::Value> {
    let mut value =
        serde_json::to_value(value).map_err(|error| CdfError::internal(error.to_string()))?;
    value.sort_all_objects();
    Ok(value)
}

fn ensure_single_path_component(value: &str, label: &str) -> Result<()> {
    if value.contains(['/', '\\']) {
        return Err(CdfError::contract(format!(
            "schema snapshot {label} `{value}` must be one path component"
        )));
    }
    let mut components = Path::new(value).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(CdfError::contract(format!(
            "schema snapshot {label} `{value}` must be one path component"
        ))),
    }
}

fn validate_snapshot_reference_path(path: &str) -> Result<()> {
    let mut components = Path::new(path).components();
    match (
        components.next(),
        components.next(),
        components.next(),
        components.next(),
    ) {
        (
            Some(Component::Normal(root)),
            Some(Component::Normal(dir)),
            Some(Component::Normal(_file)),
            None,
        ) if root == ".cdf" && dir == "schemas" => Ok(()),
        _ => Err(CdfError::contract(format!(
            "schema snapshot reference path `{path}` must match {SCHEMA_SNAPSHOT_DIR}/<resource>@<hash>.json"
        ))),
    }
}
