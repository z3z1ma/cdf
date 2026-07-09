use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
};

use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit, UnionMode};
use cdf_kernel::{CdfError, ResourceId, Result, SchemaHash, SchemaSnapshotReference};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const SCHEMA_SNAPSHOT_ARTIFACT_VERSION: u16 = 1;
pub const SCHEMA_SNAPSHOT_DIR: &str = ".cdf/schemas";
pub const SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER: &str = "parquet-footer";
pub const SCHEMA_DISCOVERY_FORMAT_PARQUET: &str = "parquet";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SchemaSnapshotArtifact {
    pub version: u16,
    pub resource_id: String,
    pub schema_hash: SchemaHash,
    pub path: String,
    pub schema: SchemaSnapshotSchema,
    pub metadata: BTreeMap<String, String>,
    pub hash_input: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DiscoveredParquetSchemaSnapshot {
    pub artifact: SchemaSnapshotArtifact,
    pub reference: SchemaSnapshotReference,
    pub source_identity: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaSnapshotHashInput {
    pub version: u16,
    pub resource_id: String,
    pub schema: SchemaSnapshotSchema,
    pub metadata: BTreeMap<String, String>,
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
}

impl SchemaSnapshotIntervalUnit {
    fn from_arrow(unit: &IntervalUnit) -> Self {
        match unit {
            IntervalUnit::YearMonth => Self::YearMonth,
            IntervalUnit::DayTime => Self::DayTime,
            IntervalUnit::MonthDayNano => Self::MonthDayNano,
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
}

fn metadata_map(metadata: &std::collections::HashMap<String, String>) -> BTreeMap<String, String> {
    metadata
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
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

    pub fn validate_hash_input(&self) -> Result<()> {
        let expected_input = SchemaSnapshotHashInput {
            version: self.version,
            resource_id: self.resource_id.clone(),
            schema: self.schema.clone(),
            metadata: self.metadata.clone(),
        };
        let expected_input = canonical_json_value(&expected_input)?;
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

pub fn schema_snapshot_from_parquet_footer_schema(
    resource_id: &ResourceId,
    schema: &Schema,
    source_identity: BTreeMap<String, String>,
) -> Result<DiscoveredParquetSchemaSnapshot> {
    let metadata = BTreeMap::from([
        (
            "probe".to_owned(),
            SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER.to_owned(),
        ),
        (
            "format".to_owned(),
            SCHEMA_DISCOVERY_FORMAT_PARQUET.to_owned(),
        ),
    ]);
    let artifact = SchemaSnapshotArtifact::new(resource_id, schema, metadata)?;
    Ok(DiscoveredParquetSchemaSnapshot {
        reference: artifact.reference(),
        artifact,
        source_identity,
    })
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
        let path = self.project_root.join(&artifact.path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| CdfError::data(format!("create {}: {error}", parent.display())))?;
        }
        fs::write(&path, canonical_json_bytes(artifact)?)
            .map_err(|error| CdfError::data(format!("write {}: {error}", path.display())))?;
        Ok(path)
    }

    pub fn read(&self, reference: &SchemaSnapshotReference) -> Result<SchemaSnapshotArtifact> {
        let path = self.artifact_path(reference)?;
        let bytes = fs::read(&path)
            .map_err(|error| CdfError::data(format!("read {}: {error}", path.display())))?;
        let artifact = serde_json::from_slice::<SchemaSnapshotArtifact>(&bytes)
            .map_err(|error| CdfError::data(format!("parse {}: {error}", path.display())))?;
        artifact.validate_hash_input()?;
        if artifact.schema_hash != reference.schema_hash {
            return Err(CdfError::data(format!(
                "schema snapshot {} contains hash {} but lock/reference expected {}",
                path.display(),
                artifact.schema_hash,
                reference.schema_hash
            )));
        }
        Ok(artifact)
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
