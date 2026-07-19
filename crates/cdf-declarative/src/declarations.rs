use std::collections::BTreeMap;

use cdf_kernel::{CdfError, Result};
use cdf_runtime::SourceRegistry;
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

pub const DECLARATIVE_SCHEMA_VERSION: &str = "cdf-declarative-v4";
pub const DECLARATIVE_SCHEMA_ARTIFACT_PATH: &str = "schemas/cdf-declarative.schema.json";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DeclarativeDocument {
    #[serde(default)]
    pub source: BTreeMap<String, SourceDeclaration>,
    #[serde(default)]
    pub resource: BTreeMap<String, ResourceDeclaration>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SourceDeclaration {
    pub kind: String,
    #[serde(default, flatten)]
    pub options: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResourceDeclaration {
    pub source: Option<String>,
    #[schemars(range(min = 1))]
    pub sample_files: Option<u64>,
    #[serde(default)]
    pub primary_key: Vec<String>,
    pub merge_key: Option<Vec<String>>,
    pub cursor: Option<CursorDeclaration>,
    pub write_disposition: Option<WriteDispositionDeclaration>,
    pub deduplicate: Option<DeduplicationDeclaration>,
    pub contract: Option<String>,
    pub trust: Option<TrustDeclaration>,
    pub partition: Option<PartitionDeclaration>,
    pub freshness: Option<FreshnessDeclaration>,
    pub schema: Option<SchemaDeclaration>,
    pub schema_mode: Option<SchemaModeDeclaration>,
    pub sample: Option<SampleDeclaration>,
    pub types: Option<TypePolicyDeclaration>,
    pub execution: Option<ExecutionDeclaration>,
    #[serde(default, flatten)]
    pub options: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExecutionDeclaration {
    Bounded,
    Drain {
        checkpoint_cadence: EpochClosureDeclaration,
        package_rotation: EpochClosureDeclaration,
        termination: DrainTerminationDeclaration,
        watermark: WatermarkDeclaration,
        late_data: LateDataDeclaration,
        safe_frontier: SafeFrontierDeclaration,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EpochClosureDeclaration {
    Batches { count: u64 },
    Rows { count: u64 },
    Bytes { count: u64 },
    Elapsed { milliseconds: u64 },
    WatermarkAdvance { units: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DrainTerminationDeclaration {
    Quiescent,
    Duration { milliseconds: u64 },
    Records { count: u64 },
    Bytes { count: u64 },
    SourceFrontier { position: SourcePositionDeclaration },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourcePositionDeclaration {
    Cursor {
        field: String,
        value: CursorValueDeclaration,
    },
    Log {
        log: String,
        offset: i64,
        sequence: Option<String>,
    },
    FileManifest {
        files: Vec<FilePositionDeclaration>,
    },
    PageToken {
        token: String,
    },
    Composite {
        positions: BTreeMap<String, SourcePositionDeclaration>,
    },
    ForeignState {
        protocol: String,
        opaque_blob: Vec<u8>,
        blob_sha256: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CursorValueDeclaration {
    String(String),
    I64(i64),
    U64(u64),
    DecimalString(String),
    TimestampMicros {
        micros: i64,
        timezone: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FilePositionDeclaration {
    pub path: String,
    pub size_bytes: u64,
    pub source_generation: Option<String>,
    pub etag: Option<String>,
    pub object_version: Option<String>,
    pub sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum WatermarkDeclaration {
    Disabled,
    Enabled {
        event_time_field: String,
        domain: EventTimeDomainDeclaration,
        authority: WatermarkAuthorityDeclaration,
        partition_aggregation: PartitionWatermarkAggregationDeclaration,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventTimeDomainDeclaration {
    SignedInteger,
    UnsignedInteger,
    Decimal {
        precision: u8,
        scale: i8,
    },
    Date32,
    Date64,
    Timestamp {
        unit: TimeUnitDeclaration,
        timezone: Option<String>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TimeUnitDeclaration {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WatermarkAuthorityDeclaration {
    Source,
    Derived { mapping_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PartitionWatermarkAggregationDeclaration {
    MinimumAll,
    MinimumEligible {
        idle_after_milliseconds: u64,
        capability_id: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LateDataDeclaration {
    RecaptureNextEpoch,
    Quarantine,
    AdmitWithAnnotation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SafeFrontierDeclaration {
    CanonicalAdmittedSourcePosition,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeduplicationDeclaration {
    ExactRow,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SchemaModeDeclaration {
    Declared,
    Hints,
    Discover,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TypePolicyDeclaration {
    #[serde(default)]
    pub coerce_types: bool,
    #[serde(default)]
    pub allow_lossy_mapping: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CursorDeclaration {
    pub field: String,
    pub param: Option<String>,
    pub ordering: CursorOrderingDeclaration,
    pub lag: String,
    pub filter_fidelity: Option<FilterFidelityDeclaration>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CursorOrderingDeclaration {
    Exact,
    Inexact,
    BestEffort,
    Unordered,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FilterFidelityDeclaration {
    Exact,
    Inexact,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WriteDispositionDeclaration {
    Append,
    Replace,
    Merge,
    CdcApply,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TrustDeclaration {
    Experimental,
    Governed,
    Financial,
    Serving,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PartitionDeclaration {
    pub by: PartitionByDeclaration,
    pub width: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PartitionByDeclaration {
    Resource,
    CursorWindow,
    File,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FreshnessDeclaration {
    pub max_age: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SchemaDeclaration {
    #[serde(default)]
    pub fields: Vec<FieldDeclaration>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FieldDeclaration {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: FieldTypeDeclaration,
    pub nullable: Option<bool>,
    pub timezone: Option<String>,
    pub semantic: Option<String>,
    pub source_name: Option<String>,
    pub null_origin: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct FieldTypeDeclaration(String);

impl FieldTypeDeclaration {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SampleDeclaration {
    #[serde(default)]
    pub fields: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct JsonSchemaArtifact {
    pub version: &'static str,
    pub path: &'static str,
    pub schema: serde_json::Value,
}

pub fn parse_toml(input: &str) -> Result<DeclarativeDocument> {
    toml::from_str(input).map_err(|error| CdfError::contract(error.to_string()))
}

pub fn parse_yaml(input: &str) -> Result<DeclarativeDocument> {
    serde_yaml::from_str(input).map_err(|error| CdfError::contract(error.to_string()))
}

pub fn declarative_json_schema(registry: &SourceRegistry) -> Result<serde_json::Value> {
    let mut schema = serde_json::to_value(schema_for!(DeclarativeDocument))
        .expect("declarative schema generation must serialize");
    merge_source_driver_schemas(&mut schema, registry)?;
    Ok(schema)
}

pub fn declarative_json_schema_artifact(registry: &SourceRegistry) -> Result<JsonSchemaArtifact> {
    Ok(JsonSchemaArtifact {
        version: DECLARATIVE_SCHEMA_VERSION,
        path: DECLARATIVE_SCHEMA_ARTIFACT_PATH,
        schema: declarative_json_schema(registry)?,
    })
}

fn merge_source_driver_schemas(
    schema: &mut serde_json::Value,
    registry: &SourceRegistry,
) -> Result<()> {
    let definitions = schema
        .get_mut("$defs")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| CdfError::internal("declarative JSON Schema has no $defs object"))?;
    let common_source = definitions
        .get("SourceDeclaration")
        .cloned()
        .ok_or_else(|| CdfError::internal("declarative JSON Schema has no source definition"))?;
    let common_resource = definitions
        .get("ResourceDeclaration")
        .cloned()
        .ok_or_else(|| CdfError::internal("declarative JSON Schema has no resource definition"))?;

    let option_schemas = registry.option_schemas();
    let descriptors = registry.descriptors();
    let mut source_variants = Vec::new();
    let mut resource_variants = Vec::new();
    for descriptor in descriptors {
        let driver_schema = option_schemas
            .get(descriptor.driver_id.as_str())
            .ok_or_else(|| CdfError::internal("registered source has no option schema"))?;
        let source_options = driver_schema.get("source").cloned().ok_or_else(|| {
            CdfError::internal("registered source option schema has no source section")
        })?;
        let resource_options = driver_schema.get("resource").cloned().ok_or_else(|| {
            CdfError::internal("registered source option schema has no resource section")
        })?;
        for kind in descriptor.kinds {
            let mut variant = merge_closed_object_schemas(&common_source, &source_options)?;
            let object = variant.as_object_mut().expect("merged schema is an object");
            object
                .get_mut("properties")
                .and_then(serde_json::Value::as_object_mut)
                .expect("merged object schema has properties")
                .insert("kind".to_owned(), serde_json::json!({"const": kind}));
            require_property(object, "kind");
            source_variants.push(variant);
        }
        resource_variants.push(merge_closed_object_schemas(
            &common_resource,
            &resource_options,
        )?);
    }
    definitions.insert(
        "SourceDeclaration".to_owned(),
        serde_json::json!({"oneOf": source_variants}),
    );
    definitions.insert(
        "ResourceDeclaration".to_owned(),
        serde_json::json!({"anyOf": resource_variants}),
    );
    Ok(())
}

fn merge_closed_object_schemas(
    common: &serde_json::Value,
    driver: &serde_json::Value,
) -> Result<serde_json::Value> {
    let common = common
        .as_object()
        .ok_or_else(|| CdfError::internal("common declaration schema is not an object"))?;
    let driver = driver
        .as_object()
        .ok_or_else(|| CdfError::internal("source driver option schema is not an object"))?;
    if common.get("type").and_then(serde_json::Value::as_str) != Some("object")
        || driver.get("type").and_then(serde_json::Value::as_str) != Some("object")
    {
        return Err(CdfError::internal(
            "declarative and source-driver option schemas must describe objects",
        ));
    }

    let mut merged = common.clone();
    merged.insert("additionalProperties".to_owned(), serde_json::json!(false));
    let properties = merged
        .entry("properties".to_owned())
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
        .ok_or_else(|| CdfError::internal("common object schema properties are not an object"))?;
    for (name, property) in driver
        .get("properties")
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flatten()
    {
        if properties.insert(name.clone(), property.clone()).is_some() {
            return Err(CdfError::internal(format!(
                "source driver option `{name}` conflicts with a common declarative field"
            )));
        }
    }
    for required in driver
        .get("required")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        let name = required.as_str().ok_or_else(|| {
            CdfError::internal("source driver required option name is not a string")
        })?;
        require_property(&mut merged, name);
    }
    Ok(serde_json::Value::Object(merged))
}

fn require_property(schema: &mut serde_json::Map<String, serde_json::Value>, name: &str) {
    let required = schema
        .entry("required".to_owned())
        .or_insert_with(|| serde_json::json!([]))
        .as_array_mut()
        .expect("object schema required list must be an array");
    if !required.iter().any(|entry| entry.as_str() == Some(name)) {
        required.push(serde_json::json!(name));
        required.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    }
}
