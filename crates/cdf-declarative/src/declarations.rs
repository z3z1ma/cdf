use std::collections::BTreeMap;

use cdf_kernel::{CdfError, Result};
use schemars::{JsonSchema, schema_for};
use serde::{Deserialize, Serialize};

pub const DECLARATIVE_SCHEMA_VERSION: &str = "cdf-declarative-v1";
pub const DECLARATIVE_SCHEMA_ARTIFACT_PATH: &str = "schemas/cdf-declarative.schema.json";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct DeclarativeDocument {
    #[serde(default)]
    pub source: BTreeMap<String, SourceDeclaration>,
    #[serde(default)]
    pub resource: BTreeMap<String, ResourceDeclaration>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourceDeclaration {
    Rest(RestSourceDeclaration),
    Sql(SqlSourceDeclaration),
    Files(FileSourceDeclaration),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RestSourceDeclaration {
    pub base_url: String,
    pub auth: Option<AuthDeclaration>,
    pub rate_limit: Option<RateLimitDeclaration>,
    #[serde(default)]
    pub egress_allowlist: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SqlSourceDeclaration {
    pub connection: String,
    pub dialect: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FileSourceDeclaration {
    pub root: String,
    pub auth: Option<AuthDeclaration>,
    #[serde(default)]
    pub egress_allowlist: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AuthDeclaration {
    Bearer { token: String },
    Header { name: String, value: String },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct RateLimitDeclaration {
    pub requests_per_minute: Option<u32>,
    #[serde(default)]
    pub respect_headers: Vec<String>,
    #[serde(default)]
    pub quota_headers: Vec<QuotaHeaderDeclaration>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct QuotaHeaderDeclaration {
    pub remaining_header: String,
    pub reset_header: String,
    pub reset: ResetSemanticsDeclaration,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResetSemanticsDeclaration {
    DelaySeconds,
    EpochSeconds,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ResourceDeclaration {
    pub id: Option<String>,
    pub source: Option<String>,
    pub path: Option<String>,
    pub query: Option<String>,
    pub table: Option<String>,
    pub glob: Option<String>,
    pub format: Option<FileFormatDeclaration>,
    pub compression: Option<FileCompressionDeclaration>,
    #[serde(default)]
    pub params: BTreeMap<String, ParamValueDeclaration>,
    pub paginate: Option<PaginationDeclaration>,
    pub records: Option<String>,
    pub records_transform: Option<String>,
    #[serde(default)]
    pub primary_key: Vec<String>,
    pub merge_key: Option<Vec<String>>,
    pub cursor: Option<CursorDeclaration>,
    pub write_disposition: Option<WriteDispositionDeclaration>,
    pub contract: Option<String>,
    pub trust: Option<TrustDeclaration>,
    pub partition: Option<PartitionDeclaration>,
    pub freshness: Option<FreshnessDeclaration>,
    pub schema: Option<SchemaDeclaration>,
    pub sample: Option<SampleDeclaration>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PaginationDeclaration {
    LinkHeader,
    CursorParam {
        query_param: String,
        response_field: String,
        initial: Option<String>,
    },
    PageNumber {
        query_param: String,
        start_page: Option<u64>,
    },
    Offset {
        offset_param: String,
        limit_param: String,
        start_offset: Option<u64>,
        limit: u64,
    },
    NextToken {
        query_param: String,
        response_field: String,
        initial: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum ParamValueDeclaration {
    String(String),
    Integer(i64),
    Unsigned(u64),
    Boolean(bool),
}

impl ParamValueDeclaration {
    pub(crate) fn as_query_value(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Integer(value) => value.to_string(),
            Self::Unsigned(value) => value.to_string(),
            Self::Boolean(value) => value.to_string(),
        }
    }
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileFormatDeclaration {
    Csv,
    Json,
    Ndjson,
    Parquet,
    ArrowIpc,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FileCompressionDeclaration {
    Auto,
    None,
    Gzip,
    Zstd,
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

pub fn declarative_json_schema() -> serde_json::Value {
    serde_json::to_value(schema_for!(DeclarativeDocument))
        .expect("declarative schema generation must serialize")
}

pub fn declarative_json_schema_artifact() -> JsonSchemaArtifact {
    JsonSchemaArtifact {
        version: DECLARATIVE_SCHEMA_VERSION,
        path: DECLARATIVE_SCHEMA_ARTIFACT_PATH,
        schema: declarative_json_schema(),
    }
}
