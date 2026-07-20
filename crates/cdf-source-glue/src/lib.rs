#![doc = "AWS Glue conventional external-table source adapter for cdf."]

mod client;
mod config;
mod driver;
mod execution;
mod lake_formation;
mod model;
mod planner;
mod planning_index;
mod schema;
mod task;
mod task_reader;

pub use client::{
    AwsGlueCatalogClient, GlueCatalogClient, GlueGetPartitionsRequest, GlueGetTableRequest,
    GlueGetUnfilteredPartitionsRequest, GlueGetUnfilteredTableRequest,
    GlueLakeFormationAuthorization, GluePartitionPage, GlueTableResponse,
};
pub use config::{GlueResourceOptions, GlueSourceOptions};
pub use driver::{GlueRuntimeDependencies, GlueSourceDriver};
pub use lake_formation::{
    AwsLakeFormationClient, LakeFormationClient, LakeFormationCredentialRequest,
    LakeFormationCredentialResponse,
};
pub use model::{
    GlueColumn, GlueFormatMapping, GluePartition, GlueSerdeInfo, GlueStorageDescriptor, GlueTable,
    GlueTableClass, classify_table, merge_descriptor,
};
pub use schema::{glue_arrow_schema, parse_glue_type};
pub use task::{
    GLUE_TASK_AUTHORITY_VERSION, GLUE_TASK_SET_TYPE, GLUE_TASK_VERSION, GlueObjectTask,
    GlueTaskAuthority,
};

pub const GLUE_SOURCE_DRIVER_VERSION: &str = "1.0.0";

pub fn glue_option_schema() -> serde_json::Value {
    let secret = serde_json::json!({"type": "string", "pattern": "^secret://"});
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "source": {
            "type": "object",
            "additionalProperties": false,
            "required": ["region"],
            "properties": {
                "region": {"type": "string", "minLength": 1},
                "object_region": {"type": "string", "minLength": 1},
                "catalog_id": {"type": "string", "minLength": 1},
                "endpoint": {"type": "string", "format": "uri"},
                "lake_formation_endpoint": {"type": "string", "format": "uri"},
                "credentials": secret,
                "object_credentials": secret,
                "lake_formation_session_duration_seconds": {"type": "integer", "minimum": 900, "maximum": 43200},
                "lake_formation_refresh_margin_seconds": {"type": "integer", "minimum": 1, "maximum": 899, "default": 60},
                "lake_formation_binding_cache_entries": {"type": "integer", "minimum": 1, "default": 1024},
                "egress_allowlist": {"type": "array", "items": {"type": "string", "minLength": 1}, "uniqueItems": true},
                "maximum_response_bytes": {"type": "integer", "minimum": 1, "default": 16777216},
                "maximum_partitions": {"type": "integer", "minimum": 1, "default": 1000000},
                "maximum_objects": {"type": "integer", "minimum": 1, "default": 10000000},
                "maximum_task_bytes": {"type": "integer", "minimum": 1, "default": 262144},
                "maximum_task_authority_bytes": {"type": "integer", "minimum": 1, "default": 16777216},
                "task_writer_buffer_bytes": {"type": "integer", "minimum": 1, "default": 1048576},
                "batch_rows": {"type": "integer", "minimum": 1, "default": 65536},
                "maximum_batch_bytes": {"type": "integer", "minimum": 8192, "default": 33554432},
                "maximum_concurrency": {"type": "integer", "minimum": 1, "maximum": 65535, "default": 65535},
                "stream_buffer_batches": {"type": "integer", "minimum": 1, "maximum": 65535, "default": 2},
                "planning_spill_growth_bytes": {"type": "integer", "minimum": 8192, "default": 67108864}
            }
        },
        "resource": {
            "type": "object",
            "additionalProperties": false,
            "required": ["database", "table"],
            "properties": {
                "database": {"type": "string", "minLength": 1},
                "table": {"type": "string", "minLength": 1},
                "partition_expression": {"type": "string", "minLength": 1},
                "format": {"type": "string", "minLength": 1},
                "format_options": {"type": "object", "default": {}}
            }
        }
    })
}

pub fn glue_source_descriptor() -> cdf_kernel::Result<cdf_runtime::SourceDriverDescriptor> {
    let option_schema = glue_option_schema();
    Ok(cdf_runtime::SourceDriverDescriptor {
        driver_id: cdf_runtime::SourceDriverId::new("glue")?,
        driver_version: GLUE_SOURCE_DRIVER_VERSION.to_owned(),
        option_schema_hash: cdf_runtime::artifact_hash(&option_schema)?,
        kinds: vec!["glue".to_owned()],
        // Direct `glue://` references require catalog I/O before a physical plan can exist.
        // `cdf add` owns that authoring-time classification; project compilation consumes the
        // resulting typed source/resource options rather than advertising a compiler we do not
        // implement.
        schemes: Vec::new(),
    })
}
