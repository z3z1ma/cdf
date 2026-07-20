#![doc = "Apache Iceberg source adapter for cdf."]

use arrow_schema::Schema;
use cdf_kernel::{CdfError, Result};
use cdf_runtime::{SourceDriverDescriptor, SourceDriverId, artifact_hash};

mod scan_task;

pub use scan_task::{
    ICEBERG_SCAN_TASK_VERSION, ICEBERG_TASK_SET_AUTHORITY_VERSION, ICEBERG_TASK_SET_TYPE,
    IcebergDataFile, IcebergDeleteContent, IcebergDeleteFile, IcebergFileFormat,
    IcebergJsonAuthority, IcebergReaderRequirements, IcebergScanTask, IcebergTaskSetAuthority,
};

pub const ICEBERG_SOURCE_DRIVER_VERSION: &str = "1.0.0";

/// Returns the versioned, deterministic configuration schema owned by the Iceberg source.
///
/// Catalog-specific values remain inside a tagged Iceberg-owned object. Credentials are secret
/// references, never operational values, and the table selector is structurally exclusive so a
/// compiled resource cannot ambiguously name a branch, tag, snapshot, and timestamp at once.
pub fn iceberg_option_schema() -> serde_json::Value {
    let secret_reference = serde_json::json!({
        "type": "string",
        "pattern": "^secret://"
    });
    let egress_allowlist = serde_json::json!({
        "type": "array",
        "items": {"type": "string", "minLength": 1},
        "uniqueItems": true
    });
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "source": {
            "type": "object",
            "additionalProperties": false,
            "required": ["catalog"],
            "properties": {
                "catalog": {
                    "oneOf": [
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["kind", "warehouse"],
                            "properties": {
                                "kind": {"const": "filesystem"},
                                "warehouse": {"type": "string", "minLength": 1}
                            }
                        },
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["kind", "uri"],
                            "properties": {
                                "kind": {"const": "rest"},
                                "uri": {"type": "string", "minLength": 1},
                                "warehouse": {"type": "string", "minLength": 1},
                                "credentials": secret_reference
                            }
                        },
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["kind", "region"],
                            "properties": {
                                "kind": {"const": "glue"},
                                "region": {"type": "string", "minLength": 1},
                                "catalog_id": {"type": "string", "minLength": 1},
                                "warehouse": {"type": "string", "minLength": 1},
                                "endpoint": {"type": "string", "minLength": 1},
                                "credentials": secret_reference
                            }
                        }
                    ]
                },
                "object_credentials": secret_reference,
                "egress_allowlist": egress_allowlist
            }
        },
        "resource": {
            "type": "object",
            "additionalProperties": false,
            "required": ["namespace", "table"],
            "properties": {
                "namespace": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"type": "string", "minLength": 1}
                },
                "table": {"type": "string", "minLength": 1},
                "selector": {
                    "oneOf": [
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["kind", "name"],
                            "properties": {
                                "kind": {"const": "branch"},
                                "name": {"type": "string", "minLength": 1}
                            }
                        },
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["kind", "name"],
                            "properties": {
                                "kind": {"const": "tag"},
                                "name": {"type": "string", "minLength": 1}
                            }
                        },
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["kind", "snapshot_id"],
                            "properties": {
                                "kind": {"const": "snapshot"},
                                "snapshot_id": {"type": "integer", "minimum": 1}
                            }
                        },
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["kind", "timestamp_ms"],
                            "properties": {
                                "kind": {"const": "timestamp"},
                                "timestamp_ms": {"type": "integer", "minimum": 0}
                            }
                        }
                    ]
                }
            }
        }
    })
}

pub fn iceberg_source_descriptor() -> Result<SourceDriverDescriptor> {
    let option_schema = iceberg_option_schema();
    Ok(SourceDriverDescriptor {
        driver_id: SourceDriverId::new("iceberg")?,
        driver_version: ICEBERG_SOURCE_DRIVER_VERSION.to_owned(),
        option_schema_hash: artifact_hash(&option_schema)?,
        kinds: vec!["iceberg".to_owned()],
        schemes: Vec::new(),
    })
}

/// Decodes an Iceberg schema object into CDF's Arrow type without exposing Iceberg types.
///
/// The field-id metadata added by Iceberg's canonical Arrow conversion is preserved. Table
/// metadata selection and schema governance remain later compiler stages; this narrow bridge
/// proves and enforces the dependency tuple at the source boundary.
pub fn decode_arrow_schema(schema_json: &[u8]) -> Result<Schema> {
    let schema: iceberg::spec::Schema = serde_json::from_slice(schema_json)
        .map_err(|error| CdfError::data(format!("decode Iceberg schema JSON: {error}")))?;
    iceberg::arrow::schema_to_arrow_schema(&schema)
        .map_err(|error| CdfError::data(format!("convert Iceberg schema to Arrow: {error}")))
}

#[cfg(test)]
mod tests {
    use arrow_schema::DataType;

    use super::*;

    #[test]
    fn descriptor_and_option_schema_are_canonical() {
        let descriptor = iceberg_source_descriptor().unwrap();
        descriptor.validate().unwrap();
        assert_eq!(descriptor.driver_id.as_str(), "iceberg");
        assert_eq!(descriptor.driver_version, "1.0.0");
        assert_eq!(descriptor.kinds, ["iceberg"]);
        assert!(descriptor.schemes.is_empty());
        assert_eq!(
            descriptor.option_schema_hash,
            "sha256:044c3e033767575a7bf2a877791977340c769d0fcc75c0023aae6823b2ca7444"
        );
        assert_eq!(
            descriptor.option_schema_hash,
            artifact_hash(&iceberg_option_schema()).unwrap()
        );
    }

    #[test]
    fn schema_bridge_is_arrow_58_native_and_preserves_field_ids() {
        let schema = decode_arrow_schema(
            br#"{
                "type": "struct",
                "schema-id": 7,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long"},
                    {"id": 2, "name": "label", "required": false, "type": "string"}
                ]
            }"#,
        )
        .unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(
            schema
                .field(0)
                .metadata()
                .get("PARQUET:field_id")
                .map(String::as_str),
            Some("1")
        );
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert!(schema.field(1).is_nullable());
    }

    #[test]
    fn malformed_schema_fails_without_an_upstream_type_escape() {
        let error = decode_arrow_schema(br#"{"type":"struct","fields":"wrong"}"#).unwrap_err();
        assert!(error.message.contains("decode Iceberg schema JSON"));
    }
}
