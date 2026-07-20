use std::collections::BTreeMap;

use cdf_kernel::{CdfError, Result};
use cdf_runtime::artifact_hash;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GlueColumn {
    pub name: String,
    #[serde(rename = "Type")]
    pub type_name: String,
    #[serde(default)]
    pub comment: Option<String>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GlueSerdeInfo {
    #[serde(default)]
    pub serialization_library: Option<String>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GlueStorageDescriptor {
    #[serde(default)]
    pub columns: Vec<GlueColumn>,
    #[serde(default)]
    pub location: Option<String>,
    #[serde(default)]
    pub input_format: Option<String>,
    #[serde(default)]
    pub output_format: Option<String>,
    #[serde(default)]
    pub compressed: Option<bool>,
    #[serde(default)]
    pub serde_info: Option<GlueSerdeInfo>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GlueTable {
    pub name: String,
    #[serde(default)]
    pub database_name: Option<String>,
    #[serde(default)]
    pub catalog_id: Option<String>,
    #[serde(default)]
    pub version_id: Option<String>,
    #[serde(default)]
    pub update_time: Option<serde_json::Number>,
    #[serde(default)]
    pub table_type: Option<String>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
    #[serde(default)]
    pub partition_keys: Vec<GlueColumn>,
    #[serde(default)]
    pub storage_descriptor: Option<GlueStorageDescriptor>,
    #[serde(default)]
    pub view_original_text: Option<String>,
    #[serde(default)]
    pub view_expanded_text: Option<String>,
    #[serde(default)]
    pub target_table: Option<serde_json::Value>,
    #[serde(default)]
    pub is_registered_with_lake_formation: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GluePartition {
    #[serde(default)]
    pub values: Vec<String>,
    #[serde(default)]
    pub storage_descriptor: Option<GlueStorageDescriptor>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GlueTableClass {
    Conventional(GlueFormatMapping),
    Iceberg,
    Delta,
    Hudi,
    View,
    Federated,
    Stream,
    UnsupportedSerde { serde: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlueFormatMapping {
    pub format_id: String,
    pub options: serde_json::Value,
}

pub fn classify_table(table: &GlueTable, override_format: Option<&str>) -> Result<GlueTableClass> {
    if table.target_table.is_some() {
        return Ok(GlueTableClass::Federated);
    }
    let table_type = table.table_type.as_deref().unwrap_or_default();
    if table_type.eq_ignore_ascii_case("VIRTUAL_VIEW")
        || table.view_original_text.is_some()
        || table.view_expanded_text.is_some()
    {
        return Ok(GlueTableClass::View);
    }
    let classification = parameter_ci(&table.parameters, "classification").unwrap_or_default();
    let declared_type = parameter_ci(&table.parameters, "table_type").unwrap_or_default();
    if [table_type, classification, declared_type]
        .iter()
        .any(|value| value.eq_ignore_ascii_case("iceberg"))
    {
        return Ok(GlueTableClass::Iceberg);
    }
    if [table_type, classification, declared_type]
        .iter()
        .any(|value| value.eq_ignore_ascii_case("delta"))
    {
        return Ok(GlueTableClass::Delta);
    }
    if [table_type, classification, declared_type]
        .iter()
        .any(|value| value.eq_ignore_ascii_case("hudi"))
    {
        return Ok(GlueTableClass::Hudi);
    }
    let descriptor = table
        .storage_descriptor
        .as_ref()
        .ok_or_else(|| CdfError::data("AWS Glue table omitted its StorageDescriptor"))?;
    let input = descriptor.input_format.as_deref().unwrap_or_default();
    let serde = descriptor
        .serde_info
        .as_ref()
        .and_then(|value| value.serialization_library.as_deref())
        .unwrap_or_default();
    let protocol = format!("{input} {serde}").to_ascii_lowercase();
    if protocol.contains("kinesis") || protocol.contains("dynamodb") {
        return Ok(GlueTableClass::Stream);
    }
    if protocol.contains("jdbc") {
        return Ok(GlueTableClass::Federated);
    }
    if let Some(format_id) = override_format {
        return Ok(GlueTableClass::Conventional(GlueFormatMapping {
            format_id: format_id.to_owned(),
            options: serde_json::json!({}),
        }));
    }
    let format_id =
        if classification.eq_ignore_ascii_case("parquet") || protocol.contains("parquet") {
            "parquet"
        } else if classification.eq_ignore_ascii_case("avro") || protocol.contains("avro") {
            "avro_ocf"
        } else if classification.eq_ignore_ascii_case("json") || protocol.contains("jsonserde") {
            "ndjson"
        } else if classification.eq_ignore_ascii_case("csv")
            || protocol.contains("opencsvserde")
            || protocol.contains("lazysimpleserde")
        {
            "csv"
        } else {
            return Ok(GlueTableClass::UnsupportedSerde {
                serde: if serde.is_empty() {
                    input.to_owned()
                } else {
                    serde.to_owned()
                },
            });
        };
    let mut options = serde_json::Map::new();
    if format_id == "csv" {
        let params = descriptor
            .serde_info
            .as_ref()
            .map(|value| &value.parameters)
            .unwrap_or(&descriptor.parameters);
        if let Some(delimiter) = parameter_ci(params, "separatorChar")
            .or_else(|| parameter_ci(params, "field.delim"))
            .or_else(|| parameter_ci(params, "serialization.format"))
        {
            options.insert("delimiter".to_owned(), serde_json::json!(delimiter));
        }
        for (source, target) in [
            ("quoteChar", "quote"),
            ("escapeChar", "escape"),
            ("escape.delim", "escape"),
        ] {
            if let Some(value) = parameter_ci(params, source) {
                options.insert(target.to_owned(), serde_json::json!(value));
            }
        }
        let header = match parameter_ci(&table.parameters, "skip.header.line.count") {
            Some(header) => {
                let count = header.parse::<u64>().map_err(|_| {
                    CdfError::data("Glue skip.header.line.count must be an unsigned integer")
                })?;
                if count > 1 {
                    return Err(CdfError::contract(format!(
                        "Glue CSV skip.header.line.count `{count}` is unsupported; CDF can skip exactly one header record, or use Athena/Trino for this table"
                    )));
                }
                count == 1
            }
            None => false,
        };
        options.insert("header".to_owned(), serde_json::json!(header));
    }
    Ok(GlueTableClass::Conventional(GlueFormatMapping {
        format_id: format_id.to_owned(),
        options: serde_json::Value::Object(options),
    }))
}

pub(crate) fn table_generation(table: &GlueTable) -> Result<String> {
    table.version_id.clone().map_or_else(
        || artifact_hash(table),
        |version| Ok(format!("glue-version:{version}")),
    )
}

pub fn merge_descriptor(
    table: &GlueStorageDescriptor,
    partition: Option<&GlueStorageDescriptor>,
) -> Result<GlueStorageDescriptor> {
    let mut merged = table.clone();
    if let Some(partition) = partition {
        if !partition.columns.is_empty() {
            merged.columns = partition.columns.clone();
        }
        if partition.location.is_some() {
            merged.location = partition.location.clone();
        }
        if partition.input_format.is_some() {
            merged.input_format = partition.input_format.clone();
        }
        if partition.output_format.is_some() {
            merged.output_format = partition.output_format.clone();
        }
        if partition.serde_info.is_some() {
            merged.serde_info = partition.serde_info.clone();
        }
        if partition.compressed.is_some() {
            merged.compressed = partition.compressed;
        }
        merged.parameters.extend(partition.parameters.clone());
    }
    let location = merged.location.as_deref().unwrap_or_default();
    if !location.starts_with("s3://") {
        return Err(CdfError::contract(format!(
            "Glue conventional external-table location `{location}` is not an S3 object prefix; use the source that owns this storage protocol"
        )));
    }
    Ok(merged)
}

fn parameter_ci<'a>(parameters: &'a BTreeMap<String, String>, key: &str) -> Option<&'a str> {
    parameters
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(key))
        .map(|(_, value)| value.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(classification: Option<&str>, serde: Option<&str>) -> GlueTable {
        GlueTable {
            name: "events".to_owned(),
            database_name: Some("analytics".to_owned()),
            catalog_id: Some("123456789012".to_owned()),
            version_id: Some("7".to_owned()),
            update_time: None,
            table_type: Some("EXTERNAL_TABLE".to_owned()),
            parameters: classification
                .map(|value| BTreeMap::from([("classification".to_owned(), value.to_owned())]))
                .unwrap_or_default(),
            partition_keys: Vec::new(),
            storage_descriptor: Some(GlueStorageDescriptor {
                columns: vec![GlueColumn {
                    name: "id".to_owned(),
                    type_name: "bigint".to_owned(),
                    comment: None,
                    parameters: BTreeMap::new(),
                }],
                location: Some("s3://lake/analytics/events/".to_owned()),
                input_format: None,
                output_format: None,
                compressed: None,
                serde_info: Some(GlueSerdeInfo {
                    serialization_library: serde.map(str::to_owned),
                    parameters: BTreeMap::new(),
                }),
                parameters: BTreeMap::new(),
            }),
            view_original_text: None,
            view_expanded_text: None,
            target_table: None,
            is_registered_with_lake_formation: false,
        }
    }

    #[test]
    fn classification_routes_owned_table_families_before_format_override() {
        for (classification, expected) in [
            ("iceberg", GlueTableClass::Iceberg),
            ("delta", GlueTableClass::Delta),
            ("hudi", GlueTableClass::Hudi),
        ] {
            let observed =
                classify_table(&table(Some(classification), None), Some("parquet")).unwrap();
            assert_eq!(observed, expected);
        }

        let mut view = table(Some("parquet"), None);
        view.view_original_text = Some("select 1".to_owned());
        assert_eq!(
            classify_table(&view, Some("parquet")).unwrap(),
            GlueTableClass::View
        );

        let mut federated = table(Some("parquet"), None);
        federated.target_table = Some(serde_json::json!({"CatalogId": "remote"}));
        assert_eq!(
            classify_table(&federated, Some("parquet")).unwrap(),
            GlueTableClass::Federated
        );
    }

    #[test]
    fn conventional_formats_and_partition_overrides_are_exact() {
        let parquet = classify_table(
            &table(
                None,
                Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
            ),
            None,
        )
        .unwrap();
        assert_eq!(
            parquet,
            GlueTableClass::Conventional(GlueFormatMapping {
                format_id: "parquet".to_owned(),
                options: serde_json::json!({}),
            })
        );

        let table_descriptor = table(Some("parquet"), None).storage_descriptor.unwrap();
        let partition = GlueStorageDescriptor {
            location: Some("s3://lake/analytics/events/day=2026-07-20/".to_owned()),
            compressed: Some(true),
            ..GlueStorageDescriptor::default()
        };
        let merged = merge_descriptor(&table_descriptor, Some(&partition)).unwrap();
        assert_eq!(merged.location, partition.location);
        assert_eq!(merged.columns, table_descriptor.columns);
        assert_eq!(merged.compressed, Some(true));

        for (classification, format_id) in [("avro", "avro_ocf"), ("json", "ndjson")] {
            assert_eq!(
                classify_table(&table(Some(classification), None), None).unwrap(),
                GlueTableClass::Conventional(GlueFormatMapping {
                    format_id: format_id.to_owned(),
                    options: serde_json::json!({}),
                })
            );
        }
        assert_eq!(
            classify_table(&table(Some("csv"), None), None).unwrap(),
            GlueTableClass::Conventional(GlueFormatMapping {
                format_id: "csv".to_owned(),
                options: serde_json::json!({"header": false}),
            })
        );
    }

    #[test]
    fn unknown_serde_is_not_guessed() {
        assert_eq!(
            classify_table(&table(None, Some("com.acme.CustomSerde")), None).unwrap(),
            GlueTableClass::UnsupportedSerde {
                serde: "com.acme.CustomSerde".to_owned(),
            }
        );
    }
}
