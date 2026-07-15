use crate::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetObjectManifest {
    pub manifest_version: u16,
    pub destination: String,
    pub target: String,
    pub package_hash: String,
    pub idempotency_token: String,
    pub disposition: WriteDisposition,
    pub schema_hash: String,
    pub committed_at_ms: i64,
    pub total_rows: u64,
    pub objects: Vec<ParquetObjectEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetObjectEntry {
    pub key: String,
    pub row_count: u64,
    pub byte_count: u64,
    pub package_byte_count: u64,
    pub parquet_byte_count: u64,
    pub sha256: String,
    pub etag: Option<String>,
    pub schema_hash: String,
    pub segments: Vec<ParquetObjectSegmentEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetObjectSegmentEntry {
    pub segment_id: String,
    pub row_offset: u64,
    pub row_count: u64,
    pub byte_count: u64,
    pub package_byte_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetReplacePointerReceipt {
    pub key: String,
    pub sha256: String,
    pub etag: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplacePointer {
    pub pointer_version: u16,
    pub target: String,
    pub package_hash: String,
    pub idempotency_token: String,
    pub schema_hash: String,
    pub manifest_key: String,
    pub manifest_sha256: String,
    pub updated_at_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetCorrectionSidecar {
    pub sidecar_version: u16,
    pub destination: String,
    pub target: String,
    pub correction_package_hash: String,
    pub idempotency_token: String,
    pub resource_disposition: WriteDisposition,
    pub promotion_id: PromotionId,
    pub old_schema_hash: SchemaHash,
    pub new_schema_hash: SchemaHash,
    pub operations_digest: String,
    pub base_target_unchanged: bool,
    pub operations: Vec<DestinationCorrectionOperation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetCorrectionSidecarManifest {
    pub manifest_version: u16,
    pub destination: String,
    pub target: String,
    pub correction_package_hash: String,
    pub idempotency_token: String,
    pub resource_disposition: WriteDisposition,
    pub promotion_id: PromotionId,
    pub old_schema_hash: SchemaHash,
    pub new_schema_hash: SchemaHash,
    pub operations_digest: String,
    pub operation_count: u64,
    pub addressed_rows: u64,
    pub segments: Vec<SegmentAck>,
    pub base_target_unchanged: bool,
    pub objects: Vec<ParquetCorrectionSidecarObject>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetCorrectionSidecarObject {
    pub key: String,
    pub sha256: String,
    pub byte_count: u64,
    pub operation_count: u64,
}

pub(crate) fn canonical_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)
        .map_err(|error| CdfError::data(format!("serialize JSON value: {error}")))?;
    let mut output = Vec::new();
    write_canonical_value(&value, &mut output)?;
    Ok(output)
}

fn write_canonical_value(value: &serde_json::Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        serde_json::Value::Null => output.extend_from_slice(b"null"),
        serde_json::Value::Bool(value) => {
            output.extend_from_slice(if *value { b"true" } else { b"false" })
        }
        serde_json::Value::Number(number) => {
            output.extend_from_slice(number.to_string().as_bytes())
        }
        serde_json::Value::String(value) => write_canonical_string(value, output)?,
        serde_json::Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_value(value, output)?;
            }
            output.push(b']');
        }
        serde_json::Value::Object(map) => {
            output.push(b'{');
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_string(key, output)?;
                output.push(b':');
                write_canonical_value(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn write_canonical_string(value: &str, output: &mut Vec<u8>) -> Result<()> {
    let escaped = serde_json::to_string(value)
        .map_err(|error| CdfError::data(format!("serialize JSON string: {error}")))?;
    output.extend_from_slice(escaped.as_bytes());
    Ok(())
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
