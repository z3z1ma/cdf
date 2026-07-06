use std::collections::BTreeMap;

use firn_formats::{FormatRead, JsonOptions, ReadOptions};
use firn_kernel::{FirnError, ForeignState, Result, ScopeKey, SourcePosition};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StreamIdentity {
    pub namespace: Option<String>,
    pub name: String,
}

impl StreamIdentity {
    pub fn singer(name: impl Into<String>) -> Self {
        Self {
            namespace: None,
            name: name.into(),
        }
    }

    pub fn airbyte(namespace: Option<String>, name: impl Into<String>) -> Self {
        Self {
            namespace,
            name: name.into(),
        }
    }

    pub fn scope_name(&self) -> String {
        match &self.namespace {
            Some(namespace) => format!("{namespace}.{}", self.name),
            None => self.name.clone(),
        }
    }

    fn batch_id_part(&self) -> String {
        let value = match &self.namespace {
            Some(namespace) => format!("{namespace}-{}", self.name),
            None => self.name.clone(),
        };
        sanitize_id_part(&value)
    }
}

#[derive(Clone, Debug)]
pub struct ProtocolStreamRead {
    pub stream: StreamIdentity,
    pub read: FormatRead,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolState {
    pub kind: String,
    pub stream: Option<StreamIdentity>,
    pub position: SourcePosition,
}

pub(crate) fn records_to_stream_reads(
    records: impl IntoIterator<Item = (StreamIdentity, Value)>,
    options: &ReadOptions,
) -> Result<Vec<ProtocolStreamRead>> {
    let mut by_stream = BTreeMap::<StreamIdentity, Vec<Value>>::new();
    for (stream, record) in records {
        by_stream.entry(stream).or_default().push(record);
    }

    let mut streams = Vec::with_capacity(by_stream.len());
    for (stream, rows) in by_stream {
        let read_options = options.clone().with_batch_id_prefix(format!(
            "{}-{}",
            options.batch_id_prefix,
            stream.batch_id_part()
        ))?;
        let bytes = ndjson_bytes(&rows)?;
        let mut read =
            firn_formats::read_ndjson_bytes(&bytes, &read_options, &JsonOptions::default())?;
        read.descriptor.state_scope = ScopeKey::Stream {
            name: stream.scope_name(),
        };
        streams.push(ProtocolStreamRead { stream, read });
    }

    Ok(streams)
}

pub(crate) fn foreign_state(protocol: &str, value: &Value) -> Result<SourcePosition> {
    let opaque_blob = canonical_json_bytes(value)?;
    let mut hasher = Sha256::new();
    hasher.update(&opaque_blob);
    Ok(SourcePosition::ForeignState(ForeignState {
        version: 1,
        protocol: protocol.to_owned(),
        blob_sha256: format!("sha256:{}", hex::encode(hasher.finalize())),
        opaque_blob,
    }))
}

fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    write_canonical_value(value, &mut output)?;
    Ok(output)
}

fn write_canonical_value(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(number) => output.extend_from_slice(number.to_string().as_bytes()),
        Value::String(value) => write_canonical_string(value, output)?,
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_value(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(map) => {
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
    let escaped =
        serde_json::to_string(value).map_err(|error| FirnError::data(error.to_string()))?;
    output.extend_from_slice(escaped.as_bytes());
    Ok(())
}

pub(crate) fn json_lines(bytes: &[u8], protocol: &str) -> Result<Vec<(usize, Value)>> {
    let text = std::str::from_utf8(bytes).map_err(|error| {
        FirnError::data(format!("{protocol} stdout is not valid UTF-8: {error}"))
    })?;
    let mut values = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value = serde_json::from_str(line).map_err(|error| {
            FirnError::data(format!(
                "{protocol} message line {} is not valid JSON: {error}",
                index + 1
            ))
        })?;
        values.push((index + 1, value));
    }
    Ok(values)
}

pub(crate) fn object_message<'a>(
    value: &'a Value,
    protocol: &str,
    line: usize,
) -> Result<&'a Map<String, Value>> {
    value.as_object().ok_or_else(|| {
        FirnError::data(format!(
            "{protocol} message line {line} must be a JSON object"
        ))
    })
}

pub(crate) fn required_string(
    object: &Map<String, Value>,
    field: &str,
    protocol: &str,
    message_type: &str,
    line: usize,
) -> Result<String> {
    match object.get(field).and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(value.to_owned()),
        _ => Err(malformed_field(
            protocol,
            message_type,
            field,
            line,
            "string",
        )),
    }
}

pub(crate) fn optional_string(
    object: &Map<String, Value>,
    field: &str,
    protocol: &str,
    message_type: &str,
    line: usize,
) -> Result<Option<String>> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(malformed_field(
            protocol,
            message_type,
            field,
            line,
            "string or null",
        )),
    }
}

pub(crate) fn required_object(
    object: &Map<String, Value>,
    field: &str,
    protocol: &str,
    message_type: &str,
    line: usize,
) -> Result<Value> {
    match object.get(field) {
        Some(value) if value.is_object() => Ok(value.clone()),
        _ => Err(malformed_field(
            protocol,
            message_type,
            field,
            line,
            "object",
        )),
    }
}

pub(crate) fn required_array_strings(
    object: &Map<String, Value>,
    field: &str,
    protocol: &str,
    message_type: &str,
    line: usize,
) -> Result<Vec<String>> {
    match object.get(field) {
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value.as_str().map(ToOwned::to_owned).ok_or_else(|| {
                    malformed_field(protocol, message_type, field, line, "array of strings")
                })
            })
            .collect(),
        _ => Err(malformed_field(
            protocol,
            message_type,
            field,
            line,
            "array of strings",
        )),
    }
}

pub(crate) fn optional_array_strings(
    object: &Map<String, Value>,
    field: &str,
    protocol: &str,
    message_type: &str,
    line: usize,
) -> Result<Vec<String>> {
    match object.get(field) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(_)) => {
            required_array_strings(object, field, protocol, message_type, line)
        }
        Some(_) => Err(malformed_field(
            protocol,
            message_type,
            field,
            line,
            "array of strings or null",
        )),
    }
}

pub(crate) fn required_integer(
    object: &Map<String, Value>,
    field: &str,
    protocol: &str,
    message_type: &str,
    line: usize,
) -> Result<()> {
    match object.get(field) {
        Some(Value::Number(number)) if number.is_i64() || number.is_u64() => Ok(()),
        _ => Err(malformed_field(
            protocol,
            message_type,
            field,
            line,
            "integer",
        )),
    }
}

pub(crate) fn malformed_field(
    protocol: &str,
    message_type: &str,
    field: &str,
    line: usize,
    expected: &str,
) -> FirnError {
    FirnError::data(format!(
        "malformed {protocol} {message_type} message at line {line}: required field `{field}` must be {expected}"
    ))
}

fn ndjson_bytes(rows: &[Value]) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    for row in rows {
        serde_json::to_writer(&mut output, row)
            .map_err(|error| FirnError::data(error.to_string()))?;
        output.push(b'\n');
    }
    Ok(output)
}

fn sanitize_id_part(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}
