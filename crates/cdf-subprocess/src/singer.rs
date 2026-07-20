use cdf_kernel::{Result, SourcePosition};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::protocol::{
    malformed_field, object_message, optional_array_strings, optional_string,
    required_array_strings, required_object, required_string,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SingerMessage {
    Schema(SingerSchema),
    Record(SingerRecord),
    State(SingerState),
    Other(SingerOther),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SingerSchema {
    pub stream: String,
    pub schema: Value,
    pub key_properties: Vec<String>,
    pub bookmark_properties: Vec<String>,
    pub raw: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SingerRecord {
    pub stream: String,
    pub record: Value,
    pub time_extracted: Option<String>,
    pub raw: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SingerState {
    pub value: Value,
    pub raw: Value,
}

impl SingerState {
    /// Converts the protocol checkpoint into CDF's opaque, hash-addressed position.
    pub fn source_position(&self) -> Result<SourcePosition> {
        crate::protocol::foreign_state("singer", &self.value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SingerOther {
    pub message_type: String,
    pub raw: Value,
}

/// Decodes exactly one Singer NDJSON message without collecting a foreign stream.
pub fn decode_singer_message(line: usize, bytes: &[u8]) -> Result<Option<SingerMessage>> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    let value = serde_json::from_slice(bytes).map_err(|error| {
        cdf_kernel::CdfError::data(format!(
            "Singer message line {line} is not valid JSON: {error}"
        ))
    })?;
    parse_singer_message(line, value).map(Some)
}

fn parse_singer_message(line: usize, value: Value) -> Result<SingerMessage> {
    let object = object_message(&value, "Singer", line)?;
    let message_type = required_string(object, "type", "Singer", "message", line)?;
    match message_type.to_ascii_uppercase().as_str() {
        "SCHEMA" => parse_schema(line, value).map(SingerMessage::Schema),
        "RECORD" => parse_record(line, value).map(SingerMessage::Record),
        "STATE" => parse_state(line, value).map(SingerMessage::State),
        _ => Ok(SingerMessage::Other(SingerOther {
            message_type,
            raw: value,
        })),
    }
}

fn parse_schema(line: usize, value: Value) -> Result<SingerSchema> {
    let object = object_message(&value, "Singer", line)?;
    Ok(SingerSchema {
        stream: required_string(object, "stream", "Singer", "SCHEMA", line)?,
        schema: required_object(object, "schema", "Singer", "SCHEMA", line)?,
        key_properties: required_array_strings(object, "key_properties", "Singer", "SCHEMA", line)?,
        bookmark_properties: optional_array_strings(
            object,
            "bookmark_properties",
            "Singer",
            "SCHEMA",
            line,
        )?,
        raw: value,
    })
}

fn parse_record(line: usize, value: Value) -> Result<SingerRecord> {
    let object = object_message(&value, "Singer", line)?;
    Ok(SingerRecord {
        stream: required_string(object, "stream", "Singer", "RECORD", line)?,
        record: required_object(object, "record", "Singer", "RECORD", line)?,
        time_extracted: optional_string(object, "time_extracted", "Singer", "RECORD", line)?,
        raw: value,
    })
}

fn parse_state(line: usize, value: Value) -> Result<SingerState> {
    let object = object_message(&value, "Singer", line)?;
    let state = object
        .get("value")
        .cloned()
        .ok_or_else(|| malformed_field("Singer", "STATE", "value", line, "present"))?;
    Ok(SingerState {
        value: state,
        raw: value,
    })
}
