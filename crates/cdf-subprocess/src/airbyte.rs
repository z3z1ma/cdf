use cdf_kernel::{Result, SourcePosition};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::protocol::{
    StreamIdentity, malformed_field, object_message, optional_string, required_integer,
    required_object, required_string,
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum AirbyteMessage {
    Catalog(AirbyteCatalog),
    Record(AirbyteRecord),
    State(AirbyteState),
    Other(AirbyteOther),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AirbyteCatalog {
    pub catalog: Value,
    pub raw: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AirbyteRecord {
    pub stream: String,
    pub namespace: Option<String>,
    pub data: Value,
    pub emitted_at: Value,
    pub raw: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AirbyteState {
    pub kind: AirbyteStateKind,
    pub stream: Option<StreamIdentity>,
    pub value: Value,
    pub raw: Value,
}

impl AirbyteState {
    /// Converts the protocol checkpoint into CDF's opaque, hash-addressed position.
    pub fn source_position(&self) -> Result<SourcePosition> {
        crate::protocol::foreign_state("airbyte", &self.value)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AirbyteStateKind {
    Legacy,
    Stream,
    Global,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AirbyteOther {
    pub message_type: String,
    pub raw: Value,
}

/// Decodes exactly one Airbyte NDJSON message without collecting a foreign stream.
pub fn decode_airbyte_message(line: usize, bytes: &[u8]) -> Result<Option<AirbyteMessage>> {
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(None);
    }
    let value = serde_json::from_slice(bytes).map_err(|error| {
        cdf_kernel::CdfError::data(format!(
            "Airbyte message line {line} is not valid JSON: {error}"
        ))
    })?;
    parse_airbyte_message(line, value).map(Some)
}

fn parse_airbyte_message(line: usize, value: Value) -> Result<AirbyteMessage> {
    let object = object_message(&value, "Airbyte", line)?;
    let message_type = required_string(object, "type", "Airbyte", "message", line)?;
    match message_type.to_ascii_uppercase().as_str() {
        "CATALOG" => parse_catalog(line, value).map(AirbyteMessage::Catalog),
        "RECORD" => parse_record(line, value).map(AirbyteMessage::Record),
        "STATE" => parse_state(line, value).map(AirbyteMessage::State),
        _ => Ok(AirbyteMessage::Other(AirbyteOther {
            message_type,
            raw: value,
        })),
    }
}

fn parse_catalog(line: usize, value: Value) -> Result<AirbyteCatalog> {
    let object = object_message(&value, "Airbyte", line)?;
    Ok(AirbyteCatalog {
        catalog: required_object(object, "catalog", "Airbyte", "CATALOG", line)?,
        raw: value,
    })
}

fn parse_record(line: usize, value: Value) -> Result<AirbyteRecord> {
    let object = object_message(&value, "Airbyte", line)?;
    let record = required_object(object, "record", "Airbyte", "RECORD", line)?;
    let record_object = record.as_object().expect("required object");
    required_integer(record_object, "emitted_at", "Airbyte", "RECORD", line)?;
    Ok(AirbyteRecord {
        stream: required_string(record_object, "stream", "Airbyte", "RECORD", line)?,
        namespace: optional_string(record_object, "namespace", "Airbyte", "RECORD", line)?,
        data: required_object(record_object, "data", "Airbyte", "RECORD", line)?,
        emitted_at: record_object
            .get("emitted_at")
            .expect("validated emitted_at")
            .clone(),
        raw: value,
    })
}

fn parse_state(line: usize, value: Value) -> Result<AirbyteState> {
    let object = object_message(&value, "Airbyte", line)?;
    let state = required_object(object, "state", "Airbyte", "STATE", line)?;
    let state_object = state.as_object().expect("required object");
    let kind = state_kind(state_object.get("type"), line)?;
    match kind {
        AirbyteStateKind::Legacy if state_object.contains_key("type") => {
            if !state_object.contains_key("data") {
                return Err(malformed_field(
                    "Airbyte",
                    "STATE",
                    "state.data",
                    line,
                    "present for LEGACY state",
                ));
            }
        }
        AirbyteStateKind::Stream => {}
        AirbyteStateKind::Global => {
            if !state_object
                .get("global")
                .map(Value::is_object)
                .unwrap_or(false)
            {
                return Err(malformed_field(
                    "Airbyte",
                    "STATE",
                    "state.global",
                    line,
                    "object",
                ));
            }
        }
        AirbyteStateKind::Legacy => {}
    }

    let stream = match kind {
        AirbyteStateKind::Stream => Some(required_stream_state_identity(state_object, line)?),
        AirbyteStateKind::Legacy | AirbyteStateKind::Global => None,
    };
    Ok(AirbyteState {
        stream,
        kind,
        value: state,
        raw: value,
    })
}

fn state_kind(value: Option<&Value>, line: usize) -> Result<AirbyteStateKind> {
    match value {
        None | Some(Value::Null) => Ok(AirbyteStateKind::Legacy),
        Some(Value::String(value)) => match value.to_ascii_uppercase().as_str() {
            "LEGACY" => Ok(AirbyteStateKind::Legacy),
            "STREAM" => Ok(AirbyteStateKind::Stream),
            "GLOBAL" => Ok(AirbyteStateKind::Global),
            _ => Err(malformed_field(
                "Airbyte",
                "STATE",
                "state.type",
                line,
                "LEGACY, STREAM, or GLOBAL",
            )),
        },
        Some(_) => Err(malformed_field(
            "Airbyte",
            "STATE",
            "state.type",
            line,
            "string or null",
        )),
    }
}

fn required_stream_state_identity(
    state: &serde_json::Map<String, Value>,
    line: usize,
) -> Result<StreamIdentity> {
    let stream = state
        .get("stream")
        .and_then(Value::as_object)
        .ok_or_else(|| malformed_field("Airbyte", "STATE", "state.stream", line, "object"))?;
    let descriptor = stream
        .get("stream_descriptor")
        .and_then(Value::as_object)
        .unwrap_or(stream);
    let identity = StreamIdentity::airbyte(
        optional_string(
            descriptor,
            "namespace",
            "Airbyte",
            "STATE stream descriptor",
            line,
        )?,
        required_string(
            descriptor,
            "name",
            "Airbyte",
            "STATE stream descriptor",
            line,
        )?,
    );
    identity.validate().map_err(|_| {
        malformed_field(
            "Airbyte",
            "STATE",
            "state.stream.stream_descriptor",
            line,
            "a non-empty stream identity",
        )
    })?;
    Ok(identity)
}
