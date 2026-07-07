use cdf_formats::ReadOptions;
use cdf_kernel::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::protocol::{
    ProtocolState, ProtocolStreamRead, StreamIdentity, foreign_state, json_lines, malformed_field,
    object_message, optional_string, records_to_stream_reads, required_integer, required_object,
    required_string,
};

#[derive(Clone, Debug)]
pub struct AirbyteRead {
    pub messages: Vec<AirbyteMessage>,
    pub catalogs: Vec<AirbyteCatalog>,
    pub streams: Vec<ProtocolStreamRead>,
    pub states: Vec<ProtocolState>,
}

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AirbyteStateKind {
    Legacy,
    Stream,
    Global,
}

impl AirbyteStateKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Stream => "stream",
            Self::Global => "global",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AirbyteOther {
    pub message_type: String,
    pub raw: Value,
}

pub fn parse_airbyte_ndjson(bytes: &[u8]) -> Result<Vec<AirbyteMessage>> {
    json_lines(bytes, "Airbyte")?
        .into_iter()
        .map(|(line, value)| parse_airbyte_message(line, value))
        .collect()
}

pub fn read_airbyte_ndjson_bytes(bytes: &[u8], options: &ReadOptions) -> Result<AirbyteRead> {
    let messages = parse_airbyte_ndjson(bytes)?;
    let records = messages.iter().filter_map(|message| match message {
        AirbyteMessage::Record(record) => Some((
            StreamIdentity::airbyte(record.namespace.clone(), record.stream.clone()),
            record.data.clone(),
        )),
        _ => None,
    });
    let streams = records_to_stream_reads(records, options)?;
    let catalogs = messages
        .iter()
        .filter_map(|message| match message {
            AirbyteMessage::Catalog(catalog) => Some(catalog.clone()),
            _ => None,
        })
        .collect();
    let states = messages
        .iter()
        .filter_map(|message| match message {
            AirbyteMessage::State(state) => Some(state),
            _ => None,
        })
        .map(|state| {
            Ok(ProtocolState {
                kind: state.kind.as_str().to_owned(),
                stream: state.stream.clone(),
                position: foreign_state("airbyte", &state.value)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(AirbyteRead {
        messages,
        catalogs,
        streams,
        states,
    })
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
        AirbyteStateKind::Stream => {
            if !state_object
                .get("stream")
                .map(Value::is_object)
                .unwrap_or(false)
            {
                return Err(malformed_field(
                    "Airbyte",
                    "STATE",
                    "state.stream",
                    line,
                    "object",
                ));
            }
        }
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

    Ok(AirbyteState {
        stream: stream_state_identity(state_object),
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

fn stream_state_identity(state: &serde_json::Map<String, Value>) -> Option<StreamIdentity> {
    let stream = state.get("stream")?.as_object()?;
    let descriptor = stream
        .get("stream_descriptor")
        .and_then(Value::as_object)
        .unwrap_or(stream);
    let name = descriptor.get("name")?.as_str()?.to_owned();
    let namespace = descriptor
        .get("namespace")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    Some(StreamIdentity::airbyte(namespace, name))
}
