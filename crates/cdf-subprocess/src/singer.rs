use cdf_kernel::Result;
use cdf_runtime::ReadOptions;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::protocol::{
    ProtocolState, ProtocolStreamRead, StreamIdentity, foreign_state, json_lines, malformed_field,
    object_message, optional_array_strings, optional_string, records_to_stream_reads,
    required_array_strings, required_object, required_string,
};

#[derive(Clone, Debug)]
pub struct SingerRead {
    pub messages: Vec<SingerMessage>,
    pub schemas: Vec<SingerSchema>,
    pub streams: Vec<ProtocolStreamRead>,
    pub states: Vec<ProtocolState>,
}

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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SingerOther {
    pub message_type: String,
    pub raw: Value,
}

pub fn parse_singer_ndjson(bytes: &[u8]) -> Result<Vec<SingerMessage>> {
    json_lines(bytes, "Singer")?
        .into_iter()
        .map(|(line, value)| parse_singer_message(line, value))
        .collect()
}

pub fn read_singer_ndjson_bytes(bytes: &[u8], options: &ReadOptions) -> Result<SingerRead> {
    let messages = parse_singer_ndjson(bytes)?;
    let records = messages.iter().filter_map(|message| match message {
        SingerMessage::Record(record) => Some((
            StreamIdentity::singer(record.stream.clone()),
            record.record.clone(),
        )),
        _ => None,
    });
    let streams = records_to_stream_reads(records, options)?;
    let schemas = messages
        .iter()
        .filter_map(|message| match message {
            SingerMessage::Schema(schema) => Some(schema.clone()),
            _ => None,
        })
        .collect();
    let states = messages
        .iter()
        .filter_map(|message| match message {
            SingerMessage::State(state) => Some(state),
            _ => None,
        })
        .map(|state| {
            Ok(ProtocolState {
                kind: "state".to_owned(),
                stream: None,
                position: foreign_state("singer", &state.value)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(SingerRead {
        messages,
        schemas,
        streams,
        states,
    })
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
