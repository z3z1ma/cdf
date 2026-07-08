use std::panic;

use cdf_formats::ReadOptions;
use cdf_kernel::{ErrorKind, ForeignState, PartitionId, ResourceId, SourcePosition};
use cdf_subprocess::{
    AirbyteMessage, AirbyteStateKind, SingerMessage, StreamIdentity, parse_airbyte_ndjson,
    parse_singer_ndjson, read_airbyte_ndjson_bytes, read_singer_ndjson_bytes,
};
use proptest::prelude::*;
use serde_json::{Value, json};

fn read_options() -> ReadOptions {
    ReadOptions::new(
        ResourceId::new("property_fuzz_protocol").unwrap(),
        PartitionId::new("p0").unwrap(),
    )
    .with_batch_size(8)
    .unwrap()
}

fn ndjson(values: &[Value]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in values {
        serde_json::to_writer(&mut bytes, value).unwrap();
        bytes.push(b'\n');
    }
    bytes
}

fn foreign_state(position: &SourcePosition) -> &ForeignState {
    match position {
        SourcePosition::ForeignState(state) => state,
        other => panic!("expected foreign state, got {other:?}"),
    }
}

fn assert_data_error(error: cdf_kernel::CdfError) {
    assert_eq!(error.kind, ErrorKind::Data);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    #[test]
    fn property_fuzz_protocol_parsers_never_panic_on_adversarial_bytes(
        bytes in prop::collection::vec(any::<u8>(), 0..=1024)
    ) {
        prop_assert!(panic::catch_unwind(|| parse_singer_ndjson(&bytes)).is_ok());
        prop_assert!(panic::catch_unwind(|| parse_airbyte_ndjson(&bytes)).is_ok());
        prop_assert!(
            panic::catch_unwind(|| read_singer_ndjson_bytes(&bytes, &read_options())).is_ok()
        );
        prop_assert!(
            panic::catch_unwind(|| read_airbyte_ndjson_bytes(&bytes, &read_options())).is_ok()
        );
    }
}

#[test]
fn property_fuzz_singer_unknown_messages_and_fields_follow_current_contract() {
    let bytes = ndjson(&[
        json!({
            "type": "TRACE",
            "level": "info",
            "payload": { "retained": true }
        }),
        json!({
            "type": "SCHEMA",
            "stream": "orders",
            "schema": { "type": "object" },
            "key_properties": ["id"],
            "bookmark_properties": null,
            "extra_schema_field": { "retained": true }
        }),
        json!({
            "type": "RECORD",
            "stream": "orders",
            "record": { "id": 1 },
            "extra_record_field": { "retained": true }
        }),
    ]);

    let messages = parse_singer_ndjson(&bytes).unwrap();
    assert!(matches!(
        &messages[0],
        SingerMessage::Other(other)
            if other.message_type == "TRACE" && other.raw["payload"]["retained"] == true
    ));
    assert!(matches!(
        &messages[1],
        SingerMessage::Schema(schema)
            if schema.raw["extra_schema_field"]["retained"] == true
                && schema.bookmark_properties.is_empty()
    ));
    assert!(matches!(
        &messages[2],
        SingerMessage::Record(record)
            if record.raw["extra_record_field"]["retained"] == true
    ));

    let read = read_singer_ndjson_bytes(&bytes, &read_options()).unwrap();
    assert_eq!(read.messages.len(), 3);
    assert_eq!(read.schemas.len(), 1);
    assert_eq!(read.streams.len(), 1);
    assert!(read.states.is_empty());
}

#[test]
fn property_fuzz_singer_malformed_and_truncated_inputs_error() {
    for bytes in [
        b"42\n".as_slice(),
        b"{\"stream\":\"orders\"}\n",
        b"{\"type\":\"SCHEMA\",\"stream\":\"orders\",\"schema\":{},\"key_properties\":[1]}\n",
        b"{\"type\":\"RECORD\",\"stream\":\"orders\",\"record\":[]}\n",
        b"{\"type\":\"STATE\"}\n",
        b"{\"type\":\"RECORD\",\"stream\":\"orders\",\"record\":{\"id\":1}\n",
    ] {
        assert_data_error(parse_singer_ndjson(bytes).unwrap_err());
        assert_data_error(read_singer_ndjson_bytes(bytes, &read_options()).unwrap_err());
    }
}

#[test]
fn property_fuzz_singer_foreign_state_payloads_round_trip() {
    let state_value = json!({
        "z": 1,
        "a": [true, null, { "nested": "yes" }],
        "cursor": "2026-07-08T00:00:00Z"
    });
    let bytes = ndjson(&[json!({
        "type": "STATE",
        "value": state_value
    })]);

    let read = read_singer_ndjson_bytes(&bytes, &read_options()).unwrap();

    assert_eq!(read.states.len(), 1);
    let state = foreign_state(&read.states[0].position);
    assert_eq!(state.version, cdf_kernel::CHECKPOINT_STATE_VERSION);
    assert_eq!(state.protocol, "singer");
    assert_eq!(
        serde_json::from_slice::<Value>(&state.opaque_blob).unwrap(),
        state_value
    );
    assert!(state.blob_sha256.starts_with("sha256:"));
}

#[test]
fn property_fuzz_airbyte_unknown_messages_and_fields_follow_current_contract() {
    let bytes = ndjson(&[
        json!({
            "type": "CONTROL",
            "payload": { "retained": true }
        }),
        json!({
            "type": "RECORD",
            "record": {
                "namespace": "crm",
                "stream": "users",
                "data": { "id": 1 },
                "emitted_at": 1783468800000u64,
                "extra_record_field": { "retained": true }
            },
            "extra_top_field": { "retained": true }
        }),
    ]);

    let messages = parse_airbyte_ndjson(&bytes).unwrap();
    assert!(matches!(
        &messages[0],
        AirbyteMessage::Other(other)
            if other.message_type == "CONTROL" && other.raw["payload"]["retained"] == true
    ));
    assert!(matches!(
        &messages[1],
        AirbyteMessage::Record(record)
            if record.raw["extra_top_field"]["retained"] == true
                && record.raw["record"]["extra_record_field"]["retained"] == true
    ));

    let read = read_airbyte_ndjson_bytes(&bytes, &read_options()).unwrap();
    assert_eq!(read.messages.len(), 2);
    assert_eq!(read.streams.len(), 1);
    assert!(read.catalogs.is_empty());
    assert!(read.states.is_empty());
}

#[test]
fn property_fuzz_airbyte_malformed_and_truncated_inputs_error() {
    for bytes in [
        b"null\n".as_slice(),
        b"{\"record\":{\"stream\":\"users\",\"data\":{},\"emitted_at\":1}}\n",
        b"{\"type\":\"RECORD\",\"record\":{\"stream\":\"users\",\"data\":{}}}\n",
        b"{\"type\":\"RECORD\",\"record\":{\"stream\":\"users\",\"data\":[],\"emitted_at\":1}}\n",
        b"{\"type\":\"STATE\",\"state\":{\"type\":\"STREAM\",\"data\":{}}}\n",
        b"{\"type\":\"STATE\",\"state\":{\"type\":\"UNKNOWN\",\"data\":{}}}\n",
        b"{\"type\":\"RECORD\",\"record\":{\"stream\":\"users\",\"data\":{\"id\":1},\"emitted_at\":",
    ] {
        assert_data_error(parse_airbyte_ndjson(bytes).unwrap_err());
        assert_data_error(read_airbyte_ndjson_bytes(bytes, &read_options()).unwrap_err());
    }
}

#[test]
fn property_fuzz_airbyte_foreign_state_payloads_round_trip() {
    let legacy_state = json!({
        "type": "LEGACY",
        "data": { "cursor": "old", "nested": [1, 2] }
    });
    let stream_state = json!({
        "type": "STREAM",
        "stream": {
            "stream_descriptor": {
                "namespace": "crm",
                "name": "users"
            },
            "stream_state": { "cursor": 7 }
        }
    });
    let bytes = ndjson(&[
        json!({ "type": "STATE", "state": legacy_state }),
        json!({ "type": "STATE", "state": stream_state }),
    ]);

    let read = read_airbyte_ndjson_bytes(&bytes, &read_options()).unwrap();

    assert_eq!(read.states.len(), 2);
    assert_eq!(read.states[0].kind, "legacy");
    assert_eq!(read.states[0].stream, None);
    assert_eq!(read.states[1].kind, "stream");
    assert_eq!(
        read.states[1].stream,
        Some(StreamIdentity::airbyte(Some("crm".to_owned()), "users"))
    );
    assert!(matches!(
        read.messages[0],
        AirbyteMessage::State(cdf_subprocess::AirbyteState {
            kind: AirbyteStateKind::Legacy,
            ..
        })
    ));

    for (state, expected) in read.states.iter().zip([legacy_state, stream_state]) {
        let position = foreign_state(&state.position);
        assert_eq!(position.version, cdf_kernel::CHECKPOINT_STATE_VERSION);
        assert_eq!(position.protocol, "airbyte");
        assert_eq!(
            serde_json::from_slice::<Value>(&position.opaque_blob).unwrap(),
            expected
        );
        assert!(position.blob_sha256.starts_with("sha256:"));
    }
}
