use std::{
    collections::BTreeMap,
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use arrow_array::{
    Array, BinaryArray, Int32Array, Int64Array, ListArray, MapArray, StringArray, StructArray,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use cdf_kernel::{PartitionId, ResourceId};
use cdf_memory::{
    ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator,
    ReservationRequest, reserve,
};
use cdf_runtime::{
    BoundedFormatRequest, ByteSource, MemoryByteSource, ReadOptions, RunCancellation,
    SequentialReadRequest, decode_bounded_format,
};
use futures_executor::block_on;
use prost::Message;
use prost_reflect::DynamicMessage;
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FieldOptions, FileDescriptorProto, FileDescriptorSet, MessageOptions, OneofDescriptorProto,
    field_descriptor_proto::{Label, Type},
};

use super::*;

fn field(name: &str, number: i32, label: Label, field_type: Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_owned()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(field_type as i32),
        json_name: Some(name.to_owned()),
        ..Default::default()
    }
}

fn message_field(name: &str, number: i32, label: Label, type_name: &str) -> FieldDescriptorProto {
    FieldDescriptorProto {
        type_name: Some(type_name.to_owned()),
        ..field(name, number, label, Type::Message)
    }
}

fn descriptor_set() -> Vec<u8> {
    let child = DescriptorProto {
        name: Some("Child".to_owned()),
        field: vec![field("note", 1, Label::Optional, Type::String)],
        ..Default::default()
    };
    let map_entry = DescriptorProto {
        name: Some("AttrsEntry".to_owned()),
        field: vec![
            field("key", 1, Label::Optional, Type::String),
            field("value", 2, Label::Optional, Type::Int32),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut email = field("email", 4, Label::Optional, Type::String);
    email.oneof_index = Some(0);
    let mut phone = field("phone", 5, Label::Optional, Type::String);
    phone.oneof_index = Some(0);
    let mut status = field("status", 6, Label::Optional, Type::Enum);
    status.type_name = Some(".test.Status".to_owned());
    let mut samples = field("samples", 9, Label::Repeated, Type::Int32);
    samples.options = Some(FieldOptions {
        packed: Some(true),
        ..Default::default()
    });
    let row = DescriptorProto {
        name: Some("Row".to_owned()),
        field: vec![
            field("id", 1, Label::Optional, Type::Int64),
            field("name", 2, Label::Optional, Type::String),
            field("labels", 3, Label::Repeated, Type::String),
            email,
            phone,
            status,
            message_field("child", 7, Label::Optional, ".test.Child"),
            message_field("attrs", 8, Label::Repeated, ".test.Row.AttrsEntry"),
            samples,
            message_field(
                "created_at",
                10,
                Label::Optional,
                ".google.protobuf.Timestamp",
            ),
            message_field("parent", 11, Label::Optional, ".test.Row"),
        ],
        nested_type: vec![map_entry],
        oneof_decl: vec![OneofDescriptorProto {
            name: Some("contact".to_owned()),
            ..Default::default()
        }],
        ..Default::default()
    };
    let status_enum = EnumDescriptorProto {
        name: Some("Status".to_owned()),
        value: vec![
            EnumValueDescriptorProto {
                name: Some("STATUS_UNSPECIFIED".to_owned()),
                number: Some(0),
                ..Default::default()
            },
            EnumValueDescriptorProto {
                name: Some("ACTIVE".to_owned()),
                number: Some(1),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let timestamp = DescriptorProto {
        name: Some("Timestamp".to_owned()),
        field: vec![
            field("seconds", 1, Label::Optional, Type::Int64),
            field("nanos", 2, Label::Optional, Type::Int32),
        ],
        ..Default::default()
    };
    FileDescriptorSet {
        file: vec![
            FileDescriptorProto {
                name: Some("google/protobuf/timestamp.proto".to_owned()),
                package: Some("google.protobuf".to_owned()),
                message_type: vec![timestamp],
                syntax: Some("proto3".to_owned()),
                ..Default::default()
            },
            FileDescriptorProto {
                name: Some("test.proto".to_owned()),
                package: Some("test".to_owned()),
                dependency: vec!["google/protobuf/timestamp.proto".to_owned()],
                message_type: vec![child, row],
                enum_type: vec![status_enum],
                syntax: Some("proto3".to_owned()),
                ..Default::default()
            },
        ],
    }
    .encode_to_vec()
}

fn options() -> serde_json::Value {
    serde_json::json!({
        "descriptor_set_base64": BASE64_STANDARD.encode(descriptor_set()),
        "message": "test.Row",
        "framing": "length_delimited",
        "maximum_descriptor_bytes": 1024 * 1024,
        "maximum_message_bytes": 1024 * 1024,
        "maximum_output_batch_bytes": 64 * 1024 * 1024,
        "maximum_nesting_depth": 100
    })
}

fn evolved_options(field: FieldDescriptorProto, syntax: &str) -> serde_json::Value {
    let mut descriptors = FileDescriptorSet::decode(descriptor_set().as_slice()).unwrap();
    let file = descriptors
        .file
        .iter_mut()
        .find(|file| file.name.as_deref() == Some("test.proto"))
        .unwrap();
    file.syntax = Some(syntax.to_owned());
    file.message_type
        .iter_mut()
        .find(|message| message.name.as_deref() == Some("Row"))
        .unwrap()
        .field
        .push(field);
    let mut options = options();
    options["descriptor_set_base64"] =
        serde_json::Value::String(BASE64_STANDARD.encode(descriptors.encode_to_vec()));
    options
}

fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn varint_field(number: u32, value: u64, output: &mut Vec<u8>) {
    encode_varint(u64::from(number) << 3, output);
    encode_varint(value, output);
}

fn bytes_field(number: u32, value: &[u8], output: &mut Vec<u8>) {
    encode_varint((u64::from(number) << 3) | 2, output);
    encode_varint(value.len() as u64, output);
    output.extend_from_slice(value);
}

fn framed(message: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    encode_varint(message.len() as u64, &mut output);
    output.extend_from_slice(message);
    output
}

fn sample_message() -> Vec<u8> {
    let mut child = Vec::new();
    bytes_field(1, b"nested", &mut child);
    varint_field(77, 9, &mut child);

    let mut map = Vec::new();
    bytes_field(1, b"region", &mut map);
    bytes_field(2, b"wrong-wire", &mut map);
    varint_field(2, 7, &mut map);
    varint_field(9, 55, &mut map);
    let mut second_map = Vec::new();
    bytes_field(1, b"zone", &mut second_map);
    varint_field(2, 3, &mut second_map);
    let mut replacement_map = Vec::new();
    bytes_field(1, b"region", &mut replacement_map);
    varint_field(2, 8, &mut replacement_map);

    let mut timestamp = Vec::new();
    bytes_field(1, b"wrong-wire", &mut timestamp);
    varint_field(1, 1_700_000_000, &mut timestamp);
    varint_field(2, 123, &mut timestamp);
    varint_field(7, 22, &mut timestamp);

    let mut parent = Vec::new();
    varint_field(1, 5, &mut parent);

    let mut message = Vec::new();
    varint_field(1, 42, &mut message);
    bytes_field(2, b"alice", &mut message);
    bytes_field(3, b"red", &mut message);
    bytes_field(3, b"blue", &mut message);
    bytes_field(4, b"old@example.com", &mut message);
    bytes_field(5, b"555-0100", &mut message);
    varint_field(6, 1, &mut message);
    bytes_field(7, &child, &mut message);
    bytes_field(8, &map, &mut message);
    bytes_field(8, &second_map, &mut message);
    bytes_field(8, &replacement_map, &mut message);
    bytes_field(9, &[1, 2, 0x96, 0x01], &mut message);
    bytes_field(10, &timestamp, &mut message);
    bytes_field(11, &parent, &mut message);
    varint_field(99, 1234, &mut message);
    message
}

fn conformant_message() -> Vec<u8> {
    let mut child = Vec::new();
    bytes_field(1, b"nested", &mut child);
    let mut map = Vec::new();
    bytes_field(1, b"region", &mut map);
    varint_field(2, 7, &mut map);
    let mut timestamp = Vec::new();
    varint_field(1, 1_700_000_000, &mut timestamp);
    varint_field(2, 123, &mut timestamp);
    let mut parent = Vec::new();
    varint_field(1, 5, &mut parent);

    let mut message = Vec::new();
    varint_field(1, 42, &mut message);
    bytes_field(2, b"alice", &mut message);
    bytes_field(3, b"red", &mut message);
    bytes_field(3, b"blue", &mut message);
    bytes_field(5, b"555-0100", &mut message);
    varint_field(6, 1, &mut message);
    bytes_field(7, &child, &mut message);
    bytes_field(8, &map, &mut message);
    bytes_field(9, &[1, 2, 0x96, 0x01], &mut message);
    bytes_field(10, &timestamp, &mut message);
    bytes_field(11, &parent, &mut message);
    message
}

fn memory() -> Arc<dyn MemoryCoordinator> {
    Arc::new(DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new()).unwrap())
}

#[test]
fn descriptor_and_framing_are_mandatory_plan_authority() {
    let driver = ProtobufFormatDriver::new().unwrap();
    let error = driver
        .canonical_options(serde_json::json!({
            "message": "test.Row",
            "framing": "length_delimited"
        }))
        .unwrap_err();
    assert!(error.to_string().contains("descriptor_set_base64"));

    let mut missing_framing = options();
    missing_framing.as_object_mut().unwrap().remove("framing");
    assert!(
        driver
            .canonical_options(missing_framing)
            .unwrap_err()
            .to_string()
            .contains("framing")
    );
}

#[test]
fn descriptor_discovery_reads_zero_payload_bytes() {
    block_on(async {
        let memory = memory();
        let source = Arc::new(
            MemoryByteSource::from_bytes("memory:protobuf", vec![1], Arc::clone(&memory))
                .await
                .unwrap(),
        );
        let observation = ProtobufFormatDriver::new()
            .unwrap()
            .discover(
                source,
                FormatDiscoveryRequest {
                    options: options(),
                    discovery_kind: FormatDiscoveryKind::FormatMetadata,
                    maximum_bytes: 0,
                    maximum_records: 0,
                    memory,
                    cancellation: RunCancellation::default(),
                },
            )
            .await
            .unwrap();
        assert_eq!(observation.sampled_bytes, 0);
        assert_eq!(observation.sampled_records, 0);
        assert_eq!(
            observation.arrow_schema.metadata()["cdf:protobuf_message"],
            "test.Row"
        );
    });
}

#[test]
fn decodes_nested_repeated_map_oneof_wkt_recursive_and_unknown_provenance() {
    block_on(async {
        let memory = memory();
        let payload = framed(&sample_message());
        let source = Arc::new(
            MemoryByteSource::from_bytes("memory:protobuf", payload, Arc::clone(&memory))
                .await
                .unwrap(),
        );
        let read = decode_bounded_format(
            Arc::new(ProtobufFormatDriver::new().unwrap()),
            source,
            BoundedFormatRequest::new(
                ReadOptions::new(
                    ResourceId::new("test.rows").unwrap(),
                    PartitionId::new("p0").unwrap(),
                ),
                Arc::clone(&memory),
            )
            .with_options(options()),
        )
        .await
        .unwrap();
        assert_eq!(read.batches.len(), 1);
        let batch = &read.batches[0];
        let record = batch.record_batch().unwrap();
        assert_eq!(record.num_rows(), 1);
        assert_eq!(
            record
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            42
        );
        assert_eq!(
            record
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "alice"
        );

        let labels = record
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let label_values = labels.value(0);
        let label_values = label_values.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(label_values.value(0), "red");
        assert_eq!(label_values.value(1), "blue");

        assert!(record.column(3).is_null(0));
        assert_eq!(
            record
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "555-0100"
        );
        assert_eq!(
            record
                .column(5)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );
        assert!(record.schema().field(5).metadata()["cdf:protobuf_enum_values"].contains("ACTIVE"));

        let child = record
            .column(6)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            child
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "nested"
        );

        let attrs = record
            .column(7)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert_eq!(attrs.value_length(0), 2);
        assert_eq!(
            attrs
                .keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "region"
        );
        assert_eq!(
            attrs
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            8
        );
        assert_eq!(
            attrs
                .keys()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "zone"
        );
        assert_eq!(
            attrs
                .values()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(1),
            3
        );

        let samples = record
            .column(8)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let values = samples.value(0);
        let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values(), &[1, 2, 150]);

        let created_at = record
            .column(9)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            created_at
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1_700_000_000
        );
        assert_eq!(
            created_at
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            123
        );
        let parent = record
            .column(10)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(parent.value(0), &[0x08, 0x05]);
        assert_eq!(
            record.schema().field(10).metadata()["cdf:protobuf_message_encoding"],
            "wire"
        );

        let residuals = batch.header.residual_candidates();
        assert_eq!(residuals.len(), 6);
        assert!(
            residuals
                .iter()
                .any(|candidate| candidate.source_path() == ["child", "$protobuf_unknown", "77"])
        );
        assert!(
            residuals
                .iter()
                .any(|candidate| candidate.source_path() == ["$protobuf_unknown", "99"])
        );
        assert!(residuals.iter().any(|candidate| {
            candidate.source_path() == ["attrs", "$map_entry", "$protobuf_unknown", "2"]
        }));
        assert!(residuals.iter().any(|candidate| {
            candidate.source_path() == ["attrs", "$map_entry", "$protobuf_unknown", "9"]
        }));
        assert!(residuals.iter().any(|candidate| {
            candidate.source_path() == ["created_at", "$protobuf_unknown", "1"]
        }));
        assert!(residuals.iter().any(|candidate| {
            candidate.source_path() == ["created_at", "$protobuf_unknown", "7"]
        }));
        drop(read);
        assert_eq!(memory.snapshot().current_bytes, 0);
    });
}

#[test]
fn schema_evolution_is_hash_stable_and_changes_only_with_descriptor_authority() {
    let (_, baseline) = ProtobufOptions::parse(options()).unwrap();
    let future = field("future", 12, Label::Optional, Type::String);
    let evolved_options = evolved_options(future, "proto3");
    let (_, evolved) = ProtobufOptions::parse(evolved_options.clone()).unwrap();
    let (_, repeated) = ProtobufOptions::parse(evolved_options).unwrap();
    let baseline_hash =
        cdf_kernel::canonical_arrow_schema_hash(baseline.arrow_schema.as_ref()).unwrap();
    let evolved_hash =
        cdf_kernel::canonical_arrow_schema_hash(evolved.arrow_schema.as_ref()).unwrap();
    let repeated_hash =
        cdf_kernel::canonical_arrow_schema_hash(repeated.arrow_schema.as_ref()).unwrap();
    assert_ne!(baseline_hash, evolved_hash);
    assert_eq!(evolved_hash, repeated_hash);
}

#[test]
fn forged_well_known_type_layout_is_rejected_at_plan_time() {
    let mut descriptors = FileDescriptorSet::decode(descriptor_set().as_slice()).unwrap();
    let timestamp = descriptors
        .file
        .iter_mut()
        .find(|file| file.package.as_deref() == Some("google.protobuf"))
        .unwrap()
        .message_type
        .iter_mut()
        .find(|message| message.name.as_deref() == Some("Timestamp"))
        .unwrap();
    timestamp
        .field
        .iter_mut()
        .find(|field| field.name.as_deref() == Some("nanos"))
        .unwrap()
        .r#type = Some(Type::String as i32);
    let mut forged = options();
    forged["descriptor_set_base64"] =
        serde_json::Value::String(BASE64_STANDARD.encode(descriptors.encode_to_vec()));
    let error = ProtobufFormatDriver::new()
        .unwrap()
        .canonical_options(forged)
        .unwrap_err();
    assert!(error.to_string().contains("well-known type"));
    assert!(error.to_string().contains("google.protobuf.Timestamp"));
}

#[test]
fn decoder_microbatch_sizes_do_not_change_rows_or_unknown_ordinals() {
    block_on(async {
        async fn decode(batch_size: usize) -> (Vec<i64>, Vec<u64>) {
            let memory = memory();
            let mut payload = Vec::new();
            for _ in 0..5 {
                payload.extend_from_slice(&framed(&sample_message()));
            }
            let source = Arc::new(
                MemoryByteSource::from_bytes("memory:determinism", payload, Arc::clone(&memory))
                    .await
                    .unwrap(),
            );
            let read = decode_bounded_format(
                Arc::new(ProtobufFormatDriver::new().unwrap()),
                source,
                BoundedFormatRequest::new(
                    ReadOptions::new(
                        ResourceId::new("test.rows").unwrap(),
                        PartitionId::new("p0").unwrap(),
                    )
                    .with_batch_size(batch_size)
                    .unwrap(),
                    memory,
                )
                .with_options(options()),
            )
            .await
            .unwrap();
            let ids = read
                .batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .record_batch()
                        .unwrap()
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .map(Option::unwrap)
                })
                .collect();
            let mut ordinals = read
                .batches
                .iter()
                .flat_map(|batch| batch.header.residual_candidates())
                .map(|candidate| candidate.source_row_ordinal())
                .collect::<Vec<_>>();
            ordinals.sort_unstable();
            (ids, ordinals)
        }

        assert_eq!(decode(1).await, decode(64).await);
    });
}

#[test]
fn missing_proto2_required_field_fails_even_before_destination_admission() {
    block_on(async {
        let mut required = field("required_value", 12, Label::Required, Type::Int32);
        required.json_name = Some("requiredValue".to_owned());
        let memory = memory();
        let source = Arc::new(
            MemoryByteSource::from_bytes(
                "memory:required",
                framed(&sample_message()),
                Arc::clone(&memory),
            )
            .await
            .unwrap(),
        );
        let error = decode_bounded_format(
            Arc::new(ProtobufFormatDriver::new().unwrap()),
            source,
            BoundedFormatRequest::new(
                ReadOptions::new(
                    ResourceId::new("test.rows").unwrap(),
                    PartitionId::new("p0").unwrap(),
                ),
                memory,
            )
            .with_options(evolved_options(required, "proto2")),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("required field"));
        assert!(error.to_string().contains("required_value"));
    });
}

#[test]
fn length_prefix_and_payload_cross_arbitrary_chunk_boundaries() {
    block_on(async {
        let memory = memory();
        let payload = framed(&[0_u8; 300]);
        for chunk_bytes in 1..=17 {
            let source = MemoryByteSource::from_bytes(
                format!("memory:chunks-{chunk_bytes}"),
                payload.clone(),
                Arc::clone(&memory),
            )
            .await
            .unwrap();
            let stream = source
                .open_sequential(SequentialReadRequest {
                    preferred_chunk_bytes: chunk_bytes,
                    cancellation: RunCancellation::default(),
                })
                .await
                .unwrap();
            let mut cursor = AccountedByteCursor::new(stream);
            assert_eq!(read_length_prefix(&mut cursor).await.unwrap(), Some(300));
            assert_eq!(
                cursor
                    .read_exact(300, "chunked Protobuf message")
                    .await
                    .unwrap()
                    .len(),
                300
            );
            assert_eq!(read_length_prefix(&mut cursor).await.unwrap(), None);
        }
    });
}

#[test]
fn malformed_payload_fails_without_publishing_a_partial_window() {
    block_on(async {
        let memory = memory();
        let source = Arc::new(
            MemoryByteSource::from_bytes(
                "memory:malformed",
                framed(&[0x08, 0x80]),
                Arc::clone(&memory),
            )
            .await
            .unwrap(),
        );
        let error = decode_bounded_format(
            Arc::new(ProtobufFormatDriver::new().unwrap()),
            source,
            BoundedFormatRequest::new(
                ReadOptions::new(
                    ResourceId::new("test.rows").unwrap(),
                    PartitionId::new("p0").unwrap(),
                ),
                memory,
            )
            .with_options(options()),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("ended inside a varint"));
    });
}

#[test]
fn malformed_length_and_bounded_message_corpus_fail_closed() {
    block_on(async {
        let memory = memory();
        let source = Arc::new(
            MemoryByteSource::from_bytes(
                "memory:overflow-length",
                vec![0xff; 10],
                Arc::clone(&memory),
            )
            .await
            .unwrap(),
        );
        let error = decode_bounded_format(
            Arc::new(ProtobufFormatDriver::new().unwrap()),
            source,
            BoundedFormatRequest::new(
                ReadOptions::new(
                    ResourceId::new("test.rows").unwrap(),
                    PartitionId::new("p0").unwrap(),
                ),
                Arc::clone(&memory),
            )
            .with_options(options()),
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("length prefix exceeds ten bytes")
        );

        let mut bounded = options();
        bounded["maximum_message_bytes"] = serde_json::json!(8);
        bounded["maximum_output_batch_bytes"] = serde_json::json!(1024);
        let source = Arc::new(
            MemoryByteSource::from_bytes(
                "memory:oversized",
                framed(&[0_u8; 9]),
                Arc::clone(&memory),
            )
            .await
            .unwrap(),
        );
        let error = decode_bounded_format(
            Arc::new(ProtobufFormatDriver::new().unwrap()),
            source,
            BoundedFormatRequest::new(
                ReadOptions::new(
                    ResourceId::new("test.rows").unwrap(),
                    PartitionId::new("p0").unwrap(),
                ),
                memory,
            )
            .with_options(bounded),
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("above the configured 8-byte maximum")
        );
    });
}

#[test]
#[ignore = "same-host release-mode performance evidence"]
fn direct_arrow_decoder_meets_native_dynamic_reference_floor() {
    let (options, plan) = ProtobufOptions::parse(options()).unwrap();
    let descriptor = plan.descriptor.clone();
    let memory = memory();
    let encoded = conformant_message();
    let messages = block_on(async {
        let mut messages = Vec::with_capacity(4096);
        for _ in 0..4096 {
            let bytes = encoded.clone();
            let accounted = u64::try_from(bytes.capacity()).unwrap().max(1);
            let lease = reserve(
                Arc::clone(&memory),
                ReservationRequest::new(
                    ConsumerKey::new("protobuf-reference-benchmark", MemoryClass::Decode).unwrap(),
                    accounted,
                )
                .unwrap(),
            )
            .await
            .unwrap();
            messages.push(BufferedMessage {
                bytes,
                _lease: lease,
            });
        }
        messages
    });

    let measure = |mut operation: Box<dyn FnMut()>| -> Duration {
        let mut samples = Vec::with_capacity(7);
        operation();
        for _ in 0..7 {
            let started = Instant::now();
            operation();
            samples.push(started.elapsed());
        }
        samples.sort_unstable();
        samples[samples.len() / 2]
    };
    let reference = measure(Box::new(|| {
        for message in &messages {
            let decoded = DynamicMessage::decode(descriptor.clone(), message.bytes.as_slice())
                .expect("reference decoder");
            black_box(decoded);
        }
    }));
    let direct = measure(Box::new(|| {
        let decoded = build_record_batch(&plan, &plan, &messages, 0, options.maximum_nesting_depth)
            .expect("direct Arrow decoder");
        black_box(decoded);
    }));
    let ratio = reference.as_secs_f64() / direct.as_secs_f64();
    eprintln!(
        "protobuf direct Arrow median={direct:?}; prost-reflect median={reference:?}; throughput ratio={ratio:.3}x"
    );
    assert!(
        ratio >= 0.6,
        "direct Protobuf-to-Arrow throughput {ratio:.3}x is below the 0.6x native reference floor"
    );
}
