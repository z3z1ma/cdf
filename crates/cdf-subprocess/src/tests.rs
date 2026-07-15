use super::*;

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Write,
    sync::Arc,
    time::Duration,
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{
    ErrorKind, ForeignState, PartitionId, ResourceId, ScopeKey, SegmentId, SourcePosition,
};
use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
use cdf_runtime::ReadOptions;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn read_options() -> ReadOptions {
    ReadOptions::new(
        ResourceId::new("orders").unwrap(),
        PartitionId::new("p0").unwrap(),
    )
}

fn memory() -> Arc<dyn MemoryCoordinator> {
    Arc::new(DeterministicMemoryCoordinator::new(256 * 1024 * 1024, BTreeMap::new()).unwrap())
}

fn execution() -> cdf_runtime::ExecutionServices {
    static SERVICES: std::sync::OnceLock<cdf_runtime::ExecutionServices> =
        std::sync::OnceLock::new();
    SERVICES
        .get_or_init(|| {
            cdf_runtime::ExecutionServices::new(Arc::new(TestIoHost::new().unwrap())).unwrap()
        })
        .clone()
}

struct TestIoHost {
    runtime: tokio::runtime::Runtime,
    memory: Arc<dyn MemoryCoordinator>,
    spill: Arc<dyn cdf_runtime::SpillBudgetCoordinator>,
}

impl TestIoHost {
    fn new() -> cdf_kernel::Result<Self> {
        Ok(Self {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|error| cdf_kernel::CdfError::internal(error.to_string()))?,
            memory: memory(),
            spill: Arc::new(cdf_runtime::FixedSpillBudget::new(256 * 1024 * 1024)?),
        })
    }
}

impl cdf_runtime::ExecutionHost for TestIoHost {
    fn capabilities(&self) -> cdf_runtime::ExecutionHostCapabilities {
        cdf_runtime::ExecutionHostCapabilities {
            logical_cpu_slots: 2,
            io_workers: 2,
            blocking_lanes: Vec::new(),
        }
    }

    fn memory(&self) -> Arc<dyn MemoryCoordinator> {
        Arc::clone(&self.memory)
    }

    fn spill(&self) -> Arc<dyn cdf_runtime::SpillBudgetCoordinator> {
        Arc::clone(&self.spill)
    }

    fn open_scope(
        &self,
        _run_id: &str,
    ) -> cdf_kernel::Result<Box<dyn cdf_runtime::ExecutionTaskScope>> {
        Err(cdf_kernel::CdfError::internal(
            "subprocess protocol test does not open task scopes",
        ))
    }

    fn run_io_blocking(
        &self,
        task: cdf_runtime::IoValueTask,
    ) -> cdf_kernel::Result<cdf_runtime::IoValue> {
        self.runtime.block_on(task)
    }

    fn delay(
        &self,
        duration: Duration,
        cancellation: cdf_runtime::RunCancellation,
    ) -> cdf_kernel::BoxFuture<'static, cdf_kernel::Result<()>> {
        Box::pin(async move {
            cancellation.check()?;
            tokio::time::sleep(duration).await;
            cancellation.check()
        })
    }

    fn monotonic_now(&self) -> Duration {
        Duration::ZERO
    }

    fn entropy_u64(&self) -> u64 {
        0
    }

    fn ensure_blocking_lanes(
        &self,
        _lanes: &[cdf_runtime::BlockingLaneSpec],
    ) -> cdf_kernel::Result<()> {
        Ok(())
    }

    fn run_blocking_value(
        &self,
        _lane: &str,
        task: cdf_runtime::BlockingValueTask,
    ) -> cdf_kernel::Result<cdf_runtime::IoValue> {
        task()
    }
}

fn shell(args: impl IntoIterator<Item = impl Into<String>>) -> CommandSpec {
    CommandSpec::new("/bin/sh").with_args(args)
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

fn foreign_state_blob(position: &SourcePosition) -> String {
    String::from_utf8(foreign_state(position).opaque_blob.clone()).unwrap()
}

fn expected_hash(value: &Value) -> String {
    let bytes = canonical_json_bytes(value);
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn canonical_json_bytes(value: &Value) -> Vec<u8> {
    let mut output = Vec::new();
    write_canonical_value(value, &mut output);
    output
}

fn write_canonical_value(value: &Value, output: &mut Vec<u8>) {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(number) => output.extend_from_slice(number.to_string().as_bytes()),
        Value::String(value) => {
            output.extend_from_slice(serde_json::to_string(value).unwrap().as_bytes())
        }
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_value(value, output);
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
                output.extend_from_slice(serde_json::to_string(key).unwrap().as_bytes());
                output.push(b':');
                write_canonical_value(value, output);
            }
            output.push(b'}');
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ndjson_stdout_adapter_captures_stderr_and_packages_output() {
    let temp = tempfile::tempdir().unwrap();
    let ndjson_path = temp.path().join("orders.ndjson");
    fs::write(&ndjson_path, "{\"id\":1,\"name\":\"ada\"}\n").unwrap();
    let command = shell([
        "-c",
        "printf 'fetch trace\\n' >&2; cat \"$1\"",
        "cdf-test",
        ndjson_path.to_str().unwrap(),
    ]);

    let output = run_stdout_adapter(
        &command,
        StdoutFormat::Ndjson,
        &read_options(),
        &SupervisionOptions::default(),
        memory(),
    )
    .await
    .unwrap();

    assert_eq!(output.stderr.lines, vec!["fetch trace"]);
    assert_eq!(output.read.batches[0].header.row_count, 1);

    let package =
        cdf_package::PackageBuilder::create(temp.path().join("package"), "pkg-subprocess").unwrap();
    let batches = output
        .read
        .batches
        .iter()
        .map(|batch| batch.record_batch().unwrap().clone())
        .collect::<Vec<_>>();
    package
        .write_segment(SegmentId::new("seg-subprocess").unwrap(), &batches)
        .unwrap();
    package.finish().unwrap();
    cdf_package::PackageReader::open(temp.path().join("package"))
        .unwrap()
        .verify()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn arrow_ipc_stdout_adapter_reads_kernel_batches() {
    let temp = tempfile::tempdir().unwrap();
    let ipc_path = temp.path().join("orders.arrow");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let name: ArrayRef = Arc::new(StringArray::from(vec![Some("ada"), Some("grace")]));
    let batch = RecordBatch::try_new(schema.clone(), vec![id, name]).unwrap();
    {
        let mut file = File::create(&ipc_path).unwrap();
        let mut writer = StreamWriter::try_new(&mut file, schema.as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        file.flush().unwrap();
    }
    let command = CommandSpec::new("cat").with_args([ipc_path.to_str().unwrap()]);

    let output = run_stdout_adapter(
        &command,
        StdoutFormat::ArrowIpc,
        &read_options(),
        &SupervisionOptions::default(),
        memory(),
    )
    .await
    .unwrap();

    assert_eq!(output.read.batches.len(), 1);
    assert_eq!(
        output.read.batches[0]
            .record_batch()
            .unwrap()
            .schema()
            .as_ref(),
        schema.as_ref()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn nonzero_exit_maps_to_transient_with_stderr() {
    let command = shell(["-c", "printf 'adapter failed\\n' >&2; exit 7"]);
    let error = run_stdout_adapter(
        &command,
        StdoutFormat::Ndjson,
        &read_options(),
        &SupervisionOptions::default(),
        memory(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Transient);
    assert!(error.message.contains("exit code 7"));
    assert!(error.message.contains("adapter failed"));
}

#[tokio::test(flavor = "current_thread")]
async fn timeout_maps_to_transient() {
    let command = shell(["-c", "sleep 2"]);
    let error = run_stdout_adapter(
        &command,
        StdoutFormat::Ndjson,
        &read_options(),
        &SupervisionOptions {
            timeout: Some(Duration::from_millis(10)),
            stderr_line_limit: DEFAULT_STDERR_LINE_LIMIT,
        },
        memory(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Transient);
    assert!(error.message.contains("timed out"));
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_stdout_maps_to_data_with_stderr_context() {
    let command = shell(["-c", "printf 'parser warning\\n' >&2; printf '{bad\\n'"]);
    let error = run_stdout_adapter(
        &command,
        StdoutFormat::Ndjson,
        &read_options(),
        &SupervisionOptions::default(),
        memory(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Data);
    assert!(error.message.contains("parser warning"));
    assert!(error.message.contains("malformed Ndjson"));
}

#[test]
fn singer_protocol_parses_schema_record_state_and_batches_by_stream() {
    let state_value = json!({
        "bookmarks": {
            "orders": {
                "replication_key_value": "2026-07-06T00:00:00Z"
            }
        }
    });
    let bytes = ndjson(&[
        json!({
            "type": "schema",
            "stream": "orders",
            "schema": {
                "type": "object",
                "properties": {
                    "id": { "type": "integer" },
                    "status": { "type": "string" }
                }
            },
            "key_properties": ["id"],
            "bookmark_properties": ["updated_at"],
            "tap_metadata": { "unknown": true }
        }),
        json!({
            "type": "ReCoRd",
            "stream": "orders",
            "record": { "id": 1, "status": "open" },
            "time_extracted": "2026-07-06T00:00:01Z",
            "extra": { "retained": true }
        }),
        json!({
            "type": "STATE",
            "value": state_value
        }),
    ]);

    let read = read_singer_ndjson_bytes(&bytes, &read_options(), &execution()).unwrap();

    assert_eq!(read.schemas.len(), 1);
    assert_eq!(read.schemas[0].raw["tap_metadata"]["unknown"], true);
    assert_eq!(read.schemas[0].key_properties, vec!["id"]);
    assert_eq!(read.schemas[0].bookmark_properties, vec!["updated_at"]);
    assert_eq!(read.streams.len(), 1);
    assert_eq!(read.streams[0].stream, StreamIdentity::singer("orders"));
    assert_eq!(
        read.streams[0].read.batches[0].header.batch_id.as_str(),
        "orders-p0-orders-u00000000-b00000000"
    );
    assert_eq!(read.streams[0].read.batches[0].header.row_count, 1);
    match &read.streams[0].read.descriptor.state_scope {
        ScopeKey::Stream { name } => assert_eq!(name, "orders"),
        other => panic!("expected stream scope, got {other:?}"),
    }

    assert_eq!(read.states.len(), 1);
    let state = foreign_state(&read.states[0].position);
    assert_eq!(state.protocol, "singer");
    assert_eq!(
        serde_json::from_slice::<Value>(&state.opaque_blob).unwrap(),
        state_value
    );
    assert_eq!(state.blob_sha256, expected_hash(&state_value));
}

#[test]
fn airbyte_protocol_parses_catalog_record_and_state_variants() {
    let legacy_state = json!({
        "type": "LEGACY",
        "data": { "cursor": "old" }
    });
    let stream_state = json!({
        "type": "STREAM",
        "stream": {
            "stream_descriptor": {
                "namespace": "crm data",
                "name": "users/new_v2"
            },
            "stream_state": { "cursor": 7 }
        }
    });
    let global_state = json!({
        "type": "GLOBAL",
        "global": {
            "shared_state": { "sync_id": "abc" },
            "stream_states": []
        }
    });
    let bytes = ndjson(&[
        json!({
            "type": "CATALOG",
            "catalog": {
                "streams": [{
                    "name": "users/new_v2",
                    "namespace": "crm data",
                    "json_schema": {
                        "type": "object",
                        "properties": {
                            "id": { "type": "integer" },
                            "email": { "type": "string" }
                        }
                    },
                    "supported_sync_modes": ["full_refresh", "incremental"]
                }]
            },
            "future_field": { "retained": true }
        }),
        json!({
            "type": "RECORD",
            "record": {
                "namespace": "crm data",
                "stream": "users/new_v2",
                "data": { "id": 1, "email": "ada@example.com" },
                "emitted_at": 1783296000000u64,
                "unknown": "ok"
            }
        }),
        json!({ "type": "STATE", "state": legacy_state }),
        json!({ "type": "STATE", "state": stream_state }),
        json!({ "type": "STATE", "state": global_state }),
    ]);

    let read = read_airbyte_ndjson_bytes(&bytes, &read_options(), &execution()).unwrap();

    assert_eq!(read.catalogs.len(), 1);
    assert_eq!(read.catalogs[0].raw["future_field"]["retained"], true);
    assert_eq!(read.streams.len(), 1);
    let users = StreamIdentity::airbyte(Some("crm data".to_owned()), "users/new_v2");
    assert_eq!(read.streams[0].stream, users);
    match &read.streams[0].read.descriptor.state_scope {
        ScopeKey::Stream { name } => assert_eq!(name, "crm data.users/new_v2"),
        other => panic!("expected stream scope, got {other:?}"),
    }
    assert_eq!(
        read.streams[0].read.batches[0].header.batch_id.as_str(),
        "orders-p0-crm-data-users-new_v2-u00000000-b00000000"
    );
    assert_eq!(read.streams[0].read.batches[0].header.row_count, 1);

    assert_eq!(
        read.states
            .iter()
            .map(|state| state.kind.as_str())
            .collect::<Vec<_>>(),
        vec!["legacy", "stream", "global"]
    );
    assert_eq!(read.states[1].stream, Some(users));
    for (state, expected) in read
        .states
        .iter()
        .zip([legacy_state, stream_state, global_state])
    {
        let position = foreign_state(&state.position);
        assert_eq!(position.protocol, "airbyte");
        assert_eq!(
            serde_json::from_slice::<Value>(&position.opaque_blob).unwrap(),
            expected
        );
        assert_eq!(position.blob_sha256, expected_hash(&expected));
    }
}

#[test]
fn malformed_protocol_messages_are_data_errors_without_raw_state() {
    let singer =
        parse_singer_ndjson(br#"{"type":"STATE","secret":"do-not-leak","token":"super-secret"}"#)
            .unwrap_err();
    assert_eq!(singer.kind, ErrorKind::Data);
    assert!(singer.message.contains("Singer STATE"));
    assert!(!singer.message.contains("super-secret"));

    let airbyte = parse_airbyte_ndjson(
        br#"{"type":"STATE","state":{"type":"STREAM","token":"super-secret"}}"#,
    )
    .unwrap_err();
    assert_eq!(airbyte.kind, ErrorKind::Data);
    assert!(airbyte.message.contains("Airbyte STATE"));
    assert!(!airbyte.message.contains("super-secret"));

    let malformed_record =
        parse_airbyte_ndjson(br#"{"type":"RECORD","record":{"stream":"users","data":{"id":1}}}"#)
            .unwrap_err();
    assert_eq!(malformed_record.kind, ErrorKind::Data);
    assert!(malformed_record.message.contains("emitted_at"));
}

#[test]
fn protocol_parsers_validate_required_field_shapes_and_line_numbers() {
    let singer_blank_stream =
        parse_singer_ndjson(br#"{"type":"RECORD","stream":"  ","record":{"id":1}}"#).unwrap_err();
    assert_eq!(singer_blank_stream.kind, ErrorKind::Data);
    assert!(singer_blank_stream.message.contains("stream"));

    let singer_record_object =
        parse_singer_ndjson(br#"{"type":"RECORD","stream":"orders","record":[]}"#).unwrap_err();
    assert_eq!(singer_record_object.kind, ErrorKind::Data);
    assert!(singer_record_object.message.contains("record"));

    let singer_schema_object = parse_singer_ndjson(
        br#"{"type":"SCHEMA","stream":"orders","schema":"bad","key_properties":["id"]}"#,
    )
    .unwrap_err();
    assert_eq!(singer_schema_object.kind, ErrorKind::Data);
    assert!(singer_schema_object.message.contains("schema"));

    let singer_key_properties = parse_singer_ndjson(
        br#"{"type":"SCHEMA","stream":"orders","schema":{},"key_properties":[1]}"#,
    )
    .unwrap_err();
    assert_eq!(singer_key_properties.kind, ErrorKind::Data);
    assert!(singer_key_properties.message.contains("key_properties"));

    let singer_bookmark_properties = parse_singer_ndjson(
        br#"{"type":"SCHEMA","stream":"orders","schema":{},"key_properties":["id"],"bookmark_properties":[1]}"#,
    )
    .unwrap_err();
    assert_eq!(singer_bookmark_properties.kind, ErrorKind::Data);
    assert!(
        singer_bookmark_properties
            .message
            .contains("bookmark_properties")
    );

    let line_number =
        parse_singer_ndjson(b"\n{\"type\":\"RECORD\",\"stream\":\"orders\",\"record\":[]}\n")
            .unwrap_err();
    assert!(line_number.message.contains("line 2"));

    let airbyte_catalog = parse_airbyte_ndjson(br#"{"type":"CATALOG","catalog":[]}"#).unwrap_err();
    assert_eq!(airbyte_catalog.kind, ErrorKind::Data);
    assert!(airbyte_catalog.message.contains("catalog"));

    let airbyte_decimal_emitted_at = parse_airbyte_ndjson(
        br#"{"type":"RECORD","record":{"stream":"users","data":{},"emitted_at":1.25}}"#,
    )
    .unwrap_err();
    assert_eq!(airbyte_decimal_emitted_at.kind, ErrorKind::Data);
    assert!(airbyte_decimal_emitted_at.message.contains("emitted_at"));

    let airbyte_u64_emitted_at = parse_airbyte_ndjson(
        br#"{"type":"RECORD","record":{"stream":"users","data":{},"emitted_at":18446744073709551615}}"#,
    )
    .unwrap();
    assert!(matches!(
        airbyte_u64_emitted_at[0],
        AirbyteMessage::Record(_)
    ));
}

#[test]
fn airbyte_legacy_state_distinguishes_explicit_and_implicit_forms() {
    let implicit = parse_airbyte_ndjson(br#"{"type":"STATE","state":{"cursor":"old"}}"#).unwrap();
    assert!(matches!(
        implicit[0],
        AirbyteMessage::State(AirbyteState {
            kind: AirbyteStateKind::Legacy,
            ..
        })
    ));

    let explicit_missing_data =
        parse_airbyte_ndjson(br#"{"type":"STATE","state":{"type":"LEGACY"}}"#).unwrap_err();
    assert_eq!(explicit_missing_data.kind, ErrorKind::Data);
    assert!(explicit_missing_data.message.contains("state.data"));
}

#[test]
fn protocol_state_hashes_are_deterministic() {
    let singer_state = json!({
        "z": 1,
        "a": [true, false],
        "m": {
            "b": 2,
            "a": 3
        }
    });
    let singer_bytes = ndjson(&[json!({
        "type": "STATE",
        "value": singer_state
    })]);
    let first = read_singer_ndjson_bytes(&singer_bytes, &read_options(), &execution()).unwrap();
    let second = read_singer_ndjson_bytes(&singer_bytes, &read_options(), &execution()).unwrap();
    let first_state = foreign_state(&first.states[0].position);
    let second_state = foreign_state(&second.states[0].position);
    assert_eq!(first_state.blob_sha256, second_state.blob_sha256);
    assert_eq!(first_state.blob_sha256, expected_hash(&singer_state));
    assert_eq!(
        foreign_state_blob(&first.states[0].position),
        r#"{"a":[true,false],"m":{"a":3,"b":2},"z":1}"#
    );

    let reordered_singer_bytes =
        br#"{"type":"STATE","value":{"m":{"b":2,"a":3},"z":1,"a":[true,false]}}"#;
    let reordered =
        read_singer_ndjson_bytes(reordered_singer_bytes, &read_options(), &execution()).unwrap();
    assert_eq!(
        foreign_state(&reordered.states[0].position).blob_sha256,
        first_state.blob_sha256
    );

    let airbyte_state = json!({
        "type": "GLOBAL",
        "global": {
            "shared_state": { "cursor": "z" },
            "stream_states": []
        }
    });
    let airbyte_bytes = ndjson(&[json!({
        "type": "STATE",
        "state": airbyte_state
    })]);
    let first = read_airbyte_ndjson_bytes(&airbyte_bytes, &read_options(), &execution()).unwrap();
    let second = read_airbyte_ndjson_bytes(&airbyte_bytes, &read_options(), &execution()).unwrap();
    let first_state = foreign_state(&first.states[0].position);
    let second_state = foreign_state(&second.states[0].position);
    assert_eq!(first_state.blob_sha256, second_state.blob_sha256);
    assert_eq!(first_state.blob_sha256, expected_hash(&airbyte_state));
}

#[test]
fn protocol_batches_write_to_and_replay_from_package() {
    let temp = tempfile::tempdir().unwrap();
    let package_dir = temp.path().join("protocol-package");
    let bytes = ndjson(&[
        json!({
            "type": "RECORD",
            "record": {
                "stream": "users",
                "data": { "id": 1, "email": "ada@example.com" },
                "emitted_at": 1783296000000u64
            }
        }),
        json!({
            "type": "RECORD",
            "record": {
                "stream": "users",
                "data": { "id": 2, "email": "grace@example.com" },
                "emitted_at": 1783296001000u64
            }
        }),
    ]);
    let read = read_airbyte_ndjson_bytes(&bytes, &read_options(), &execution()).unwrap();

    let package = cdf_package::PackageBuilder::create(&package_dir, "pkg-protocol").unwrap();
    for (index, stream) in read.streams.iter().enumerate() {
        let batches = stream
            .read
            .batches
            .iter()
            .map(|batch| batch.record_batch().unwrap().clone())
            .collect::<Vec<_>>();
        package
            .write_segment(
                SegmentId::new(format!("seg-protocol-{index}")).unwrap(),
                &batches,
            )
            .unwrap();
    }
    package.finish().unwrap();

    let reader = cdf_package::PackageReader::open(&package_dir).unwrap();
    reader.verify().unwrap();
    let replay = reader.replay_view().unwrap();
    assert_eq!(replay.segments.len(), 1);
    let replayed = reader
        .read_segment(&SegmentId::new("seg-protocol-0").unwrap())
        .unwrap();
    assert_eq!(replayed.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
}
