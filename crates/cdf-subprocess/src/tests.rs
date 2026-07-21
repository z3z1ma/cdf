use super::*;

use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{BufWriter, Write},
    sync::Arc,
    time::Duration,
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use cdf_foreign_stream::{
    ForeignControlKind, ForeignProducer, ForeignStreamEvent, ForeignStreamOpenRequest,
    ForeignTerminalStatus, ForeignTransferMode,
};
use cdf_kernel::{ErrorKind, ForeignState, PartitionId, ResourceId, SegmentId, SourcePosition};
use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};
use cdf_runtime::{DecodeSchemaPlan, ReadOptions};
use futures_util::StreamExt;
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

fn shell(args: impl IntoIterator<Item = impl Into<String>>) -> CommandSpec {
    CommandSpec::new("/bin/sh").with_args(args)
}

async fn collect_subprocess_events(producer: SubprocessProducer) -> Vec<ForeignStreamEvent> {
    let mut opened = producer
        .open(ForeignStreamOpenRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            partition_id: PartitionId::new("p0").unwrap(),
            cancellation: Default::default(),
        })
        .await
        .unwrap();
    let mut events = Vec::new();
    while let Some(event) = opened.events.next().await {
        events.push(event.unwrap());
    }
    opened.termination.join().await.unwrap();
    events
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

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let events = collect_subprocess_events(
        SubprocessProducer::new(
            command,
            SubprocessProtocol::Ndjson,
            read_options(),
            DecodeSchemaPlan::fixed_admission(schema),
            SupervisionOptions::default(),
            memory(),
        )
        .unwrap(),
    )
    .await;
    let mut batches = Vec::new();
    let mut diagnostics = Vec::new();
    let mut terminal = None;
    for event in events {
        match event {
            ForeignStreamEvent::Outcome(outcome) => batches.push(outcome.batch),
            ForeignStreamEvent::Control(control) => {
                if let ForeignControlKind::Diagnostic { message, .. } = control.kind {
                    diagnostics.push(message);
                }
            }
            ForeignStreamEvent::Terminal(status) => terminal = Some(status),
        }
    }
    assert_eq!(diagnostics, vec!["fetch trace"]);
    assert!(matches!(
        terminal,
        Some(ForeignTerminalStatus::Succeeded { .. })
    ));
    assert_eq!(batches[0].header.row_count, 1);

    let package = cdf_package::PackageBuilder::create(
        temp.path().join("package"),
        "pkg-subprocess",
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    let batches = batches
        .iter()
        .map(|batch| batch.record_batch().unwrap().clone())
        .collect::<Vec<_>>();
    let batches = cdf_package_contract::append_package_row_ord(batches, 0).unwrap();
    package
        .write_segment(SegmentId::new("seg-subprocess").unwrap(), 0, &batches)
        .unwrap();
    package.finish().unwrap();
    cdf_package::PackageReader::open(temp.path().join("package"))
        .unwrap()
        .verify()
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn arrow_ipc_stdout_adapter_streams_unknown_length_without_executor_deadlock() {
    let temp = tempfile::tempdir().unwrap();
    let ipc_path = temp.path().join("orders-stream.arrow");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("ada"), Some("grace")])),
        ],
    )
    .unwrap();
    {
        let mut file = File::create(&ipc_path).unwrap();
        let mut writer = StreamWriter::try_new(&mut file, schema.as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        file.flush().unwrap();
    }
    let command = CommandSpec::new("cat").with_args([ipc_path.to_str().unwrap()]);

    let producer = SubprocessProducer::new(
        command,
        SubprocessProtocol::ArrowIpc,
        read_options(),
        DecodeSchemaPlan::fixed_admission(Arc::clone(&schema)),
        SupervisionOptions {
            maximum_stdout_bytes: 1024 * 1024,
            ..SupervisionOptions::default()
        },
        memory(),
    )
    .unwrap();
    let mut opened = producer
        .open(ForeignStreamOpenRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            partition_id: PartitionId::new("p0").unwrap(),
            cancellation: Default::default(),
        })
        .await
        .unwrap();
    let mut batches = Vec::new();
    let mut terminal = None;
    while let Some(event) = opened.events.next().await {
        match event.unwrap() {
            ForeignStreamEvent::Outcome(outcome) => batches.push(outcome),
            ForeignStreamEvent::Control(_) => {}
            ForeignStreamEvent::Terminal(status) => terminal = Some(status),
        }
    }
    opened.termination.join().await.unwrap();

    assert!(matches!(
        terminal,
        Some(ForeignTerminalStatus::Succeeded { .. })
    ));
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].transfer_mode,
        ForeignTransferMode::ArrowIpcStream
    );
    assert_eq!(batches[0].batch.header.row_count, 2);
    assert_eq!(
        batches[0].batch.record_batch().unwrap().schema().as_ref(),
        schema.as_ref()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ndjson_stdout_adapter_streams_with_compiled_schema_without_reserving_stdout_ceiling() {
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(96 * 1024 * 1024, BTreeMap::new()).unwrap());
    let constrained: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let command = shell([
        "-c",
        "printf 'stream trace\\n' >&2; i=0; while [ \"$i\" -lt 4096 ]; do printf '{\"id\":%s,\"name\":\"ada\"}\\n' \"$i\"; i=$((i + 1)); done",
    ]);

    let producer = SubprocessProducer::new(
        command,
        SubprocessProtocol::Ndjson,
        read_options(),
        DecodeSchemaPlan::fixed_admission(schema),
        SupervisionOptions {
            maximum_stdout_bytes: 8,
            maximum_stream_chunk_bytes: 4 * 1024,
            maximum_stderr_bytes: 64 * 1024,
            ..SupervisionOptions::default()
        },
        constrained,
    )
    .unwrap();
    let mut opened = producer
        .open(ForeignStreamOpenRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            partition_id: PartitionId::new("p0").unwrap(),
            cancellation: Default::default(),
        })
        .await
        .unwrap();
    let mut batches = Vec::new();
    let mut diagnostics = Vec::new();
    let mut terminal = None;
    while let Some(event) = opened.events.next().await {
        match event.unwrap() {
            ForeignStreamEvent::Outcome(outcome) => batches.push(outcome.batch),
            ForeignStreamEvent::Control(control) => {
                if let ForeignControlKind::Diagnostic { message, .. } = control.kind {
                    diagnostics.push(message);
                }
            }
            ForeignStreamEvent::Terminal(status) => terminal = Some(status),
        }
    }
    opened.termination.join().await.unwrap();

    assert!(matches!(
        terminal,
        Some(ForeignTerminalStatus::Succeeded { .. })
    ));
    assert_eq!(diagnostics, vec!["stream trace"]);
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        4096
    );
    drop(batches);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn successful_parent_reaps_background_descendant_before_inherited_pipe_eof() {
    let output = tokio::time::timeout(
        Duration::from_secs(2),
        run_bounded_command(
            shell(["-c", "sleep 30 & printf ok"]),
            SupervisionOptions {
                maximum_stdout_bytes: 8,
                termination_grace: Duration::from_millis(50),
                ..SupervisionOptions::default()
            },
            cdf_runtime::RunCancellation::default(),
            memory(),
        ),
    )
    .await
    .expect("background descendant held inherited pipes open")
    .unwrap();

    assert_eq!(output.stdout.as_bytes(), b"ok");
}

#[tokio::test(flavor = "current_thread")]
async fn nonzero_exit_after_data_emits_a_failed_terminal_and_cannot_gate() {
    let temp = tempfile::tempdir().unwrap();
    let ipc_path = temp.path().join("late-failure.arrow");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1])) as ArrayRef],
    )
    .unwrap();
    {
        let mut file = File::create(&ipc_path).unwrap();
        let mut writer = StreamWriter::try_new(&mut file, schema.as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        file.flush().unwrap();
    }
    let producer = SubprocessProducer::new(
        shell([
            "-c",
            "cat \"$1\"; printf 'late failure\n' >&2; exit 7",
            "cdf-test",
            ipc_path.to_str().unwrap(),
        ]),
        SubprocessProtocol::ArrowIpc,
        read_options(),
        DecodeSchemaPlan::fixed_admission(schema),
        SupervisionOptions::default(),
        memory(),
    )
    .unwrap();
    let opened = producer
        .open(ForeignStreamOpenRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            partition_id: PartitionId::new("p0").unwrap(),
            cancellation: Default::default(),
        })
        .await
        .unwrap();
    let termination = opened.termination.clone();
    let mut batches = cdf_foreign_stream::batch_stream_from_foreign_events(opened.events);

    let first = batches.next().await.unwrap().unwrap();
    assert_eq!(first.header.row_count, 1);
    let error = batches.next().await.unwrap().unwrap_err();
    assert_eq!(error.kind, ErrorKind::Transient);
    assert!(error.message.contains("exit code 7"));
    assert!(error.message.contains("late failure"));
    assert!(batches.next().await.is_none());
    termination.join().await.unwrap();
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn cancellation_before_first_frame_kills_descendants_and_joins() {
    let temp = tempfile::tempdir().unwrap();
    let descendant_pid = temp.path().join("stream-descendant.pid");
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let producer = SubprocessProducer::new(
        shell([
            "-c",
            "sleep 30 & child=$!; printf '%s' \"$child\" > \"$1\"; wait",
            "cdf-test",
            descendant_pid.to_str().unwrap(),
        ]),
        SubprocessProtocol::Ndjson,
        read_options(),
        DecodeSchemaPlan::fixed_admission(schema),
        SupervisionOptions {
            termination_grace: Duration::from_millis(100),
            ..SupervisionOptions::default()
        },
        memory(),
    )
    .unwrap();
    let mut opened = producer
        .open(ForeignStreamOpenRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            partition_id: PartitionId::new("p0").unwrap(),
            cancellation: Default::default(),
        })
        .await
        .unwrap();
    let termination = opened.termination.clone();
    let terminal = {
        let next = opened.events.next();
        tokio::pin!(next);
        tokio::select! {
            event = &mut next => panic!("subprocess unexpectedly terminated before cancellation: {event:?}"),
            () = tokio::time::sleep(Duration::from_millis(100)) => termination.cancel(),
        }
        next.as_mut().await.unwrap().unwrap()
    };
    assert!(
        matches!(
            terminal,
            ForeignStreamEvent::Terminal(ForeignTerminalStatus::Cancelled)
        ),
        "{terminal:?}"
    );
    drop(opened.events);
    termination.join().await.unwrap();

    let pid = fs::read_to_string(&descendant_pid)
        .unwrap()
        .trim()
        .parse::<i32>()
        .unwrap();
    let pid = rustix::process::Pid::from_raw(pid).unwrap();
    assert!(
        rustix::process::test_kill_process(pid).is_err(),
        "subprocess descendant {pid:?} survived cancellation"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn dropping_a_stream_cancels_the_child_and_join_releases_all_leases() {
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(96 * 1024 * 1024, BTreeMap::new()).unwrap());
    let admitted: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let producer = SubprocessProducer::new(
        shell(["-c", "printf '{\"id\":1}\\n'; sleep 30"]),
        SubprocessProtocol::Ndjson,
        read_options().with_batch_size(1).unwrap(),
        DecodeSchemaPlan::fixed_admission(schema),
        SupervisionOptions {
            termination_grace: Duration::from_millis(50),
            maximum_stream_chunk_bytes: 1024,
            ..SupervisionOptions::default()
        },
        admitted,
    )
    .unwrap();
    let opened = producer
        .open(ForeignStreamOpenRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            partition_id: PartitionId::new("p0").unwrap(),
            cancellation: Default::default(),
        })
        .await
        .unwrap();
    let termination = opened.termination.clone();
    let mut events = opened.events;
    let event = tokio::time::timeout(Duration::from_secs(2), events.next())
        .await
        .expect("NDJSON batch waited for child EOF instead of the configured row boundary")
        .unwrap()
        .unwrap();
    assert!(matches!(event, ForeignStreamEvent::Outcome(_)));
    drop(event);
    drop(events);

    tokio::time::timeout(Duration::from_secs(2), termination.join())
        .await
        .expect("producer task did not join after stream drop")
        .unwrap();
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn cancel_and_join_does_not_require_draining_a_full_stdout_channel() {
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(96 * 1024 * 1024, BTreeMap::new()).unwrap());
    let admitted: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let producer = SubprocessProducer::new(
        shell([
            "-c",
            "i=0; while [ \"$i\" -lt 100000 ]; do printf '{\"id\":%s}\\n' \"$i\"; i=$((i + 1)); done; sleep 30",
        ]),
        SubprocessProtocol::Ndjson,
        read_options().with_batch_size(1).unwrap(),
        DecodeSchemaPlan::fixed_admission(schema),
        SupervisionOptions {
            termination_grace: Duration::from_millis(50),
            maximum_stream_chunk_bytes: 64,
            ..SupervisionOptions::default()
        },
        admitted,
    )
    .unwrap();
    let mut opened = producer
        .open(ForeignStreamOpenRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            partition_id: PartitionId::new("p0").unwrap(),
            cancellation: Default::default(),
        })
        .await
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(2), opened.events.next())
        .await
        .expect("subprocess did not publish its first row")
        .unwrap()
        .unwrap();
    assert!(matches!(first, ForeignStreamEvent::Outcome(_)));
    drop(first);
    tokio::time::sleep(Duration::from_millis(20)).await;

    tokio::time::timeout(
        Duration::from_secs(2),
        opened.termination.terminate_and_join(),
    )
    .await
    .expect("cancel-and-join blocked behind undrained stdout")
    .unwrap();
    drop(opened.events);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn stalled_diagnostic_reader_is_bounded_and_aborted() {
    let task = tokio::spawn(async {
        std::future::pending::<cdf_kernel::Result<crate::runner::DiagnosticCapture>>().await
    });
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        crate::runner::join_diagnostic_reader_bounded(task, Duration::from_millis(10)),
    )
    .await
    .expect("diagnostic join exceeded its cleanup boundary")
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Transient);
    assert!(error.message.contains("stderr reader did not terminate"));
}

#[tokio::test(flavor = "current_thread")]
async fn nonzero_exit_maps_to_transient_with_stderr() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let events = collect_subprocess_events(
        SubprocessProducer::new(
            shell(["-c", "printf 'adapter failed\\n' >&2; exit 7"]),
            SubprocessProtocol::Ndjson,
            read_options(),
            DecodeSchemaPlan::fixed_admission(schema),
            SupervisionOptions::default(),
            memory(),
        )
        .unwrap(),
    )
    .await;
    let terminal = events.last().unwrap();
    let ForeignStreamEvent::Terminal(ForeignTerminalStatus::Failed { retryable, message }) =
        terminal
    else {
        panic!("expected a failed terminal, got {terminal:?}");
    };
    assert!(*retryable);
    assert!(message.contains("exit code 7"));
    assert!(message.contains("adapter failed"));
}

#[tokio::test(flavor = "current_thread")]
async fn timeout_maps_to_transient() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let events = collect_subprocess_events(
        SubprocessProducer::new(
            shell(["-c", "sleep 2"]),
            SubprocessProtocol::Ndjson,
            read_options(),
            DecodeSchemaPlan::fixed_admission(schema),
            SupervisionOptions {
                timeout: Some(Duration::from_millis(10)),
                stderr_line_limit: DEFAULT_STDERR_LINE_LIMIT,
                ..SupervisionOptions::default()
            },
            memory(),
        )
        .unwrap(),
    )
    .await;
    let ForeignStreamEvent::Terminal(ForeignTerminalStatus::Failed { retryable, message }) =
        events.last().unwrap()
    else {
        panic!("expected a failed terminal");
    };
    assert!(*retryable);
    assert!(message.contains("timed out"));
}

#[tokio::test(flavor = "current_thread")]
async fn command_supervisor_bounds_output_and_observes_cancellation() {
    let constrained: Arc<dyn MemoryCoordinator> =
        Arc::new(DeterministicMemoryCoordinator::new(8, BTreeMap::new()).unwrap());
    let error = run_bounded_command(
        shell(["-c", "printf ok"]),
        SupervisionOptions {
            maximum_stdout_bytes: 7,
            maximum_stderr_bytes: 1,
            ..SupervisionOptions::default()
        },
        cdf_runtime::RunCancellation::default(),
        constrained,
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Data);
    assert!(error.message.contains("memory budget cannot admit"));

    let error = run_bounded_command(
        shell(["-c", "printf '0123456789abcdef'"]),
        SupervisionOptions {
            maximum_stdout_bytes: 8,
            ..SupervisionOptions::default()
        },
        cdf_runtime::RunCancellation::default(),
        memory(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Data);
    assert!(error.message.contains("stdout"));
    assert!(error.message.contains("8-byte boundary"));

    let cancellation = cdf_runtime::RunCancellation::default();
    cancellation.cancel();
    let error = run_bounded_command(
        shell(["-c", "sleep 5"]),
        SupervisionOptions::default(),
        cancellation,
        memory(),
    )
    .await
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("cancelled"));
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn inherited_child_address_space_limit_is_declared_and_enforced() {
    let maximum = 64_u64 * 1024 * 1024 * 1024;
    let expected = effective_child_address_space_limit(
        maximum,
        rustix::process::getrlimit(rustix::process::Resource::As),
    );
    let supervision = SupervisionOptions {
        maximum_child_address_space_bytes: Some(maximum),
        maximum_stdout_bytes: 128,
        ..SupervisionOptions::default()
    };
    let output = run_bounded_command(
        shell(["-c", "ulimit -v"]),
        supervision.clone(),
        cdf_runtime::RunCancellation::default(),
        memory(),
    )
    .await
    .unwrap();
    let reported_kib = std::str::from_utf8(output.stdout.as_bytes())
        .unwrap()
        .trim()
        .parse::<u64>()
        .unwrap();
    assert_eq!(reported_kib, expected / 1024);

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let producer = SubprocessProducer::new(
        shell(["-c", "printf '{\"id\":1}\\n'"]),
        SubprocessProtocol::Ndjson,
        read_options(),
        DecodeSchemaPlan::fixed_admission(schema),
        supervision,
        memory(),
    )
    .unwrap();
    assert_eq!(
        producer.descriptor().memory.child_process_bytes,
        Some(maximum)
    );
}

#[cfg(target_os = "linux")]
#[test]
fn child_address_space_limit_never_raises_an_inherited_soft_limit() {
    assert_eq!(
        effective_child_address_space_limit(
            2 * 1024 * 1024 * 1024,
            rustix::process::Rlimit {
                current: Some(512 * 1024 * 1024),
                maximum: None,
            },
        ),
        512 * 1024 * 1024
    );
    assert_eq!(
        effective_child_address_space_limit(
            2 * 1024 * 1024 * 1024,
            rustix::process::Rlimit {
                current: None,
                maximum: Some(1024 * 1024 * 1024),
            },
        ),
        1024 * 1024 * 1024
    );
}

#[test]
fn child_address_space_limit_rejects_invalid_or_unsupported_authority() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    for invalid in [0, u64::MAX] {
        let error = SubprocessProducer::new(
            shell(["-c", "printf '{\"id\":1}\\n'"]),
            SubprocessProtocol::Ndjson,
            read_options(),
            DecodeSchemaPlan::fixed_admission(Arc::clone(&schema)),
            SupervisionOptions {
                maximum_child_address_space_bytes: Some(invalid),
                ..SupervisionOptions::default()
            },
            memory(),
        )
        .err()
        .unwrap();
        assert_eq!(error.kind, ErrorKind::Contract);
    }
    #[cfg(not(target_os = "linux"))]
    {
        let error = SubprocessProducer::new(
            shell(["-c", "printf '{\"id\":1}\\n'"]),
            SubprocessProtocol::Ndjson,
            read_options(),
            DecodeSchemaPlan::fixed_admission(schema),
            SupervisionOptions {
                maximum_child_address_space_bytes: Some(64 * 1024 * 1024 * 1024),
                ..SupervisionOptions::default()
            },
            memory(),
        )
        .err()
        .unwrap();
        assert_eq!(error.kind, ErrorKind::Contract);
        assert!(error.message.contains("unsupported"));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stderr_flood_is_drained_while_only_the_diagnostic_ring_is_retained() {
    let output = run_bounded_command(
        shell(["-c", "yes diagnostic-line | head -c 1048576 >&2; printf ok"]),
        SupervisionOptions {
            maximum_stdout_bytes: 8,
            maximum_stderr_bytes: 1024,
            ..SupervisionOptions::default()
        },
        cdf_runtime::RunCancellation::default(),
        memory(),
    )
    .await
    .unwrap();

    assert_eq!(output.stdout.as_bytes(), b"ok");
    assert!(output.stderr.is_truncated());
    assert!(output.stderr.discarded_bytes() > 1_000_000);
    assert!(output.stderr.summary().contains("<truncated>"));
}

#[tokio::test(flavor = "current_thread")]
async fn retained_stderr_redacts_injected_environment_values() {
    let command = shell(["-c", "printf '%s\n' \"$CDF_TEST_SECRET\" >&2; printf ok"])
        .with_env("CDF_TEST_SECRET", "super-secret-value");
    let output = run_bounded_command(
        command,
        SupervisionOptions {
            maximum_stdout_bytes: 8,
            maximum_stderr_bytes: 1024,
            ..SupervisionOptions::default()
        },
        cdf_runtime::RunCancellation::default(),
        memory(),
    )
    .await
    .unwrap();

    assert_eq!(output.stderr.lines(), vec!["<redacted>"]);
    assert!(!output.stderr.summary().contains("super-secret-value"));
}

#[tokio::test(flavor = "current_thread")]
async fn truncated_stderr_redacts_a_retained_secret_prefix() {
    let command = shell(["-c", "printf '%s' \"$CDF_TEST_SECRET\" >&2; printf ok"])
        .with_env("CDF_TEST_SECRET", "super-secret-value");
    let output = run_bounded_command(
        command,
        SupervisionOptions {
            maximum_stdout_bytes: 8,
            maximum_stderr_bytes: 6,
            ..SupervisionOptions::default()
        },
        cdf_runtime::RunCancellation::default(),
        memory(),
    )
    .await
    .unwrap();

    assert!(output.stderr.is_truncated());
    assert!(!output.stderr.summary().contains("super"));
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn timeout_terminates_the_entire_subprocess_process_group() {
    let temp = tempfile::tempdir().unwrap();
    let descendant_pid = temp.path().join("descendant.pid");
    let command = shell([
        "-c",
        "sleep 30 & child=$!; printf '%s' \"$child\" > \"$1\"; wait",
        "cdf-test",
        descendant_pid.to_str().unwrap(),
    ]);
    let error = run_bounded_command(
        command,
        SupervisionOptions {
            timeout: Some(Duration::from_millis(100)),
            termination_grace: Duration::from_millis(100),
            ..SupervisionOptions::default()
        },
        cdf_runtime::RunCancellation::default(),
        memory(),
    )
    .await
    .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Transient);
    let pid = fs::read_to_string(&descendant_pid).unwrap();
    let pid = pid.trim().parse::<i32>().unwrap();
    let pid = rustix::process::Pid::from_raw(pid).unwrap();
    assert!(
        rustix::process::test_kill_process(pid).is_err(),
        "subprocess descendant {pid:?} survived timeout"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn timeout_force_terminates_a_term_resistant_process_group() {
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        run_bounded_command(
            shell(["-c", "trap '' TERM; while :; do sleep 1; done"]),
            SupervisionOptions {
                timeout: Some(Duration::from_millis(50)),
                termination_grace: Duration::from_millis(50),
                ..SupervisionOptions::default()
            },
            cdf_runtime::RunCancellation::default(),
            memory(),
        ),
    )
    .await
    .expect("TERM-resistant process group was not force-terminated")
    .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Transient);
    assert!(error.message.contains("timed out"));
}

#[tokio::test(flavor = "current_thread")]
async fn malformed_stdout_maps_to_data_with_stderr_context() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let events = collect_subprocess_events(
        SubprocessProducer::new(
            shell(["-c", "printf 'parser warning\\n' >&2; printf '{bad\\n'"]),
            SubprocessProtocol::Ndjson,
            read_options(),
            DecodeSchemaPlan::fixed_admission(schema),
            SupervisionOptions::default(),
            memory(),
        )
        .unwrap(),
    )
    .await;
    let ForeignStreamEvent::Terminal(ForeignTerminalStatus::Failed { retryable, message }) =
        events.last().unwrap()
    else {
        panic!("expected a failed terminal");
    };
    assert!(!retryable);
    assert!(message.contains("parser warning"), "{message}");
    assert!(message.contains("decode NDJSON"), "{message}");
}

#[tokio::test(flavor = "current_thread")]
async fn singer_protocol_streams_selected_rows_and_ordered_control_with_bounded_memory() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("singer.ndjson");
    let state_value = json!({
        "bookmarks": {"orders": {"replication_key_value": "2026-07-06T00:00:00Z"}}
    });
    let schema_message = json!({
        "type": "SCHEMA",
        "stream": "orders",
        "schema": {"type": "object"},
        "key_properties": ["id"],
        "bookmark_properties": ["updated_at"],
        "tap_metadata": {"unknown": true}
    });
    fs::write(
        &input,
        ndjson(&[
            schema_message.clone(),
            json!({"type":"RECORD","stream":"other","record":{"id":0,"status":"ignored"}}),
            json!({"type":"RECORD","stream":"orders","record":{"id":1,"status":"open"}}),
            json!({"type":"RECORD","stream":"orders","record":{"id":2,"status":"closed"}}),
            json!({"type":"STATE","value":state_value}),
            json!({"type":"ACTIVATE_VERSION","stream":"orders","version":7}),
        ]),
    )
    .unwrap();
    let coordinator =
        Arc::new(DeterministicMemoryCoordinator::new(96 * 1024 * 1024, BTreeMap::new()).unwrap());
    let admitted: Arc<dyn MemoryCoordinator> = coordinator.clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("status", DataType::Utf8, true),
    ]));
    let events = collect_subprocess_events(
        SubprocessProducer::new(
            CommandSpec::new("cat").with_args([input.to_str().unwrap()]),
            SubprocessProtocol::Singer {
                stream: StreamIdentity::singer("orders"),
            },
            read_options().with_batch_size(1).unwrap(),
            DecodeSchemaPlan::fixed_admission(schema),
            SupervisionOptions {
                maximum_stream_chunk_bytes: 7,
                maximum_protocol_line_bytes: 4096,
                protocol_parser_scratch_bytes: 128 * 1024,
                protocol_row_window_bytes: 64,
                ..SupervisionOptions::default()
            },
            admitted,
        )
        .unwrap(),
    )
    .await;

    let mut sequences = Vec::new();
    let mut rows = 0_u64;
    let mut metadata = Vec::new();
    let mut state_position = None;
    let mut terminal = None;
    for event in events {
        match event {
            ForeignStreamEvent::Outcome(outcome) => {
                sequences.push(outcome.sequence);
                rows += outcome.batch.header.row_count;
                assert_eq!(outcome.transfer_mode, ForeignTransferMode::RowCompat);
            }
            ForeignStreamEvent::Control(control) => {
                sequences.push(control.sequence);
                match control.kind {
                    ForeignControlKind::ProtocolMetadata {
                        protocol,
                        message_type,
                        payload_sha256,
                    } => metadata.push((protocol, message_type, payload_sha256)),
                    ForeignControlKind::ForeignState { position } => {
                        state_position = Some(position)
                    }
                    other => panic!("unexpected Singer control: {other:?}"),
                }
            }
            ForeignStreamEvent::Terminal(status) => terminal = Some(status),
        }
    }
    assert_eq!(rows, 2);
    assert_eq!(sequences, (1..=sequences.len() as u64).collect::<Vec<_>>());
    assert_eq!(metadata[0].0, "singer");
    assert_eq!(metadata[0].1, "schema");
    assert_eq!(metadata[0].2, expected_hash(&schema_message));
    assert_eq!(metadata[1].1, "activate_version");
    let state_position = state_position.unwrap();
    assert_eq!(
        foreign_state(&state_position).blob_sha256,
        expected_hash(&state_value)
    );
    assert!(matches!(
        terminal,
        Some(ForeignTerminalStatus::Succeeded {
            final_position: Some(ref final_position)
        }) if final_position == &state_position
    ));
    drop(terminal);
    assert_eq!(coordinator.snapshot().current_bytes, 0);
}

#[test]
fn protocol_decoders_preserve_metadata_and_state_variants_without_collecting_streams() {
    let singer = decode_singer_message(
        7,
        br#"{"type":"schema","stream":"orders","schema":{},"key_properties":["id"],"bookmark_properties":["updated_at"],"future":true}"#,
    )
    .unwrap()
    .unwrap();
    let SingerMessage::Schema(singer) = singer else {
        panic!("expected Singer schema");
    };
    assert_eq!(singer.key_properties, vec!["id"]);
    assert_eq!(singer.bookmark_properties, vec!["updated_at"]);
    assert_eq!(singer.raw["future"], true);

    let catalog = decode_airbyte_message(
        8,
        br#"{"type":"CATALOG","catalog":{"streams":[]},"future":{"retained":true}}"#,
    )
    .unwrap()
    .unwrap();
    let AirbyteMessage::Catalog(catalog) = catalog else {
        panic!("expected Airbyte catalog");
    };
    assert_eq!(catalog.raw["future"]["retained"], true);

    let legacy = decode_airbyte_message(9, br#"{"type":"STATE","state":{"cursor":"old"}}"#)
        .unwrap()
        .unwrap();
    assert!(matches!(
        legacy,
        AirbyteMessage::State(AirbyteState {
            kind: AirbyteStateKind::Legacy,
            ..
        })
    ));
    let stream = decode_airbyte_message(
        10,
        br#"{"type":"STATE","state":{"type":"STREAM","stream":{"stream_descriptor":{"namespace":"crm","name":"users"}},"stream_state":{"cursor":7}}}"#,
    )
    .unwrap()
    .unwrap();
    let AirbyteMessage::State(stream) = stream else {
        panic!("expected Airbyte stream state");
    };
    assert_eq!(stream.kind, AirbyteStateKind::Stream);
    assert_eq!(
        stream.stream,
        Some(StreamIdentity::airbyte(Some("crm".to_owned()), "users"))
    );
    let global = decode_airbyte_message(
        11,
        br#"{"type":"STATE","state":{"type":"GLOBAL","global":{"shared_state":{},"stream_states":[]}}}"#,
    )
    .unwrap()
    .unwrap();
    assert!(matches!(
        global,
        AirbyteMessage::State(AirbyteState {
            kind: AirbyteStateKind::Global,
            ..
        })
    ));
}

#[test]
fn malformed_protocol_messages_are_data_errors_without_raw_state() {
    let singer = decode_singer_message(
        12,
        br#"{"type":"STATE","secret":"do-not-leak","token":"super-secret"}"#,
    )
    .err()
    .unwrap();
    assert_eq!(singer.kind, ErrorKind::Data);
    assert!(singer.message.contains("Singer STATE"));
    assert!(singer.message.contains("line 12"));
    assert!(!singer.message.contains("super-secret"));

    let airbyte = decode_airbyte_message(
        13,
        br#"{"type":"STATE","state":{"type":"STREAM","token":"super-secret"}}"#,
    )
    .unwrap_err();
    assert_eq!(airbyte.kind, ErrorKind::Data);
    assert!(airbyte.message.contains("Airbyte STATE"));
    assert!(airbyte.message.contains("line 13"));
    assert!(!airbyte.message.contains("super-secret"));

    let malformed_record = decode_airbyte_message(
        14,
        br#"{"type":"RECORD","record":{"stream":"users","data":{"id":1}}}"#,
    )
    .unwrap_err();
    assert_eq!(malformed_record.kind, ErrorKind::Data);
    assert!(malformed_record.message.contains("emitted_at"));
}

#[test]
fn protocol_decoders_validate_required_field_shapes() {
    for (line, bytes, field) in [
        (
            20,
            br#"{"type":"RECORD","stream":"  ","record":{"id":1}}"#.as_slice(),
            "stream",
        ),
        (
            21,
            br#"{"type":"RECORD","stream":"orders","record":[]}"#.as_slice(),
            "record",
        ),
        (
            22,
            br#"{"type":"SCHEMA","stream":"orders","schema":"bad","key_properties":["id"]}"#
                .as_slice(),
            "schema",
        ),
        (
            23,
            br#"{"type":"SCHEMA","stream":"orders","schema":{},"key_properties":[1]}"#.as_slice(),
            "key_properties",
        ),
    ] {
        let error = decode_singer_message(line, bytes).unwrap_err();
        assert!(error.message.contains(field));
        assert!(error.message.contains(&format!("line {line}")));
    }
    let catalog = decode_airbyte_message(24, br#"{"type":"CATALOG","catalog":[]}"#).unwrap_err();
    assert!(catalog.message.contains("catalog"));
    let decimal = decode_airbyte_message(
        25,
        br#"{"type":"RECORD","record":{"stream":"users","data":{},"emitted_at":1.25}}"#,
    )
    .unwrap_err();
    assert!(decimal.message.contains("emitted_at"));
    let maximum = decode_airbyte_message(
        26,
        br#"{"type":"RECORD","record":{"stream":"users","data":{},"emitted_at":18446744073709551615}}"#,
    )
    .unwrap()
    .unwrap();
    assert!(matches!(maximum, AirbyteMessage::Record(_)));
    let missing_legacy =
        decode_airbyte_message(27, br#"{"type":"STATE","state":{"type":"LEGACY"}}"#).unwrap_err();
    assert!(missing_legacy.message.contains("state.data"));
}

#[test]
fn protocol_state_hashes_are_deterministic() {
    let state = json!({"z":1,"a":[true,false],"m":{"b":2,"a":3}});
    let first = decode_singer_message(
        1,
        br#"{"type":"STATE","value":{"z":1,"a":[true,false],"m":{"b":2,"a":3}}}"#,
    )
    .unwrap()
    .unwrap();
    let reordered = decode_singer_message(
        1,
        br#"{"type":"STATE","value":{"m":{"a":3,"b":2},"a":[true,false],"z":1}}"#,
    )
    .unwrap()
    .unwrap();
    let SingerMessage::State(first) = first else {
        panic!("expected Singer state");
    };
    let SingerMessage::State(reordered) = reordered else {
        panic!("expected Singer state");
    };
    let first = first.source_position().unwrap();
    let reordered = reordered.source_position().unwrap();
    assert_eq!(foreign_state(&first).blob_sha256, expected_hash(&state));
    assert_eq!(first, reordered);
    assert_eq!(
        foreign_state_blob(&first),
        r#"{"a":[true,false],"m":{"a":3,"b":2},"z":1}"#
    );
}

#[tokio::test(flavor = "current_thread")]
async fn airbyte_protocol_streams_selected_rows_and_packages_for_replay() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("airbyte.ndjson");
    let global_state = json!({
        "type":"GLOBAL",
        "global":{"shared_state":{"sync_id":"abc"},"stream_states":[]}
    });
    fs::write(
        &input,
        ndjson(&[
            json!({"type":"CATALOG","catalog":{"streams":[]}}),
            json!({"type":"RECORD","record":{"namespace":"crm","stream":"other","data":{"id":0,"email":"ignored"},"emitted_at":1}}),
            json!({"type":"RECORD","record":{"namespace":"crm","stream":"users","data":{"id":1,"email":"ada@example.com"},"emitted_at":2}}),
            json!({"type":"RECORD","record":{"namespace":"crm","stream":"users","data":{"id":2,"email":"grace@example.com"},"emitted_at":3}}),
            json!({"type":"STATE","state":global_state}),
        ]),
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("email", DataType::Utf8, true),
    ]));
    let events = collect_subprocess_events(
        SubprocessProducer::new(
            CommandSpec::new("cat").with_args([input.to_str().unwrap()]),
            SubprocessProtocol::Airbyte {
                stream: StreamIdentity::airbyte(Some("crm".to_owned()), "users"),
            },
            read_options().with_batch_size(1).unwrap(),
            DecodeSchemaPlan::fixed_admission(schema),
            SupervisionOptions {
                maximum_stream_chunk_bytes: 11,
                maximum_protocol_line_bytes: 4096,
                protocol_parser_scratch_bytes: 128 * 1024,
                protocol_row_window_bytes: 128,
                ..SupervisionOptions::default()
            },
            memory(),
        )
        .unwrap(),
    )
    .await;
    let mut batches = Vec::new();
    let mut terminal = None;
    for event in events {
        match event {
            ForeignStreamEvent::Outcome(outcome) => {
                batches.push(outcome.batch.record_batch().unwrap().clone())
            }
            ForeignStreamEvent::Terminal(status) => terminal = Some(status),
            ForeignStreamEvent::Control(_) => {}
        }
    }
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    let Some(ForeignTerminalStatus::Succeeded {
        final_position: Some(position),
    }) = terminal
    else {
        panic!("expected successful Airbyte terminal with state");
    };
    assert_eq!(
        foreign_state(&position).blob_sha256,
        expected_hash(&global_state)
    );

    let package_dir = temp.path().join("protocol-package");
    let batches = cdf_package_contract::append_package_row_ord(batches, 0).unwrap();
    let package = cdf_package::PackageBuilder::create(
        &package_dir,
        "pkg-protocol",
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)
            .unwrap(),
    )
    .unwrap();
    package
        .write_segment(SegmentId::new("seg-protocol-0").unwrap(), 0, &batches)
        .unwrap();
    package.finish().unwrap();
    let reader = cdf_package::PackageReader::open(&package_dir).unwrap();
    reader.verify().unwrap();
    let replayed = reader
        .verified_canonical_segment_stream(memory(), 256 * 1024 * 1024)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .batches;
    assert_eq!(replayed.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn protocol_line_boundary_fails_closed_across_tiny_pipe_chunks() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let events = collect_subprocess_events(
        SubprocessProducer::new(
            shell([
                "-c",
                "printf '%s\\n' '{\"type\":\"RECORD\",\"stream\":\"orders\",\"record\":{\"id\":123456789}}'",
            ]),
            SubprocessProtocol::Singer {
                stream: StreamIdentity::singer("orders"),
            },
            read_options(),
            DecodeSchemaPlan::fixed_admission(schema),
            SupervisionOptions {
                maximum_stream_chunk_bytes: 3,
                maximum_protocol_line_bytes: 32,
                protocol_parser_scratch_bytes: 1024,
                protocol_row_window_bytes: 1024,
                ..SupervisionOptions::default()
            },
            memory(),
        )
        .unwrap(),
    )
    .await;
    let ForeignStreamEvent::Terminal(ForeignTerminalStatus::Failed { message, .. }) =
        events.last().unwrap()
    else {
        panic!("expected oversized protocol line to fail");
    };
    assert!(message.contains("32-byte payload boundary"));
}

#[tokio::test(flavor = "current_thread")]
async fn protocol_line_boundary_excludes_eof_lf_and_crlf_framing() {
    const PREFIX: &[u8] = br#"{"type":"RECORD","stream":"orders","record":{"id":1,"pad":""#;
    const SUFFIX: &[u8] = br#""}}"#;
    const PAYLOAD_BYTES: usize = 256;

    let mut payload = Vec::from(PREFIX);
    payload.extend(std::iter::repeat_n(
        b'x',
        PAYLOAD_BYTES - PREFIX.len() - SUFFIX.len(),
    ));
    payload.extend_from_slice(SUFFIX);
    assert_eq!(payload.len(), PAYLOAD_BYTES);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("pad", DataType::Utf8, true),
    ]));

    for (name, terminator) in [
        ("eof", &b""[..]),
        ("lf", &b"\n"[..]),
        ("crlf", &b"\r\n"[..]),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let input = temp.path().join(format!("singer-{name}.ndjson"));
        let mut framed = payload.clone();
        framed.extend_from_slice(terminator);
        fs::write(&input, framed).unwrap();
        let events = collect_subprocess_events(
            SubprocessProducer::new(
                CommandSpec::new("cat").with_args([input.to_str().unwrap()]),
                SubprocessProtocol::Singer {
                    stream: StreamIdentity::singer("orders"),
                },
                read_options(),
                DecodeSchemaPlan::fixed_admission(Arc::clone(&schema)),
                SupervisionOptions {
                    maximum_stream_chunk_bytes: 3,
                    maximum_protocol_line_bytes: PAYLOAD_BYTES as u64,
                    protocol_parser_scratch_bytes: (PAYLOAD_BYTES as u64) * 32,
                    protocol_row_window_bytes: 1024,
                    ..SupervisionOptions::default()
                },
                memory(),
            )
            .unwrap(),
        )
        .await;
        assert!(events.iter().any(|event| matches!(
            event,
            ForeignStreamEvent::Outcome(outcome) if outcome.batch.header.row_count == 1
        )));
        assert!(matches!(
            events.last(),
            Some(ForeignStreamEvent::Terminal(
                ForeignTerminalStatus::Succeeded { .. }
            ))
        ));
    }

    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("singer-too-long.ndjson");
    let mut framed = payload;
    framed.push(b'\n');
    fs::write(&input, framed).unwrap();
    let maximum = (PAYLOAD_BYTES - 1) as u64;
    let events = collect_subprocess_events(
        SubprocessProducer::new(
            CommandSpec::new("cat").with_args([input.to_str().unwrap()]),
            SubprocessProtocol::Singer {
                stream: StreamIdentity::singer("orders"),
            },
            read_options(),
            DecodeSchemaPlan::fixed_admission(schema),
            SupervisionOptions {
                maximum_stream_chunk_bytes: 3,
                maximum_protocol_line_bytes: maximum,
                protocol_parser_scratch_bytes: maximum * 32,
                protocol_row_window_bytes: 1024,
                ..SupervisionOptions::default()
            },
            memory(),
        )
        .unwrap(),
    )
    .await;
    assert!(matches!(
        events.last(),
        Some(ForeignStreamEvent::Terminal(
            ForeignTerminalStatus::Failed { message, .. }
        )) if message.contains("255-byte payload boundary")
    ));
}

#[test]
fn protocol_parser_scratch_must_cover_preparse_dom_expansion() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let error = SubprocessProducer::new(
        shell(["-c", "printf ''"]),
        SubprocessProtocol::Singer {
            stream: StreamIdentity::singer("orders"),
        },
        read_options(),
        DecodeSchemaPlan::fixed_admission(schema),
        SupervisionOptions {
            maximum_protocol_line_bytes: 1024,
            protocol_parser_scratch_bytes: 32 * 1024 - 1,
            ..SupervisionOptions::default()
        },
        memory(),
    )
    .err()
    .unwrap();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(error.message.contains("32x"));
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "release performance envelope"]
async fn subprocess_stream_release_envelope_reports_ipc_and_row_modes_separately() {
    const ROWS: usize = 512 * 1024;
    const BATCH_ROWS: usize = 64 * 1024;

    async fn measure(
        input: &std::path::Path,
        protocol: SubprocessProtocol,
        schema: Arc<Schema>,
        source_bytes: u64,
    ) -> (u64, u64, u64, u64, u64, String) {
        let coordinator = Arc::new(
            DeterministicMemoryCoordinator::new(512 * 1024 * 1024, BTreeMap::new()).unwrap(),
        );
        let admitted: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let producer = SubprocessProducer::new(
            CommandSpec::new("cat").with_args([input.to_str().unwrap()]),
            protocol,
            read_options().with_batch_size(BATCH_ROWS).unwrap(),
            DecodeSchemaPlan::fixed_admission(schema),
            SupervisionOptions::default(),
            admitted,
        )
        .unwrap();
        let started = std::time::Instant::now();
        let mut opened = producer
            .open(ForeignStreamOpenRequest {
                resource_id: ResourceId::new("orders").unwrap(),
                partition_id: PartitionId::new("p0").unwrap(),
                cancellation: Default::default(),
            })
            .await
            .unwrap();
        let mut rows = 0_u64;
        let mut logical_bytes = 0_u64;
        let mut batches = 0_u64;
        let mut first_batch_ns = None;
        let mut copy = None;
        let mut terminal = None;
        while let Some(event) = opened.events.next().await {
            match event.unwrap() {
                ForeignStreamEvent::Outcome(outcome) => {
                    first_batch_ns.get_or_insert_with(|| {
                        u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
                    });
                    rows += outcome.batch.header.row_count;
                    logical_bytes += outcome.batch.header.byte_count;
                    batches += 1;
                    copy.get_or_insert_with(|| format!("{:?}", outcome.copy));
                }
                ForeignStreamEvent::Control(_) => {}
                ForeignStreamEvent::Terminal(status) => terminal = Some(status),
            }
        }
        opened.termination.join().await.unwrap();
        assert!(matches!(
            terminal,
            Some(ForeignTerminalStatus::Succeeded { .. })
        ));
        let elapsed_ns = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let peak = coordinator.snapshot().peak_bytes;
        assert_eq!(coordinator.snapshot().current_bytes, 0);
        (
            rows,
            batches,
            logical_bytes,
            first_batch_ns.unwrap(),
            elapsed_ns,
            format!(
                "source_bytes={source_bytes} managed_peak_bytes={peak} copy={}",
                copy.unwrap()
            ),
        )
    }

    let temp = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
    ]));
    let ipc = temp.path().join("subprocess-envelope.arrow");
    {
        let file = File::create(&ipc).unwrap();
        let mut writer = StreamWriter::try_new(BufWriter::new(file), schema.as_ref()).unwrap();
        for batch in 0..(ROWS / BATCH_ROWS) {
            let start = i64::try_from(batch * BATCH_ROWS).unwrap();
            let ids = Int64Array::from_iter_values(
                (0..BATCH_ROWS).map(|offset| start + i64::try_from(offset).unwrap()),
            );
            let values = StringArray::from_iter_values((0..BATCH_ROWS).map(|_| "subprocess-value"));
            writer
                .write(
                    &RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(ids) as ArrayRef, Arc::new(values)],
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        writer.finish().unwrap();
    }
    let row = temp.path().join("subprocess-envelope.ndjson");
    {
        let mut writer = BufWriter::new(File::create(&row).unwrap());
        for id in 0..ROWS {
            writeln!(writer, "{{\"id\":{id},\"value\":\"subprocess-value\"}}").unwrap();
        }
        writer.flush().unwrap();
    }

    for (name, path, protocol) in [
        ("arrow_ipc_stream", &ipc, SubprocessProtocol::ArrowIpc),
        ("row_compat_ndjson", &row, SubprocessProtocol::Ndjson),
    ] {
        let source_bytes = fs::metadata(path).unwrap().len();
        let (rows, batches, logical_bytes, first_batch_ns, elapsed_ns, detail) =
            measure(path, protocol, Arc::clone(&schema), source_bytes).await;
        assert_eq!(rows, ROWS as u64);
        println!(
            "mode={name} rows={rows} batches={batches} logical_bytes={logical_bytes} first_batch_ns={first_batch_ns} elapsed_ns={elapsed_ns} {detail}"
        );
    }
}
