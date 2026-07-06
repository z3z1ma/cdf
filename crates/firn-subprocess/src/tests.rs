use super::*;

use std::{
    fs::{self, File},
    io::Write,
    sync::Arc,
    time::Duration,
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use firn_formats::ReadOptions;
use firn_kernel::{ErrorKind, PartitionId, ResourceId, SegmentId};

fn read_options() -> ReadOptions {
    ReadOptions::new(
        ResourceId::new("orders").unwrap(),
        PartitionId::new("p0").unwrap(),
    )
}

fn shell(args: impl IntoIterator<Item = impl Into<String>>) -> CommandSpec {
    CommandSpec::new("/bin/sh").with_args(args)
}

#[tokio::test(flavor = "current_thread")]
async fn ndjson_stdout_adapter_captures_stderr_and_packages_output() {
    let temp = tempfile::tempdir().unwrap();
    let ndjson_path = temp.path().join("orders.ndjson");
    fs::write(&ndjson_path, "{\"id\":1,\"name\":\"ada\"}\n").unwrap();
    let command = shell([
        "-c",
        "printf 'fetch trace\\n' >&2; cat \"$1\"",
        "firn-test",
        ndjson_path.to_str().unwrap(),
    ]);

    let output = run_stdout_adapter(
        &command,
        StdoutFormat::Ndjson,
        &read_options(),
        &SupervisionOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(output.stderr.lines, vec!["fetch trace"]);
    assert_eq!(output.read.batches[0].header.row_count, 1);

    let mut package =
        firn_package::PackageBuilder::create(temp.path().join("package"), "pkg-subprocess")
            .unwrap();
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
    firn_package::PackageReader::open(temp.path().join("package"))
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
    )
    .await
    .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Data);
    assert!(error.message.contains("parser warning"));
    assert!(error.message.contains("malformed Ndjson"));
}
