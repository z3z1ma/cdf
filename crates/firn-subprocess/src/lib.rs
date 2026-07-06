#![doc = "Subprocess adapter boundary for firn."]

use std::{
    collections::BTreeMap,
    path::PathBuf,
    process::{ExitStatus, Stdio},
    time::Duration,
};

use firn_formats::{FormatRead, JsonOptions, ReadOptions};
use firn_kernel::{ErrorKind, FirnError, Result};
use serde::{Deserialize, Serialize};
use tokio::{process::Command, time::timeout};

pub const DEFAULT_STDERR_LINE_LIMIT: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub current_dir: Option<PathBuf>,
    pub env: BTreeMap<String, String>,
}

impl CommandSpec {
    pub fn new(program: impl Into<String>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            current_dir: None,
            env: BTreeMap::new(),
        }
    }

    pub fn with_args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_current_dir(mut self, current_dir: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(current_dir.into());
        self
    }

    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StdoutFormat {
    ArrowIpc,
    Ndjson,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SupervisionOptions {
    pub timeout: Option<Duration>,
    pub stderr_line_limit: usize,
}

impl Default for SupervisionOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            stderr_line_limit: DEFAULT_STDERR_LINE_LIMIT,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StderrTrace {
    pub lines: Vec<String>,
    pub truncated: bool,
}

#[derive(Clone, Debug)]
pub struct SubprocessOutput {
    pub read: FormatRead,
    pub stderr: StderrTrace,
    pub exit_status: ExitStatus,
}

pub async fn run_stdout_adapter(
    command: &CommandSpec,
    stdout_format: StdoutFormat,
    read_options: &ReadOptions,
    supervision: &SupervisionOptions,
) -> Result<SubprocessOutput> {
    let mut process = Command::new(&command.program);
    process
        .args(&command.args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(current_dir) = &command.current_dir {
        process.current_dir(current_dir);
    }
    for (key, value) in &command.env {
        process.env(key, value);
    }

    let child = process
        .spawn()
        .map_err(|error| FirnError::internal(format!("spawn subprocess: {error}")))?;
    let wait = child.wait_with_output();
    let output = match supervision.timeout {
        Some(duration) => timeout(duration, wait).await.map_err(|_| {
            FirnError::transient(format!(
                "subprocess timed out after {} ms",
                duration.as_millis()
            ))
        })?,
        None => wait.await,
    }
    .map_err(|error| FirnError::internal(format!("wait for subprocess: {error}")))?;

    let stderr = StderrTrace::from_bytes(&output.stderr, supervision.stderr_line_limit);
    if !output.status.success() {
        return Err(FirnError::new(
            ErrorKind::Transient,
            format!(
                "subprocess exited unsuccessfully: {}; stderr: {}",
                status_message(output.status),
                stderr.summary()
            ),
        ));
    }

    let read = match stdout_format {
        StdoutFormat::ArrowIpc => {
            firn_formats::read_arrow_ipc_stream(output.stdout.as_slice(), read_options)
        }
        StdoutFormat::Ndjson => {
            firn_formats::read_ndjson_bytes(&output.stdout, read_options, &JsonOptions::default())
        }
    }
    .map_err(|error| add_stderr_context(error, &stderr, stdout_format))?;

    Ok(SubprocessOutput {
        read,
        stderr,
        exit_status: output.status,
    })
}

impl StderrTrace {
    pub fn from_bytes(bytes: &[u8], line_limit: usize) -> Self {
        let text = String::from_utf8_lossy(bytes);
        let mut lines = Vec::new();
        let mut truncated = false;
        for line in text.lines() {
            if lines.len() == line_limit {
                truncated = true;
                break;
            }
            lines.push(line.to_owned());
        }
        Self { lines, truncated }
    }

    pub fn summary(&self) -> String {
        if self.lines.is_empty() {
            return "<empty>".to_owned();
        }
        let mut summary = self.lines.join(" | ");
        if self.truncated {
            summary.push_str(" | <truncated>");
        }
        summary
    }
}

fn add_stderr_context(
    error: FirnError,
    stderr: &StderrTrace,
    stdout_format: StdoutFormat,
) -> FirnError {
    FirnError {
        kind: error.kind,
        message: format!(
            "malformed {stdout_format:?} subprocess stdout: {}; stderr: {}",
            error.message,
            stderr.summary()
        ),
        retry_after_ms: error.retry_after_ms,
    }
}

fn status_message(status: ExitStatus) -> String {
    if let Some(code) = status.code() {
        return format!("exit code {code}");
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return format!("signal {signal}");
        }
    }
    "unknown exit status".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        fs::{self, File},
        io::Write,
        sync::Arc,
    };

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field, Schema};
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
}
