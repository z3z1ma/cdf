use std::process::{ExitStatus, Stdio};

use firn_formats::{JsonOptions, ReadOptions};
use firn_kernel::{ErrorKind, FirnError, Result};
use tokio::{process::Command, time::timeout};

use crate::{CommandSpec, StderrTrace, StdoutFormat, SubprocessOutput, SupervisionOptions};

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
