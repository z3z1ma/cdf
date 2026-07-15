use std::{
    process::{ExitStatus, Stdio},
    sync::Arc,
};

use cdf_kernel::{CdfError, ErrorKind, Result};
use cdf_memory::MemoryCoordinator;
use cdf_runtime::{BoundedFormatRequest, MemoryByteSource, ReadOptions, decode_bounded_format};
use tokio::{process::Command, time::timeout};

use crate::{
    CommandSpec, StderrTrace, StdoutFormat, SubprocessOutput, SubprocessRead, SupervisionOptions,
};

pub async fn run_stdout_adapter(
    command: &CommandSpec,
    stdout_format: StdoutFormat,
    read_options: &ReadOptions,
    supervision: &SupervisionOptions,
    memory: Arc<dyn MemoryCoordinator>,
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
        .map_err(|error| CdfError::internal(format!("spawn subprocess: {error}")))?;
    let wait = child.wait_with_output();
    let output = match supervision.timeout {
        Some(duration) => timeout(duration, wait).await.map_err(|_| {
            CdfError::transient(format!(
                "subprocess timed out after {} ms",
                duration.as_millis()
            ))
        })?,
        None => wait.await,
    }
    .map_err(|error| CdfError::internal(format!("wait for subprocess: {error}")))?;

    let stderr = StderrTrace::from_bytes(&output.stderr, supervision.stderr_line_limit);
    if !output.status.success() {
        return Err(CdfError::new(
            ErrorKind::Transient,
            format!(
                "subprocess exited unsuccessfully: {}; stderr: {}",
                status_message(output.status),
                stderr.summary()
            ),
        ));
    }

    let driver: Arc<dyn cdf_runtime::FormatDriver> = match stdout_format {
        StdoutFormat::ArrowIpc => {
            Arc::new(cdf_format_arrow_ipc::ArrowIpcStreamFormatDriver::new()?)
        }
        StdoutFormat::Ndjson => Arc::new(cdf_format_json::NdjsonFormatDriver::new()?),
    };
    let source = Arc::new(
        MemoryByteSource::from_bytes("subprocess:stdout", output.stdout, Arc::clone(&memory))
            .await
            .map_err(|error| add_stderr_context(error, &stderr, stdout_format))?,
    );
    let read = decode_bounded_format(
        driver,
        source,
        BoundedFormatRequest::new(read_options.clone(), memory),
    )
    .await
    .and_then(|read| {
        SubprocessRead::from_bounded(
            read,
            cdf_kernel::ScopeKey::Stream {
                name: "subprocess_stdout".to_owned(),
            },
        )
    })
    .map_err(|error| add_stderr_context(error, &stderr, stdout_format))?;

    Ok(SubprocessOutput {
        read,
        stderr,
        exit_status: output.status,
    })
}

fn add_stderr_context(
    error: CdfError,
    stderr: &StderrTrace,
    stdout_format: StdoutFormat,
) -> CdfError {
    CdfError {
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
