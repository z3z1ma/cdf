use std::{
    future::Future,
    pin::Pin,
    process::{ExitStatus, Stdio},
    sync::Arc,
};

use cdf_kernel::{CdfError, ErrorKind, Result};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest};
use cdf_runtime::{
    BoundedFormatRequest, MemoryByteSource, ReadOptions, RunCancellation, decode_bounded_format,
};
use tokio::{io::AsyncReadExt, process::Command};

use crate::{
    BoundedCommandBytes, BoundedCommandOutput, CommandSpec, StderrTrace, StdoutFormat,
    SubprocessOutput, SubprocessRead, SupervisionOptions,
};

pub async fn run_bounded_command(
    command: CommandSpec,
    supervision: SupervisionOptions,
    cancellation: RunCancellation,
    memory: Arc<dyn MemoryCoordinator>,
) -> Result<BoundedCommandOutput> {
    if supervision.maximum_stdout_bytes == 0
        || supervision.maximum_stderr_bytes == 0
        || supervision.maximum_stdout_bytes == u64::MAX
        || supervision.maximum_stderr_bytes == u64::MAX
    {
        return Err(CdfError::contract(
            "subprocess stdout and stderr byte boundaries must be within 1..u64::MAX",
        ));
    }
    if supervision.stderr_line_limit == 0 {
        return Err(CdfError::contract(
            "subprocess stderr line boundary must be greater than zero",
        ));
    }
    cancellation.check()?;
    let stdout_lease = reserve_output_capacity(
        &memory,
        "subprocess-stdout",
        supervision.maximum_stdout_bytes,
    )?;
    let stderr_lease = reserve_output_capacity(
        &memory,
        "subprocess-stderr",
        supervision.maximum_stderr_bytes,
    )?;
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

    let mut child = process
        .spawn()
        .map_err(|error| CdfError::internal(format!("spawn subprocess: {error}")))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| CdfError::internal("subprocess stdout pipe was not created"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| CdfError::internal("subprocess stderr pipe was not created"))?;
    let mut stdout_task = tokio::spawn(read_bounded(
        stdout,
        supervision.maximum_stdout_bytes,
        "stdout",
    ));
    let mut stderr_task = tokio::spawn(read_bounded(
        stderr,
        supervision.maximum_stderr_bytes,
        "stderr",
    ));
    let mut wait = Box::pin(child.wait());
    let mut deadline: Pin<Box<dyn Future<Output = ()> + Send>> = match supervision.timeout {
        Some(duration) => Box::pin(tokio::time::sleep(duration)),
        None => Box::pin(std::future::pending()),
    };
    let cancelled = cancellation.cancelled();
    tokio::pin!(cancelled);
    let mut exit_status = None;
    let mut stdout = None;
    let mut stderr = None;
    let mut stdout_done = false;
    let mut stderr_done = false;
    let terminal_error = loop {
        if exit_status.is_some() && stdout.is_some() && stderr.is_some() {
            break None;
        }
        tokio::select! {
            result = &mut wait, if exit_status.is_none() => {
                match result {
                    Ok(status) => exit_status = Some(status),
                    Err(error) => break Some(CdfError::internal(format!("wait for subprocess: {error}"))),
                }
            }
            result = &mut stdout_task, if !stdout_done => {
                stdout_done = true;
                match join_bounded_reader(result, "stdout") {
                    Ok(bytes) => stdout = Some(bytes),
                    Err(error) => break Some(error),
                }
            }
            result = &mut stderr_task, if !stderr_done => {
                stderr_done = true;
                match join_bounded_reader(result, "stderr") {
                    Ok(bytes) => stderr = Some(bytes),
                    Err(error) => break Some(error),
                }
            }
            () = &mut deadline => {
                let timeout_ms = supervision.timeout.map_or(0, |duration| duration.as_millis());
                break Some(CdfError::transient(format!(
                    "subprocess timed out after {timeout_ms} ms"
                )));
            }
            () = &mut cancelled => {
                break Some(cancellation.check().unwrap_err());
            }
        }
    };
    drop(wait);
    if let Some(error) = terminal_error {
        let _ = child.start_kill();
        let _ = child.wait().await;
        if !stdout_done {
            stdout_task.abort();
            let _ = stdout_task.await;
        }
        if !stderr_done {
            stderr_task.abort();
            let _ = stderr_task.await;
        }
        return Err(error);
    }
    let stdout = BoundedCommandBytes::new(
        stdout.expect("completed subprocess captured stdout"),
        stdout_lease,
    )?;
    let stderr = BoundedCommandBytes::new(
        stderr.expect("completed subprocess captured stderr"),
        stderr_lease,
    )?;
    Ok(BoundedCommandOutput {
        stdout,
        stderr: StderrTrace::new(stderr, supervision.stderr_line_limit),
        exit_status: exit_status.expect("completed subprocess captured exit status"),
    })
}

fn reserve_output_capacity(
    memory: &Arc<dyn MemoryCoordinator>,
    consumer: &str,
    maximum_bytes: u64,
) -> Result<MemoryLease> {
    let accounted_bytes = maximum_bytes.saturating_add(1);
    let request = ReservationRequest::new(
        ConsumerKey::new(consumer, MemoryClass::Source)?,
        accounted_bytes,
    )?
    .as_minimum_working_set();
    memory.try_reserve(&request)?.ok_or_else(|| {
        CdfError::data(format!(
            "subprocess requires a {accounted_bytes}-byte {consumer} buffer (including the overflow sentinel) but the memory budget cannot admit it; lower the subprocess output boundary or raise the run memory budget"
        ))
    })
}

async fn read_bounded<R>(
    reader: R,
    maximum_bytes: u64,
    stream_name: &'static str,
) -> Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    reader
        .take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| CdfError::internal(format!("read subprocess {stream_name}: {error}")))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum_bytes {
        return Err(CdfError::data(format!(
            "subprocess {stream_name} exceeded the {maximum_bytes}-byte boundary"
        )));
    }
    Ok(bytes)
}

fn join_bounded_reader(
    result: std::result::Result<Result<Vec<u8>>, tokio::task::JoinError>,
    stream_name: &str,
) -> Result<Vec<u8>> {
    result.map_err(|error| {
        CdfError::internal(format!("subprocess {stream_name} reader failed: {error}"))
    })?
}

pub async fn run_stdout_adapter(
    command: &CommandSpec,
    stdout_format: StdoutFormat,
    read_options: &ReadOptions,
    supervision: &SupervisionOptions,
    memory: Arc<dyn MemoryCoordinator>,
) -> Result<SubprocessOutput> {
    let output = run_bounded_command(
        command.clone(),
        supervision.clone(),
        RunCancellation::default(),
        Arc::clone(&memory),
    )
    .await?;
    let stderr = output.stderr;
    if !output.exit_status.success() {
        return Err(CdfError::new(
            ErrorKind::Transient,
            format!(
                "subprocess exited unsuccessfully: {}; stderr: {}",
                status_message(output.exit_status),
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
        MemoryByteSource::from_accounted_bytes(
            "subprocess:stdout",
            output.stdout.into_accounted()?,
        )
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
        exit_status: output.exit_status,
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
