use std::{
    future::Future,
    pin::Pin,
    process::{ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use cdf_kernel::{CdfError, ErrorKind, Result};
use cdf_memory::AccountedBytes;
use cdf_memory::{ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest};
use cdf_runtime::{
    AccountedByteStream, BoundedFormatRequest, ByteExtent, ByteSource, ByteSourceCapabilities,
    ContentIdentity, DecodeSchemaPlan, GenerationStrength, MemoryByteSource, ReadOptions,
    RunCancellation, SequentialReadRequest, decode_bounded_format, decode_format_stream,
};
use futures_util::stream;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, ChildStdout, Command},
};

use crate::{
    BoundedCommandBytes, BoundedCommandOutput, CommandSpec, StderrTrace, StdoutFormat,
    SubprocessCompletion, SubprocessCompletionHandle, SubprocessOutput, SubprocessRead,
    SubprocessStreamOutput, SupervisionOptions, command::descriptor_for_schema_hash,
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

pub async fn run_stdout_adapter_streaming(
    command: &CommandSpec,
    stdout_format: StdoutFormat,
    read_options: &ReadOptions,
    schema: DecodeSchemaPlan,
    supervision: &SupervisionOptions,
    memory: Arc<dyn MemoryCoordinator>,
) -> Result<SubprocessStreamOutput> {
    validate_supervision(supervision)?;
    let completion = SubprocessCompletionHandle::new();
    let driver: Arc<dyn cdf_runtime::FormatDriver> = match stdout_format {
        StdoutFormat::ArrowIpc => {
            Arc::new(cdf_format_arrow_ipc::ArrowIpcStreamFormatDriver::new()?)
        }
        StdoutFormat::Ndjson => Arc::new(cdf_format_json::NdjsonFormatDriver::new()?),
    };
    let schema_hash = cdf_kernel::canonical_arrow_schema_hash(schema.authority_schema.as_ref())?;
    let source: Arc<dyn ByteSource> = Arc::new(SubprocessStdoutByteSource::new(
        command.clone(),
        supervision.clone(),
        Arc::clone(&memory),
        completion.clone(),
    )?);
    let stream = decode_format_stream(
        driver,
        source,
        BoundedFormatRequest::new(read_options.clone(), memory)
            .with_schema(schema)
            .with_cancellation(RunCancellation::default()),
    )
    .await
    .map_err(|error| CdfError {
        kind: error.kind,
        message: format!(
            "stream subprocess {stdout_format:?} stdout: {}",
            error.message
        ),
        retry_after_ms: error.retry_after_ms,
    })?;
    let descriptor = descriptor_for_schema_hash(
        read_options.resource_id.clone(),
        schema_hash,
        cdf_kernel::ScopeKey::Stream {
            name: "subprocess_stdout".to_owned(),
        },
        "subprocess-stream-format-driver",
    );
    Ok(SubprocessStreamOutput {
        descriptor,
        batches: stream.batches,
        completion,
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

fn validate_supervision(supervision: &SupervisionOptions) -> Result<()> {
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
    Ok(())
}

struct SubprocessStdoutByteSource {
    command: CommandSpec,
    supervision: SupervisionOptions,
    memory: Arc<dyn MemoryCoordinator>,
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    opened: AtomicBool,
    completion: SubprocessCompletionHandle,
}

impl SubprocessStdoutByteSource {
    fn new(
        command: CommandSpec,
        supervision: SupervisionOptions,
        memory: Arc<dyn MemoryCoordinator>,
        completion: SubprocessCompletionHandle,
    ) -> Result<Self> {
        validate_supervision(&supervision)?;
        let stable_id = format!("subprocess:stdout:{}", command.program);
        let identity = ContentIdentity {
            stable_id,
            size_bytes: None,
            generation: Some("invocation-local-subprocess-stdout".to_owned()),
            checksum: None,
            strength: GenerationStrength::Weak,
        };
        identity.validate()?;
        let capabilities = ByteSourceCapabilities {
            known_length: false,
            reopenable: false,
            seekable: false,
            exact_ranges: false,
            useful_range_concurrency: 0,
            minimum_chunk_bytes: 1,
            maximum_chunk_bytes: supervision.maximum_stdout_bytes,
        };
        capabilities.validate()?;
        Ok(Self {
            command,
            supervision,
            memory,
            identity,
            capabilities,
            opened: AtomicBool::new(false),
            completion,
        })
    }
}

impl ByteSource for SubprocessStdoutByteSource {
    fn identity(&self) -> &ContentIdentity {
        &self.identity
    }

    fn capabilities(&self) -> &ByteSourceCapabilities {
        &self.capabilities
    }

    fn open_sequential(
        &self,
        request: SequentialReadRequest,
    ) -> cdf_kernel::BoxFuture<'_, Result<AccountedByteStream>> {
        Box::pin(async move {
            request.cancellation.check()?;
            if request.preferred_chunk_bytes == 0 {
                return Err(CdfError::contract(
                    "subprocess stdout stream requires a nonzero preferred chunk size",
                ));
            }
            if self.opened.swap(true, Ordering::AcqRel) {
                return Err(CdfError::contract(
                    "subprocess stdout stream is one-shot and cannot be reopened",
                ));
            }
            let stdout = spawn_streaming_subprocess_stdout(
                &self.command,
                &self.supervision,
                Arc::clone(&self.memory),
                self.completion.clone(),
                request.preferred_chunk_bytes,
                request.cancellation,
            )
            .await?;
            Ok(stdout)
        })
    }

    fn read_exact_range(
        &self,
        _extent: ByteExtent,
        _cancellation: RunCancellation,
    ) -> cdf_kernel::BoxFuture<'_, Result<AccountedBytes>> {
        Box::pin(async {
            Err(CdfError::contract(
                "subprocess stdout stream does not support exact byte ranges",
            ))
        })
    }
}

struct RunningSubprocessStdout {
    stdout: ChildStdout,
    child: Child,
    stderr_task: tokio::task::JoinHandle<Result<Vec<u8>>>,
    stderr_lease: MemoryLease,
    stderr_line_limit: usize,
    maximum_stdout_bytes: u64,
    stdout_bytes: u64,
    preferred_chunk_bytes: u64,
    deadline: Option<tokio::time::Instant>,
    cancellation: RunCancellation,
    memory: Arc<dyn MemoryCoordinator>,
    completion: SubprocessCompletionHandle,
}

async fn start_streaming_subprocess(
    command: &CommandSpec,
    supervision: &SupervisionOptions,
    memory: Arc<dyn MemoryCoordinator>,
    completion: SubprocessCompletionHandle,
    preferred_chunk_bytes: u64,
    cancellation: RunCancellation,
) -> Result<RunningSubprocessStdout> {
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
    let stderr_task = tokio::spawn(read_bounded(
        stderr,
        supervision.maximum_stderr_bytes,
        "stderr",
    ));
    let deadline = supervision
        .timeout
        .map(|duration| tokio::time::Instant::now() + duration);
    Ok(RunningSubprocessStdout {
        stdout,
        child,
        stderr_task,
        stderr_lease,
        stderr_line_limit: supervision.stderr_line_limit,
        maximum_stdout_bytes: supervision.maximum_stdout_bytes,
        stdout_bytes: 0,
        preferred_chunk_bytes,
        deadline,
        cancellation,
        memory,
        completion,
    })
}

async fn spawn_streaming_subprocess_stdout(
    command: &CommandSpec,
    supervision: &SupervisionOptions,
    memory: Arc<dyn MemoryCoordinator>,
    completion: SubprocessCompletionHandle,
    preferred_chunk_bytes: u64,
    cancellation: RunCancellation,
) -> Result<AccountedByteStream> {
    let mut running = start_streaming_subprocess(
        command,
        supervision,
        memory,
        completion,
        preferred_chunk_bytes,
        cancellation,
    )
    .await?;
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<AccountedBytes>>(1);
    tokio::spawn(async move {
        loop {
            match read_subprocess_stdout_chunk(&mut running).await {
                Ok(Some(chunk)) => {
                    if sender.send(Ok(chunk)).await.is_err() {
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    break;
                }
            }
        }
    });
    Ok(
        Box::pin(stream::try_unfold(receiver, |mut receiver| async move {
            match receiver.recv().await {
                Some(Ok(chunk)) => Ok(Some((chunk, receiver))),
                Some(Err(error)) => Err(error),
                None => Ok(None),
            }
        })) as AccountedByteStream,
    )
}

async fn read_subprocess_stdout_chunk(
    state: &mut RunningSubprocessStdout,
) -> Result<Option<AccountedBytes>> {
    state.cancellation.check()?;
    let remaining = state
        .maximum_stdout_bytes
        .saturating_sub(state.stdout_bytes);
    let read_window = if remaining == 0 {
        1
    } else {
        state
            .preferred_chunk_bytes
            .min(remaining.saturating_add(1))
            .max(1)
    };
    let read_window = usize::try_from(read_window)
        .map_err(|_| CdfError::data("subprocess stdout chunk boundary exceeds usize"))?;
    let lease = reserve_subprocess_stdout_chunk(&state.memory, read_window as u64).await?;
    let mut buffer = vec![0_u8; read_window];
    let read = read_with_deadline(
        &mut state.stdout,
        &mut buffer,
        state.deadline,
        state.cancellation.clone(),
    )
    .await?;
    if read == 0 {
        drop(lease);
        finalize_streaming_subprocess(state).await?;
        return Ok(None);
    }
    let read_u64 = u64::try_from(read)
        .map_err(|_| CdfError::data("subprocess stdout read length exceeds u64"))?;
    if read_u64 > remaining {
        let _ = state.child.start_kill();
        let _ = state.child.wait().await;
        abort_stderr_task(state).await;
        return Err(CdfError::data(format!(
            "subprocess stdout exceeded the {}-byte boundary",
            state.maximum_stdout_bytes
        )));
    }
    state.stdout_bytes = state
        .stdout_bytes
        .checked_add(read_u64)
        .ok_or_else(|| CdfError::data("subprocess stdout byte count overflowed"))?;
    buffer.truncate(read);
    let chunk = AccountedBytes::new(Bytes::from(buffer), lease)?;
    Ok(Some(chunk))
}

async fn reserve_subprocess_stdout_chunk(
    memory: &Arc<dyn MemoryCoordinator>,
    bytes: u64,
) -> Result<MemoryLease> {
    cdf_memory::reserve(
        Arc::clone(memory),
        ReservationRequest::new(
            ConsumerKey::new("subprocess-stdout-chunk", MemoryClass::Source)?,
            bytes.max(1),
        )?
        .as_minimum_working_set(),
    )
    .await
}

async fn abort_stderr_task(state: &mut RunningSubprocessStdout) {
    let stderr_task = std::mem::replace(
        &mut state.stderr_task,
        tokio::spawn(async { Ok(Vec::new()) }),
    );
    stderr_task.abort();
    let _ = stderr_task.await;
}

async fn read_with_deadline<R: AsyncRead + Unpin>(
    reader: &mut R,
    buffer: &mut [u8],
    deadline: Option<tokio::time::Instant>,
    cancellation: RunCancellation,
) -> Result<usize> {
    let cancelled = cancellation.cancelled();
    tokio::pin!(cancelled);
    match deadline {
        Some(deadline) => {
            let sleep = tokio::time::sleep_until(deadline);
            tokio::pin!(sleep);
            tokio::select! {
                result = reader.read(buffer) => {
                    result.map_err(|error| CdfError::internal(format!("read subprocess stdout: {error}")))
                }
                () = &mut sleep => Err(subprocess_timeout(deadline)),
                () = &mut cancelled => Err(cancellation.check().unwrap_err()),
            }
        }
        None => {
            tokio::select! {
                result = reader.read(buffer) => {
                    result.map_err(|error| CdfError::internal(format!("read subprocess stdout: {error}")))
                }
                () = &mut cancelled => Err(cancellation.check().unwrap_err()),
            }
        }
    }
}

async fn wait_with_deadline(
    child: &mut Child,
    deadline: Option<tokio::time::Instant>,
    cancellation: RunCancellation,
) -> Result<ExitStatus> {
    let cancelled = cancellation.cancelled();
    tokio::pin!(cancelled);
    match deadline {
        Some(deadline) => {
            let sleep = tokio::time::sleep_until(deadline);
            tokio::pin!(sleep);
            tokio::select! {
                result = child.wait() => {
                    result.map_err(|error| CdfError::internal(format!("wait for subprocess: {error}")))
                }
                () = &mut sleep => Err(subprocess_timeout(deadline)),
                () = &mut cancelled => Err(cancellation.check().unwrap_err()),
            }
        }
        None => {
            tokio::select! {
                result = child.wait() => {
                    result.map_err(|error| CdfError::internal(format!("wait for subprocess: {error}")))
                }
                () = &mut cancelled => Err(cancellation.check().unwrap_err()),
            }
        }
    }
}

fn subprocess_timeout(deadline: tokio::time::Instant) -> CdfError {
    let overdue = tokio::time::Instant::now().saturating_duration_since(deadline);
    CdfError::transient(format!(
        "subprocess timed out after deadline was reached (overdue {} ms)",
        overdue.as_millis()
    ))
}

async fn finalize_streaming_subprocess(state: &mut RunningSubprocessStdout) -> Result<()> {
    let exit_status =
        wait_with_deadline(&mut state.child, state.deadline, state.cancellation.clone()).await?;
    let stderr_task = std::mem::replace(
        &mut state.stderr_task,
        tokio::spawn(async { Ok(Vec::new()) }),
    );
    let stderr_bytes = join_bounded_reader(stderr_task.await, "stderr")?;
    let stderr = StderrTrace::new(
        BoundedCommandBytes::new(stderr_bytes, state.stderr_lease.clone())?,
        state.stderr_line_limit,
    );
    if !exit_status.success() {
        return Err(CdfError::new(
            ErrorKind::Transient,
            format!(
                "subprocess exited unsuccessfully: {}; stderr: {}",
                status_message(exit_status),
                stderr.summary()
            ),
        ));
    }
    state
        .completion
        .complete(SubprocessCompletion {
            stderr,
            exit_status,
        })
        .map_err(|error| CdfError::internal(error.message))?;
    Ok(())
}
