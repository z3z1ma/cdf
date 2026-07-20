use std::{
    future::Future,
    pin::Pin,
    process::{ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use cdf_foreign_stream::{
    ForeignBackpressure, ForeignBatchOutcome, ForeignCancellation, ForeignCancellationContract,
    ForeignControlEvent, ForeignControlKind, ForeignCopyClassification, ForeignDiagnosticSeverity,
    ForeignEventStream, ForeignExecutionLane, ForeignLaneCapabilities, ForeignMemoryContract,
    ForeignProducer, ForeignProducerDescriptor, ForeignProducerId, ForeignProtocolVersion,
    ForeignSecurityContract, ForeignStartupModel, ForeignStateContract, ForeignStreamEvent,
    ForeignStreamOpen, ForeignStreamOpenRequest, ForeignTerminalStatus, ForeignTransferMode,
};
use cdf_kernel::{CdfError, ErrorKind, Result};
use cdf_memory::AccountedBytes;
use cdf_memory::{ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest};
use cdf_runtime::{
    AccountedByteStream, BoundedFormatRequest, ByteExtent, ByteSource, ByteSourceCapabilities,
    ContentIdentity, DecodeSchemaPlan, GenerationStrength, MemoryByteSource, ReadOptions,
    RunCancellation, SequentialReadRequest, decode_bounded_format, decode_format_stream,
};
use futures_util::{FutureExt, StreamExt, stream};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, ChildStdout, Command},
};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

#[cfg(unix)]
use rustix::{
    io::Errno,
    process::{Pid, Signal, kill_process_group, test_kill_process_group},
};

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
    validate_supervision(&supervision)?;
    validate_process_tree_authority()?;
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
    let mut process = subprocess_command(&command);

    let mut child = process
        .spawn()
        .map_err(|error| CdfError::internal(format!("spawn subprocess: {error}")))?;
    let process_group = ChildProcessGroup::for_child(&child)?;
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
    let mut stderr_task = tokio::spawn(read_diagnostic_ring(
        stderr,
        supervision.maximum_stderr_bytes,
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
                    Ok(status) => {
                        exit_status = Some(status);
                        break None;
                    }
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
                match join_diagnostic_reader(result) {
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
        let error =
            match terminate_child_tree(&mut child, process_group, supervision.termination_grace)
                .await
            {
                Ok(()) => error,
                Err(cleanup) => with_cleanup_error(error, cleanup),
            };
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
    if let Err(error) =
        ensure_process_group_quiescent(process_group, supervision.termination_grace).await
    {
        stdout_task.abort();
        stderr_task.abort();
        let _ = stdout_task.await;
        let _ = stderr_task.await;
        return Err(error);
    }
    if !stdout_done {
        stdout = Some(join_bounded_reader(stdout_task.await, "stdout")?);
    }
    if !stderr_done {
        stderr = Some(join_diagnostic_reader(stderr_task.await)?);
    }
    let stdout = BoundedCommandBytes::new(
        stdout.expect("completed subprocess captured stdout"),
        stdout_lease,
    )?;
    let stderr = redact_diagnostic_capture(
        stderr.expect("completed subprocess captured stderr"),
        &command,
        supervision.maximum_stderr_bytes,
    );
    let stderr_bytes = BoundedCommandBytes::new(stderr.bytes, stderr_lease)?;
    Ok(BoundedCommandOutput {
        stdout,
        stderr: StderrTrace::new(
            stderr_bytes,
            supervision.stderr_line_limit,
            stderr.discarded_bytes,
        ),
        exit_status: exit_status.expect("completed subprocess captured exit status"),
    })
}

fn subprocess_command(command: &CommandSpec) -> Command {
    let mut process = Command::new(&command.program);
    process
        .args(&command.args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    {
        process.as_std_mut().process_group(0);
    }
    if let Some(current_dir) = &command.current_dir {
        process.current_dir(current_dir);
    }
    for (key, value) in &command.env {
        process.env(key, value);
    }
    process
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

#[derive(Debug)]
struct DiagnosticCapture {
    bytes: Vec<u8>,
    discarded_bytes: u64,
}

async fn read_diagnostic_ring<R>(mut reader: R, maximum_bytes: u64) -> Result<DiagnosticCapture>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let retained_capacity = usize::try_from(maximum_bytes)
        .map_err(|_| CdfError::data("subprocess stderr boundary exceeds usize"))?;
    let mut bytes = Vec::with_capacity(retained_capacity.min(64 * 1024));
    let mut discarded_bytes = 0_u64;
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|error| CdfError::internal(format!("read subprocess stderr: {error}")))?;
        if read == 0 {
            break;
        }
        let retained = retained_capacity.saturating_sub(bytes.len()).min(read);
        bytes.extend_from_slice(&buffer[..retained]);
        discarded_bytes =
            discarded_bytes.saturating_add(u64::try_from(read - retained).unwrap_or(u64::MAX));
    }
    Ok(DiagnosticCapture {
        bytes,
        discarded_bytes,
    })
}

fn redact_diagnostic_capture(
    capture: DiagnosticCapture,
    command: &CommandSpec,
    maximum_bytes: u64,
) -> DiagnosticCapture {
    let mut bytes = capture.bytes;
    for secret in command
        .env
        .values()
        .filter(|value| !value.is_empty())
        .map(String::as_bytes)
    {
        bytes = replace_bytes(&bytes, secret, b"<redacted>");
        if capture.discarded_bytes > 0 {
            let partial = (1..secret.len())
                .rev()
                .find(|length| bytes.ends_with(&secret[..*length]));
            if let Some(partial) = partial {
                bytes.truncate(bytes.len() - partial);
                bytes.extend_from_slice(b"<redacted>");
            }
        }
    }
    let maximum = usize::try_from(maximum_bytes).unwrap_or(usize::MAX);
    let redaction_overflow = bytes.len().saturating_sub(maximum);
    bytes.truncate(maximum);
    DiagnosticCapture {
        bytes,
        discarded_bytes: capture
            .discarded_bytes
            .saturating_add(u64::try_from(redaction_overflow).unwrap_or(u64::MAX)),
    }
}

fn replace_bytes(input: &[u8], needle: &[u8], replacement: &[u8]) -> Vec<u8> {
    debug_assert!(!needle.is_empty());
    let mut output = Vec::with_capacity(input.len());
    let mut offset = 0;
    while let Some(relative) = input[offset..]
        .windows(needle.len())
        .position(|candidate| candidate == needle)
    {
        let start = offset + relative;
        output.extend_from_slice(&input[offset..start]);
        output.extend_from_slice(replacement);
        offset = start + needle.len();
    }
    output.extend_from_slice(&input[offset..]);
    output
}

fn join_diagnostic_reader(
    result: std::result::Result<Result<DiagnosticCapture>, tokio::task::JoinError>,
) -> Result<DiagnosticCapture> {
    result
        .map_err(|error| CdfError::internal(format!("subprocess stderr reader failed: {error}")))?
}

pub async fn run_bounded_stdout_adapter(
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

fn validate_supervision(supervision: &SupervisionOptions) -> Result<()> {
    if supervision.maximum_stdout_bytes == 0
        || supervision.maximum_stderr_bytes == 0
        || supervision.maximum_stream_chunk_bytes == 0
        || supervision.maximum_stdout_bytes == u64::MAX
        || supervision.maximum_stderr_bytes == u64::MAX
        || supervision.maximum_stream_chunk_bytes == u64::MAX
    {
        return Err(CdfError::contract(
            "subprocess collected stdout, stream chunk, and stderr byte boundaries must be within 1..u64::MAX",
        ));
    }
    if supervision.maximum_streamed_stdout_bytes == Some(0) {
        return Err(CdfError::contract(
            "subprocess total streamed stdout boundary must be greater than zero when configured",
        ));
    }
    if supervision.stderr_line_limit == 0 {
        return Err(CdfError::contract(
            "subprocess stderr line boundary must be greater than zero",
        ));
    }
    if supervision.termination_grace.is_zero() {
        return Err(CdfError::contract(
            "subprocess termination grace period must be greater than zero",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_process_tree_authority() -> Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn validate_process_tree_authority() -> Result<()> {
    Err(CdfError::contract(
        "subprocess adapters require process-tree termination authority; this platform is unsupported until a native job/process-tree backend is available",
    ))
}

#[derive(Clone, Debug)]
struct TerminalDiagnostic {
    summary: String,
    truncated: bool,
    discarded_bytes: u64,
}

#[derive(Clone, Debug)]
enum SubprocessTerminal {
    Succeeded {
        diagnostic: Option<TerminalDiagnostic>,
    },
    Failed(CdfError),
    Cancelled,
}

enum SubprocessLifecyclePhase {
    NotStarted,
    Running,
    Complete(SubprocessTerminal),
}

struct SubprocessLifecycleInner {
    phase: Mutex<SubprocessLifecyclePhase>,
    worker_join: Mutex<Option<tokio::sync::oneshot::Receiver<Result<()>>>>,
    notify: tokio::sync::Notify,
    run_cancellation: RunCancellation,
    foreign_cancellation: ForeignCancellation,
}

#[derive(Clone)]
struct SubprocessLifecycle(Arc<SubprocessLifecycleInner>);

impl SubprocessLifecycle {
    fn new(foreign_cancellation: ForeignCancellation) -> Self {
        Self(Arc::new(SubprocessLifecycleInner {
            phase: Mutex::new(SubprocessLifecyclePhase::NotStarted),
            worker_join: Mutex::new(None),
            notify: tokio::sync::Notify::new(),
            run_cancellation: RunCancellation::default(),
            foreign_cancellation,
        }))
    }

    fn run_cancellation(&self) -> RunCancellation {
        self.0.run_cancellation.clone()
    }

    fn mark_started(&self) -> Result<()> {
        let mut phase = self.0.phase.lock().unwrap();
        match &*phase {
            SubprocessLifecyclePhase::NotStarted => {
                *phase = SubprocessLifecyclePhase::Running;
                Ok(())
            }
            SubprocessLifecyclePhase::Running => Err(CdfError::internal(
                "subprocess lifecycle was started more than once",
            )),
            SubprocessLifecyclePhase::Complete(_) => Err(CdfError::transient(
                "subprocess invocation was cancelled before startup",
            )),
        }
    }

    fn attach_worker(&self, receiver: tokio::sync::oneshot::Receiver<Result<()>>) -> Result<()> {
        let mut worker_join = self.0.worker_join.lock().unwrap();
        if worker_join.replace(receiver).is_some() {
            return Err(CdfError::internal(
                "subprocess lifecycle attached more than one producer task",
            ));
        }
        Ok(())
    }

    fn complete(&self, terminal: SubprocessTerminal) {
        let mut phase = self.0.phase.lock().unwrap();
        if matches!(*phase, SubprocessLifecyclePhase::Complete(_)) {
            return;
        }
        *phase = SubprocessLifecyclePhase::Complete(terminal);
        drop(phase);
        self.0.notify.notify_waiters();
    }

    fn cancel(&self) {
        self.0.run_cancellation.cancel();
        self.0.foreign_cancellation.cancel();
        let mut phase = self.0.phase.lock().unwrap();
        if matches!(*phase, SubprocessLifecyclePhase::NotStarted) {
            *phase = SubprocessLifecyclePhase::Complete(SubprocessTerminal::Cancelled);
            drop(phase);
            self.0.notify.notify_waiters();
        }
    }

    fn is_cancelled(&self) -> bool {
        self.0.run_cancellation.is_cancelled() || self.0.foreign_cancellation.is_cancelled()
    }

    async fn terminal(&self) -> SubprocessTerminal {
        loop {
            let notified = self.0.notify.notified();
            if let SubprocessLifecyclePhase::Complete(terminal) = &*self.0.phase.lock().unwrap() {
                return terminal.clone();
            }
            notified.await;
        }
    }

    async fn join(&self) -> Result<()> {
        let worker = self.0.worker_join.lock().unwrap().take();
        if let Some(worker) = worker {
            worker.await.map_err(|_| {
                CdfError::internal("subprocess producer task terminated without cleanup evidence")
            })??;
        }
        self.terminal().await;
        Ok(())
    }
}

#[derive(Clone)]
pub struct SubprocessProducer {
    command: CommandSpec,
    stdout_format: StdoutFormat,
    read_options: ReadOptions,
    schema: DecodeSchemaPlan,
    supervision: SupervisionOptions,
    memory: Arc<dyn MemoryCoordinator>,
    descriptor: ForeignProducerDescriptor,
}

impl SubprocessProducer {
    pub fn new(
        command: CommandSpec,
        stdout_format: StdoutFormat,
        read_options: ReadOptions,
        schema: DecodeSchemaPlan,
        supervision: SupervisionOptions,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Result<Self> {
        validate_supervision(&supervision)?;
        validate_process_tree_authority()?;
        let transfer_mode = transfer_mode(stdout_format);
        let descriptor = ForeignProducerDescriptor {
            producer_id: ForeignProducerId::new("cdf-subprocess")?,
            protocol_version: ForeignProtocolVersion::new("1")?,
            transfer_modes: vec![transfer_mode],
            startup: ForeignStartupModel::ChildProcess,
            lanes: ForeignLaneCapabilities {
                execution_lane: ForeignExecutionLane::IsolatedProcess,
                maximum_internal_parallelism: 1,
                backpressure: ForeignBackpressure::Pipe,
            },
            memory: ForeignMemoryContract {
                payload_window_bytes: None,
                control_queue_bytes: None,
                diagnostic_queue_bytes: Some(supervision.maximum_stderr_bytes),
                native_scratch_bytes: None,
                child_process_bytes: None,
            },
            cancellation: ForeignCancellationContract {
                cooperative_stop: true,
                interrupt_safe: true,
                force_termination_authorized: true,
                drains_on_cancel: true,
            },
            state: ForeignStateContract {
                emits_positions: false,
                emits_watermarks: false,
                emits_foreign_state: false,
                terminal_state_required: true,
            },
            security: ForeignSecurityContract {
                ambient_network: true,
                ambient_filesystem: true,
                secret_names: command.env.keys().cloned().collect(),
            },
        };
        descriptor.validate()?;
        cdf_kernel::canonical_arrow_schema_hash(schema.authority_schema.as_ref())?;
        Ok(Self {
            command,
            stdout_format,
            read_options,
            schema,
            supervision,
            memory,
            descriptor,
        })
    }
}

impl ForeignProducer for SubprocessProducer {
    fn descriptor(&self) -> &ForeignProducerDescriptor {
        &self.descriptor
    }

    fn open(
        &self,
        request: ForeignStreamOpenRequest,
    ) -> cdf_kernel::BoxFuture<'_, Result<ForeignStreamOpen>> {
        Box::pin(async move {
            if request.resource_id != self.read_options.resource_id
                || request.partition_id != self.read_options.partition_id
            {
                return Err(CdfError::contract(
                    "subprocess foreign stream request does not match its compiled resource partition",
                ));
            }
            request.cancellation.check()?;
            let lifecycle = SubprocessLifecycle::new(request.cancellation.clone());
            let driver: Arc<dyn cdf_runtime::FormatDriver> = match self.stdout_format {
                StdoutFormat::ArrowIpc => {
                    Arc::new(cdf_format_arrow_ipc::ArrowIpcStreamFormatDriver::new()?)
                }
                StdoutFormat::Ndjson => Arc::new(cdf_format_json::NdjsonFormatDriver::new()?),
            };
            let source: Arc<dyn ByteSource> = Arc::new(SubprocessStdoutByteSource::new(
                self.command.clone(),
                self.supervision.clone(),
                Arc::clone(&self.memory),
                lifecycle.clone(),
            )?);
            let stream = decode_format_stream(
                driver,
                source,
                BoundedFormatRequest::new(self.read_options.clone(), Arc::clone(&self.memory))
                    .with_schema(self.schema.clone())
                    .with_cancellation(lifecycle.run_cancellation()),
            )
            .await
            .map_err(|error| CdfError {
                kind: error.kind,
                message: format!(
                    "stream subprocess {:?} stdout: {}",
                    self.stdout_format, error.message
                ),
                retry_after_ms: error.retry_after_ms,
            })?;
            let events = subprocess_foreign_events(
                stream.batches,
                transfer_mode(self.stdout_format),
                lifecycle.clone(),
            );
            let cancellation_lifecycle = lifecycle.clone();
            let joined_lifecycle = lifecycle.clone();
            let termination = cdf_kernel::InvocationTermination::new(
                move || cancellation_lifecycle.cancel(),
                Box::pin(async move { joined_lifecycle.join().await }),
            );
            let bridge_lifecycle = lifecycle.clone();
            tokio::spawn(async move {
                let terminal = bridge_lifecycle.terminal();
                let cancelled = request.cancellation.cancelled();
                tokio::pin!(terminal, cancelled);
                tokio::select! {
                    _ = &mut terminal => {}
                    () = &mut cancelled => bridge_lifecycle.cancel(),
                }
            });
            Ok(ForeignStreamOpen {
                descriptor: self.descriptor.clone(),
                events,
                termination,
            })
        })
    }
}

fn transfer_mode(format: StdoutFormat) -> ForeignTransferMode {
    match format {
        StdoutFormat::ArrowIpc => ForeignTransferMode::ArrowIpcStream,
        StdoutFormat::Ndjson => ForeignTransferMode::RowCompat,
    }
}

struct SubprocessForeignEventState {
    batches: cdf_runtime::FormatBatchStream,
    transfer_mode: ForeignTransferMode,
    lifecycle: SubprocessLifecycle,
    next_sequence: u64,
    pending_terminal: Option<ForeignTerminalStatus>,
    finished: bool,
}

impl Drop for SubprocessForeignEventState {
    fn drop(&mut self) {
        if !self.finished {
            self.lifecycle.cancel();
        }
    }
}

fn subprocess_foreign_events(
    batches: cdf_runtime::FormatBatchStream,
    transfer_mode: ForeignTransferMode,
    lifecycle: SubprocessLifecycle,
) -> ForeignEventStream {
    Box::pin(stream::unfold(
        SubprocessForeignEventState {
            batches,
            transfer_mode,
            lifecycle,
            next_sequence: 1,
            pending_terminal: None,
            finished: false,
        },
        subprocess_foreign_event_next,
    ))
}

async fn subprocess_foreign_event_next(
    mut state: SubprocessForeignEventState,
) -> Option<(Result<ForeignStreamEvent>, SubprocessForeignEventState)> {
    if state.finished {
        return None;
    }
    if let Some(terminal) = state.pending_terminal.take() {
        state.finished = true;
        return Some((Ok(ForeignStreamEvent::Terminal(terminal)), state));
    }
    match state.batches.next().await {
        Some(Ok(batch)) => {
            let copy = ForeignCopyClassification::CopyUnknown;
            let sequence = state.next_sequence;
            state.next_sequence = match state.next_sequence.checked_add(1) {
                Some(sequence) => sequence,
                None => {
                    return Some((
                        Err(CdfError::data(
                            "subprocess foreign event sequence overflowed",
                        )),
                        state,
                    ));
                }
            };
            let outcome = ForeignBatchOutcome::new(sequence, batch, state.transfer_mode, copy);
            Some((outcome.map(ForeignStreamEvent::Outcome), state))
        }
        Some(Err(error)) => {
            let externally_cancelled = state.lifecycle.is_cancelled();
            state.lifecycle.cancel();
            let subprocess_terminal = state.lifecycle.terminal().await;
            state.finished = true;
            let terminal = match subprocess_terminal {
                SubprocessTerminal::Cancelled if externally_cancelled => {
                    ForeignTerminalStatus::Cancelled
                }
                SubprocessTerminal::Cancelled | SubprocessTerminal::Succeeded { .. } => {
                    ForeignTerminalStatus::Failed {
                        retryable: matches!(
                            error.kind,
                            ErrorKind::Transient | ErrorKind::RateLimited
                        ),
                        message: error.message,
                    }
                }
                SubprocessTerminal::Failed(process_error) => ForeignTerminalStatus::Failed {
                    retryable: matches!(
                        process_error.kind,
                        ErrorKind::Transient | ErrorKind::RateLimited
                    ),
                    message: process_error.message,
                },
            };
            Some((Ok(ForeignStreamEvent::Terminal(terminal)), state))
        }
        None => match state.lifecycle.terminal().await {
            SubprocessTerminal::Succeeded { diagnostic } => {
                state.pending_terminal = Some(ForeignTerminalStatus::Succeeded {
                    final_position: None,
                });
                if let Some(diagnostic) = diagnostic {
                    let sequence = state.next_sequence;
                    state.next_sequence = match state.next_sequence.checked_add(1) {
                        Some(sequence) => sequence,
                        None => {
                            return Some((
                                Err(CdfError::data(
                                    "subprocess foreign event sequence overflowed",
                                )),
                                state,
                            ));
                        }
                    };
                    let suffix = if diagnostic.truncated {
                        format!(
                            " ({} diagnostic bytes discarded)",
                            diagnostic.discarded_bytes
                        )
                    } else {
                        String::new()
                    };
                    let control = ForeignControlEvent::new(
                        sequence,
                        ForeignControlKind::Diagnostic {
                            severity: ForeignDiagnosticSeverity::Info,
                            message: format!("{}{}", diagnostic.summary, suffix),
                        },
                    );
                    Some((control.map(ForeignStreamEvent::Control), state))
                } else {
                    state.finished = true;
                    Some((
                        Ok(ForeignStreamEvent::Terminal(
                            state.pending_terminal.take().expect("terminal was set"),
                        )),
                        state,
                    ))
                }
            }
            SubprocessTerminal::Failed(error) => {
                state.finished = true;
                Some((
                    Ok(ForeignStreamEvent::Terminal(
                        ForeignTerminalStatus::Failed {
                            retryable: matches!(
                                error.kind,
                                ErrorKind::Transient | ErrorKind::RateLimited
                            ),
                            message: error.message,
                        },
                    )),
                    state,
                ))
            }
            SubprocessTerminal::Cancelled => {
                state.finished = true;
                Some((
                    Ok(ForeignStreamEvent::Terminal(
                        ForeignTerminalStatus::Cancelled,
                    )),
                    state,
                ))
            }
        },
    }
}

#[derive(Clone, Copy)]
struct ChildProcessGroup {
    #[cfg(unix)]
    id: Pid,
}

impl ChildProcessGroup {
    fn for_child(child: &Child) -> Result<Self> {
        #[cfg(unix)]
        {
            let raw = i32::try_from(child.id().ok_or_else(|| {
                CdfError::internal("spawned subprocess did not expose a process id")
            })?)
            .map_err(|_| CdfError::internal("subprocess process id exceeds i32"))?;
            let id = Pid::from_raw(raw)
                .ok_or_else(|| CdfError::internal("subprocess process id cannot be zero"))?;
            Ok(Self { id })
        }
        #[cfg(not(unix))]
        {
            let _ = child;
            Ok(Self {})
        }
    }
}

#[cfg(unix)]
fn process_group_exists(group: ChildProcessGroup) -> Result<bool> {
    match test_kill_process_group(group.id) {
        Ok(()) => Ok(true),
        Err(Errno::SRCH) => Ok(false),
        Err(error) => Err(CdfError::internal(format!(
            "inspect subprocess process group {}: {error}",
            group.id.as_raw_nonzero()
        ))),
    }
}

#[cfg(not(unix))]
fn process_group_exists(_group: ChildProcessGroup) -> Result<bool> {
    Err(CdfError::contract(
        "subprocess process-tree supervision is unsupported on this platform",
    ))
}

#[cfg(unix)]
fn signal_process_group(group: ChildProcessGroup, signal: Signal) -> Result<()> {
    match kill_process_group(group.id, signal) {
        Ok(()) | Err(Errno::SRCH) => Ok(()),
        Err(error) => Err(CdfError::internal(format!(
            "signal subprocess process group {} with {signal:?}: {error}",
            group.id.as_raw_nonzero()
        ))),
    }
}

async fn ensure_process_group_quiescent(
    group: ChildProcessGroup,
    grace: std::time::Duration,
) -> Result<()> {
    if !process_group_exists(group)? {
        return Ok(());
    }
    #[cfg(unix)]
    {
        signal_process_group(group, Signal::TERM)?;
        if wait_for_process_group_exit(group, grace).await? {
            return Ok(());
        }
        signal_process_group(group, Signal::KILL)?;
        if wait_for_process_group_exit(group, grace).await? {
            return Ok(());
        }
        Err(CdfError::internal(format!(
            "subprocess process group {} survived forced termination",
            group.id.as_raw_nonzero()
        )))
    }
    #[cfg(not(unix))]
    {
        let _ = grace;
        unreachable!("unsupported process-tree authority returned a group")
    }
}

async fn terminate_child_tree(
    child: &mut Child,
    group: ChildProcessGroup,
    grace: std::time::Duration,
) -> Result<()> {
    #[cfg(unix)]
    {
        signal_process_group(group, Signal::TERM)?;
    }
    #[cfg(not(unix))]
    {
        let _ = child.start_kill();
    }
    let child_exited = match tokio::time::timeout(grace, child.wait()).await {
        Ok(Ok(_)) => true,
        Ok(Err(error)) => {
            return Err(CdfError::internal(format!(
                "wait for terminated subprocess: {error}"
            )));
        }
        Err(_) => false,
    };
    let group_exited = wait_for_process_group_exit(group, grace).await?;
    if !child_exited || !group_exited {
        #[cfg(unix)]
        {
            signal_process_group(group, Signal::KILL)?;
        }
        if !child_exited {
            child.start_kill().map_err(|error| {
                CdfError::internal(format!("force terminate subprocess: {error}"))
            })?;
            child.wait().await.map_err(|error| {
                CdfError::internal(format!("wait for force-terminated subprocess: {error}"))
            })?;
        }
        if !wait_for_process_group_exit(group, grace).await? {
            return Err(CdfError::internal(
                "subprocess process group survived force termination",
            ));
        }
    }
    Ok(())
}

async fn wait_for_process_group_exit(
    group: ChildProcessGroup,
    grace: std::time::Duration,
) -> Result<bool> {
    let deadline = tokio::time::Instant::now() + grace;
    while process_group_exists(group)? {
        if tokio::time::Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(grace.min(std::time::Duration::from_millis(10))).await;
    }
    Ok(true)
}

fn with_cleanup_error(mut primary: CdfError, cleanup: CdfError) -> CdfError {
    primary.message = format!(
        "{}; subprocess process-tree cleanup also failed: {}",
        primary.message, cleanup.message
    );
    primary
}

struct SubprocessStdoutByteSource {
    command: CommandSpec,
    supervision: SupervisionOptions,
    memory: Arc<dyn MemoryCoordinator>,
    identity: ContentIdentity,
    capabilities: ByteSourceCapabilities,
    opened: AtomicBool,
    lifecycle: SubprocessLifecycle,
}

impl SubprocessStdoutByteSource {
    fn new(
        command: CommandSpec,
        supervision: SupervisionOptions,
        memory: Arc<dyn MemoryCoordinator>,
        lifecycle: SubprocessLifecycle,
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
            maximum_chunk_bytes: supervision.maximum_stream_chunk_bytes,
        };
        capabilities.validate()?;
        Ok(Self {
            command,
            supervision,
            memory,
            identity,
            capabilities,
            opened: AtomicBool::new(false),
            lifecycle,
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

    fn maximum_sequential_bytes(&self) -> Option<u64> {
        self.supervision.maximum_streamed_stdout_bytes
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
                self.lifecycle.clone(),
                request.preferred_chunk_bytes,
                request.cancellation,
            )
            .await;
            if let Err(error) = &stdout {
                self.lifecycle
                    .complete(SubprocessTerminal::Failed(error.clone()));
            }
            stdout
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
    stderr_task: tokio::task::JoinHandle<Result<DiagnosticCapture>>,
    stderr_lease: MemoryLease,
    stderr_line_limit: usize,
    maximum_stdout_bytes: Option<u64>,
    stdout_bytes: u64,
    preferred_chunk_bytes: u64,
    deadline: Option<tokio::time::Instant>,
    cancellation: RunCancellation,
    memory: Arc<dyn MemoryCoordinator>,
    lifecycle: SubprocessLifecycle,
    process_group: ChildProcessGroup,
    termination_grace: std::time::Duration,
    command: CommandSpec,
    exit_status: Option<ExitStatus>,
}

async fn start_streaming_subprocess(
    command: &CommandSpec,
    supervision: &SupervisionOptions,
    memory: Arc<dyn MemoryCoordinator>,
    lifecycle: SubprocessLifecycle,
    preferred_chunk_bytes: u64,
    cancellation: RunCancellation,
) -> Result<RunningSubprocessStdout> {
    let stderr_lease = reserve_output_capacity(
        &memory,
        "subprocess-stderr",
        supervision.maximum_stderr_bytes,
    )?;
    let mut process = subprocess_command(command);
    let mut child = process
        .spawn()
        .map_err(|error| CdfError::internal(format!("spawn subprocess: {error}")))?;
    let process_group = ChildProcessGroup::for_child(&child)?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| CdfError::internal("subprocess stdout pipe was not created"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| CdfError::internal("subprocess stderr pipe was not created"))?;
    let stderr_task = tokio::spawn(read_diagnostic_ring(
        stderr,
        supervision.maximum_stderr_bytes,
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
        maximum_stdout_bytes: supervision.maximum_streamed_stdout_bytes,
        stdout_bytes: 0,
        preferred_chunk_bytes,
        deadline,
        cancellation,
        memory,
        lifecycle,
        process_group,
        termination_grace: supervision.termination_grace,
        command: command.clone(),
        exit_status: None,
    })
}

async fn spawn_streaming_subprocess_stdout(
    command: &CommandSpec,
    supervision: &SupervisionOptions,
    memory: Arc<dyn MemoryCoordinator>,
    lifecycle: SubprocessLifecycle,
    preferred_chunk_bytes: u64,
    cancellation: RunCancellation,
) -> Result<AccountedByteStream> {
    let (worker_done, worker_join) = tokio::sync::oneshot::channel();
    lifecycle.attach_worker(worker_join)?;
    lifecycle.mark_started()?;
    let running = start_streaming_subprocess(
        command,
        supervision,
        memory,
        lifecycle.clone(),
        preferred_chunk_bytes,
        cancellation,
    )
    .await;
    let mut running = match running {
        Ok(running) => running,
        Err(error) => {
            let _ = worker_done.send(Ok(()));
            return Err(error);
        }
    };
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<AccountedBytes>>(1);
    let worker_lifecycle = lifecycle;
    tokio::spawn(async move {
        let work = std::panic::AssertUnwindSafe(async {
            loop {
                match read_subprocess_stdout_chunk(&mut running).await {
                    Ok(SubprocessStdoutRead::Chunk(chunk)) => {
                        if sender.send(Ok(chunk)).await.is_err() {
                            running.lifecycle.cancel();
                            let cleanup = terminate_child_tree(
                                &mut running.child,
                                running.process_group,
                                running.termination_grace,
                            )
                            .await;
                            abort_stderr_task(&mut running).await;
                            return match cleanup {
                                Ok(()) => SubprocessTerminal::Cancelled,
                                Err(error) => SubprocessTerminal::Failed(error),
                            };
                        }
                    }
                    Ok(SubprocessStdoutRead::Complete(diagnostic)) => {
                        return SubprocessTerminal::Succeeded { diagnostic };
                    }
                    Err(error) => {
                        let cleanup = terminate_child_tree(
                            &mut running.child,
                            running.process_group,
                            running.termination_grace,
                        )
                        .await;
                        abort_stderr_task(&mut running).await;
                        let error = match cleanup {
                            Ok(()) => error,
                            Err(cleanup) => with_cleanup_error(error, cleanup),
                        };
                        let terminal = if running.lifecycle.is_cancelled() {
                            SubprocessTerminal::Cancelled
                        } else {
                            SubprocessTerminal::Failed(error.clone())
                        };
                        let _ = sender.send(Err(error)).await;
                        return terminal;
                    }
                }
            }
        })
        .catch_unwind()
        .await;
        let terminal = match work {
            Ok(terminal) => terminal,
            Err(_) => {
                let primary = CdfError::internal("subprocess producer task panicked");
                let cleanup = terminate_child_tree(
                    &mut running.child,
                    running.process_group,
                    running.termination_grace,
                )
                .await;
                abort_stderr_task(&mut running).await;
                SubprocessTerminal::Failed(match cleanup {
                    Ok(()) => primary,
                    Err(cleanup) => with_cleanup_error(primary, cleanup),
                })
            }
        };
        drop(running);
        worker_lifecycle.complete(terminal);
        let _ = worker_done.send(Ok(()));
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

enum SubprocessStdoutRead {
    Chunk(AccountedBytes),
    Complete(Option<TerminalDiagnostic>),
}

async fn read_subprocess_stdout_chunk(
    state: &mut RunningSubprocessStdout,
) -> Result<SubprocessStdoutRead> {
    state.cancellation.check()?;
    let remaining = state
        .maximum_stdout_bytes
        .map(|maximum| maximum.saturating_sub(state.stdout_bytes));
    let read_window = remaining.map_or(state.preferred_chunk_bytes, |remaining| {
        if remaining == 0 {
            1
        } else {
            state.preferred_chunk_bytes.min(remaining.saturating_add(1))
        }
    });
    let read_window = read_window.max(1);
    let read_window = usize::try_from(read_window)
        .map_err(|_| CdfError::data("subprocess stdout chunk boundary exceeds usize"))?;
    let lease = reserve_subprocess_stdout_chunk(&state.memory, read_window as u64).await?;
    let mut buffer = vec![0_u8; read_window];
    let read = read_stdout_or_observe_child_exit(state, &mut buffer).await?;
    if read == 0 {
        drop(lease);
        let diagnostic = finalize_streaming_subprocess(state).await?;
        return Ok(SubprocessStdoutRead::Complete(diagnostic));
    }
    let read_u64 = u64::try_from(read)
        .map_err(|_| CdfError::data("subprocess stdout read length exceeds u64"))?;
    if remaining.is_some_and(|remaining| read_u64 > remaining) {
        return Err(CdfError::data(format!(
            "subprocess stdout exceeded the {}-byte total-transfer policy",
            state.maximum_stdout_bytes.expect("checked maximum")
        )));
    }
    state.stdout_bytes = state
        .stdout_bytes
        .checked_add(read_u64)
        .ok_or_else(|| CdfError::data("subprocess stdout byte count overflowed"))?;
    buffer.truncate(read);
    let chunk = AccountedBytes::new(Bytes::from(buffer), lease)?;
    Ok(SubprocessStdoutRead::Chunk(chunk))
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
        tokio::spawn(async {
            Ok(DiagnosticCapture {
                bytes: Vec::new(),
                discarded_bytes: 0,
            })
        }),
    );
    stderr_task.abort();
    let _ = stderr_task.await;
}

async fn read_stdout_or_observe_child_exit(
    state: &mut RunningSubprocessStdout,
    buffer: &mut [u8],
) -> Result<usize> {
    loop {
        if state.exit_status.is_some() {
            return read_with_deadline(
                &mut state.stdout,
                buffer,
                state.deadline,
                state.cancellation.clone(),
            )
            .await;
        }
        let cancelled = state.cancellation.cancelled();
        tokio::pin!(cancelled);
        match state.deadline {
            Some(deadline) => {
                let sleep = tokio::time::sleep_until(deadline);
                tokio::pin!(sleep);
                tokio::select! {
                    result = state.stdout.read(buffer) => {
                        return result.map_err(|error| CdfError::internal(format!("read subprocess stdout: {error}")));
                    }
                    result = state.child.wait() => {
                        state.exit_status = Some(result.map_err(|error| CdfError::internal(format!("wait for subprocess: {error}")))?);
                        ensure_process_group_quiescent(state.process_group, state.termination_grace).await?;
                    }
                    () = &mut sleep => return Err(subprocess_timeout(deadline)),
                    () = &mut cancelled => return Err(state.cancellation.check().unwrap_err()),
                }
            }
            None => {
                tokio::select! {
                    result = state.stdout.read(buffer) => {
                        return result.map_err(|error| CdfError::internal(format!("read subprocess stdout: {error}")));
                    }
                    result = state.child.wait() => {
                        state.exit_status = Some(result.map_err(|error| CdfError::internal(format!("wait for subprocess: {error}")))?);
                        ensure_process_group_quiescent(state.process_group, state.termination_grace).await?;
                    }
                    () = &mut cancelled => return Err(state.cancellation.check().unwrap_err()),
                }
            }
        }
    }
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

async fn finalize_streaming_subprocess(
    state: &mut RunningSubprocessStdout,
) -> Result<Option<TerminalDiagnostic>> {
    let exit_status = match state.exit_status.take() {
        Some(status) => status,
        None => {
            wait_with_deadline(&mut state.child, state.deadline, state.cancellation.clone()).await?
        }
    };
    ensure_process_group_quiescent(state.process_group, state.termination_grace).await?;
    let stderr_task = std::mem::replace(
        &mut state.stderr_task,
        tokio::spawn(async {
            Ok(DiagnosticCapture {
                bytes: Vec::new(),
                discarded_bytes: 0,
            })
        }),
    );
    let stderr_capture = redact_diagnostic_capture(
        join_diagnostic_reader(stderr_task.await)?,
        &state.command,
        state.stderr_lease.bytes().saturating_sub(1),
    );
    let stderr = StderrTrace::new(
        BoundedCommandBytes::new(stderr_capture.bytes, state.stderr_lease.clone())?,
        state.stderr_line_limit,
        stderr_capture.discarded_bytes,
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
    let summary = stderr.summary();
    if summary == "<empty>" {
        Ok(None)
    } else {
        Ok(Some(TerminalDiagnostic {
            summary,
            truncated: stderr.is_truncated(),
            discarded_bytes: stderr.discarded_bytes(),
        }))
    }
}
