#![doc = "Executor-neutral foreign producer stream contract for cdf."]

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

use cdf_kernel::{
    Batch, BatchStream, CdfError, InvocationTermination, PartitionId, ResourceId, Result,
    SourcePosition, WatermarkClaim,
};
use futures_core::Stream;
use serde::{Deserialize, Serialize};

pub type ForeignEventStream =
    Pin<Box<dyn Stream<Item = Result<ForeignStreamEvent>> + Send + 'static>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignProducerDescriptor {
    pub producer_id: ForeignProducerId,
    pub protocol_version: ForeignProtocolVersion,
    pub transfer_modes: Vec<ForeignTransferMode>,
    pub startup: ForeignStartupModel,
    pub lanes: ForeignLaneCapabilities,
    pub memory: ForeignMemoryContract,
    pub cancellation: ForeignCancellationContract,
    pub state: ForeignStateContract,
    pub security: ForeignSecurityContract,
}

impl ForeignProducerDescriptor {
    pub fn validate(&self) -> Result<()> {
        self.producer_id.validate()?;
        self.protocol_version.validate()?;
        if self.transfer_modes.is_empty() {
            return Err(CdfError::contract(
                "foreign producer descriptor requires at least one transfer mode",
            ));
        }
        if self.lanes.maximum_internal_parallelism == 0 {
            return Err(CdfError::contract(
                "foreign producer lane parallelism must be greater than zero",
            ));
        }
        self.memory.validate()?;
        self.security.validate()
    }

    pub fn supports_transfer_mode(&self, mode: ForeignTransferMode) -> bool {
        self.transfer_modes.contains(&mode)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ForeignProducerId(String);

impl ForeignProducerId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let id = Self(value.into());
        id.validate()?;
        Ok(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<()> {
        validate_token("foreign producer id", &self.0, 128)
    }
}

impl fmt::Display for ForeignProducerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ForeignProtocolVersion(String);

impl ForeignProtocolVersion {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let version = Self(value.into());
        version.validate()?;
        Ok(version)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<()> {
        validate_token("foreign protocol version", &self.0, 64)
    }
}

impl fmt::Display for ForeignProtocolVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignTransferMode {
    ArrowCData,
    ArrowIpcStream,
    RowCompat,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignStartupModel {
    InProcessAttached,
    ChildProcess,
    Sandbox,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignLaneCapabilities {
    pub execution_lane: ForeignExecutionLane,
    pub maximum_internal_parallelism: u16,
    pub backpressure: ForeignBackpressure,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignExecutionLane {
    Cpu,
    Blocking,
    IsolatedProcess,
    Sandbox,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignBackpressure {
    Pull,
    Pipe,
    HostWindow,
    UnsupportedBounded,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignMemoryContract {
    pub payload_window_bytes: Option<u64>,
    pub control_queue_bytes: Option<u64>,
    pub diagnostic_queue_bytes: Option<u64>,
    pub native_scratch_bytes: Option<u64>,
    pub child_process_bytes: Option<u64>,
}

impl ForeignMemoryContract {
    pub fn validate(&self) -> Result<()> {
        validate_optional_positive("payload window bytes", self.payload_window_bytes)?;
        validate_optional_positive("control queue bytes", self.control_queue_bytes)?;
        validate_optional_positive("diagnostic queue bytes", self.diagnostic_queue_bytes)?;
        validate_optional_positive("native scratch bytes", self.native_scratch_bytes)?;
        validate_optional_positive("child process bytes", self.child_process_bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignCancellationContract {
    pub cooperative_stop: bool,
    pub interrupt_safe: bool,
    pub force_termination_authorized: bool,
    pub drains_on_cancel: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignStateContract {
    pub emits_positions: bool,
    pub emits_watermarks: bool,
    pub emits_foreign_state: bool,
    pub terminal_state_required: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignSecurityContract {
    pub ambient_network: bool,
    pub ambient_filesystem: bool,
    pub secret_names: Vec<String>,
}

impl ForeignSecurityContract {
    pub fn validate(&self) -> Result<()> {
        for secret_name in &self.secret_names {
            validate_token("foreign secret name", secret_name, 256)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ForeignStreamOpenRequest {
    pub resource_id: ResourceId,
    pub partition_id: PartitionId,
    pub cancellation: ForeignCancellation,
}

pub struct ForeignStreamOpen {
    pub descriptor: ForeignProducerDescriptor,
    pub events: ForeignEventStream,
    /// Invocation-wide cancellation and join authority retained independently of stream polling.
    pub termination: InvocationTermination,
}

impl fmt::Debug for ForeignStreamOpen {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ForeignStreamOpen")
            .field("descriptor", &self.descriptor)
            .field("events", &"<foreign event stream>")
            .field("termination", &"<foreign invocation termination>")
            .finish()
    }
}

pub trait ForeignProducer: Send + Sync {
    fn descriptor(&self) -> &ForeignProducerDescriptor;
    fn open(
        &self,
        request: ForeignStreamOpenRequest,
    ) -> cdf_kernel::BoxFuture<'_, Result<ForeignStreamOpen>>;
}

#[derive(Debug, Default)]
struct ForeignCancellationState {
    cancelled: AtomicBool,
    waiters: Mutex<Vec<Waker>>,
}

#[derive(Clone, Debug, Default)]
pub struct ForeignCancellation(Arc<ForeignCancellationState>);

impl ForeignCancellation {
    pub fn cancel(&self) {
        if self.0.cancelled.swap(true, Ordering::AcqRel) {
            return;
        }
        let waiters = std::mem::take(&mut *self.0.waiters.lock().unwrap());
        for waiter in waiters {
            waiter.wake();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    pub fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            return Err(CdfError::transient("foreign stream was cancelled"));
        }
        Ok(())
    }

    pub fn cancelled(&self) -> ForeignCancellationFuture {
        ForeignCancellationFuture {
            cancellation: self.clone(),
            registered: None,
        }
    }

    pub async fn await_or_cancel<T, F>(&self, operation: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        let cancelled = self.cancelled();
        futures_util::pin_mut!(operation, cancelled);
        match futures_util::future::select(operation, cancelled).await {
            futures_util::future::Either::Left((result, _)) => result,
            futures_util::future::Either::Right(((), _)) => {
                Err(CdfError::transient("foreign stream was cancelled"))
            }
        }
    }
}

pub struct ForeignCancellationFuture {
    cancellation: ForeignCancellation,
    registered: Option<Waker>,
}

impl Future for ForeignCancellationFuture {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.cancellation.is_cancelled() {
            return Poll::Ready(());
        }
        let cancellation = self.cancellation.clone();
        let mut waiters = cancellation.0.waiters.lock().unwrap();
        if cancellation.is_cancelled() {
            return Poll::Ready(());
        }
        if let Some(previous) = self.registered.take()
            && let Some(index) = waiters
                .iter()
                .position(|waiter| waiter.will_wake(&previous))
        {
            waiters.swap_remove(index);
        }
        if !waiters
            .iter()
            .any(|waiter| waiter.will_wake(context.waker()))
        {
            waiters.push(context.waker().clone());
        }
        self.registered = Some(context.waker().clone());
        Poll::Pending
    }
}

impl Drop for ForeignCancellationFuture {
    fn drop(&mut self) {
        let Some(registered) = self.registered.take() else {
            return;
        };
        if let Ok(mut waiters) = self.cancellation.0.waiters.lock()
            && let Some(index) = waiters
                .iter()
                .position(|waiter| waiter.will_wake(&registered))
        {
            waiters.swap_remove(index);
        }
    }
}

#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "ForeignStreamEvent carries the batch payload by value; boxing every outcome would add one heap allocation per foreign batch on a future hot path."
)]
pub enum ForeignStreamEvent {
    Outcome(ForeignBatchOutcome),
    Control(ForeignControlEvent),
    Terminal(ForeignTerminalStatus),
}

#[derive(Debug)]
pub struct ForeignBatchOutcome {
    pub sequence: u64,
    pub batch: Batch,
    pub transfer_mode: ForeignTransferMode,
    pub copy: ForeignCopyClassification,
}

impl ForeignBatchOutcome {
    pub fn new(
        sequence: u64,
        batch: Batch,
        transfer_mode: ForeignTransferMode,
        copy: ForeignCopyClassification,
    ) -> Result<Self> {
        if sequence == 0 {
            return Err(CdfError::contract(
                "foreign batch outcome sequence must be greater than zero",
            ));
        }
        Ok(Self {
            sequence,
            batch,
            transfer_mode,
            copy,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ForeignCopyClassification {
    PayloadZeroCopyVerified,
    PayloadCopyKnown { bytes: u64 },
    CopyUnknown,
}

impl ForeignCopyClassification {
    pub fn payload_copy_known(bytes: u64) -> Result<Self> {
        if bytes == 0 {
            return Err(CdfError::contract(
                "known foreign payload copy bytes must be greater than zero",
            ));
        }
        Ok(Self::PayloadCopyKnown { bytes })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignControlEvent {
    pub sequence: u64,
    pub kind: ForeignControlKind,
}

impl ForeignControlEvent {
    pub fn new(sequence: u64, kind: ForeignControlKind) -> Result<Self> {
        if sequence == 0 {
            return Err(CdfError::contract(
                "foreign control event sequence must be greater than zero",
            ));
        }
        Ok(Self { sequence, kind })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ForeignControlKind {
    SourcePosition {
        position: SourcePosition,
    },
    Watermarks {
        watermarks: Vec<WatermarkClaim>,
    },
    ForeignState {
        position: SourcePosition,
    },
    Progress {
        rows: u64,
        bytes: u64,
    },
    Diagnostic {
        severity: ForeignDiagnosticSeverity,
        message: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForeignDiagnosticSeverity {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ForeignTerminalStatus {
    Succeeded {
        final_position: Option<SourcePosition>,
    },
    Failed {
        retryable: bool,
        message: String,
    },
    Cancelled,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ForeignStreamSummary {
    pub outcome_count: u64,
    pub control_count: u64,
    pub terminal: Option<ForeignTerminalStatus>,
}

pub fn batch_stream_from_foreign_events(events: ForeignEventStream) -> BatchStream {
    Box::pin(ForeignBatchStream {
        events,
        terminal: None,
        completed: false,
    })
}

pub async fn summarize_foreign_events(
    mut events: ForeignEventStream,
) -> Result<ForeignStreamSummary> {
    use futures_util::StreamExt;

    let mut summary = ForeignStreamSummary::default();
    while let Some(event) = events.next().await {
        if summary.terminal.is_some() {
            return Err(CdfError::data(
                "foreign stream emitted an event after its terminal status",
            ));
        }
        match event? {
            ForeignStreamEvent::Outcome(_) => summary.outcome_count += 1,
            ForeignStreamEvent::Control(_) => summary.control_count += 1,
            ForeignStreamEvent::Terminal(terminal) => {
                if summary.terminal.replace(terminal).is_some() {
                    return Err(CdfError::data(
                        "foreign stream emitted more than one terminal status",
                    ));
                }
            }
        }
    }
    if summary.terminal.is_none() {
        return Err(CdfError::data(
            "foreign stream completed without a terminal status",
        ));
    }
    Ok(summary)
}

struct ForeignBatchStream {
    events: ForeignEventStream,
    terminal: Option<ForeignTerminalStatus>,
    completed: bool,
}

impl Stream for ForeignBatchStream {
    type Item = Result<Batch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.completed {
            return std::task::Poll::Ready(None);
        }
        loop {
            match self.events.as_mut().poll_next(context) {
                std::task::Poll::Ready(Some(Ok(ForeignStreamEvent::Outcome(outcome)))) => {
                    if self.terminal.is_some() {
                        self.completed = true;
                        return std::task::Poll::Ready(Some(Err(CdfError::data(
                            "foreign stream emitted an outcome after its terminal status",
                        ))));
                    }
                    return std::task::Poll::Ready(Some(Ok(outcome.batch)));
                }
                std::task::Poll::Ready(Some(Ok(ForeignStreamEvent::Control(_)))) => {
                    if self.terminal.is_some() {
                        self.completed = true;
                        return std::task::Poll::Ready(Some(Err(CdfError::data(
                            "foreign stream emitted a control event after its terminal status",
                        ))));
                    }
                }
                std::task::Poll::Ready(Some(Ok(ForeignStreamEvent::Terminal(status)))) => {
                    if self.terminal.replace(status).is_some() {
                        self.completed = true;
                        return std::task::Poll::Ready(Some(Err(CdfError::data(
                            "foreign stream emitted more than one terminal status",
                        ))));
                    }
                }
                std::task::Poll::Ready(Some(Err(error))) => {
                    self.completed = true;
                    return std::task::Poll::Ready(Some(Err(error)));
                }
                std::task::Poll::Ready(None) => {
                    self.completed = true;
                    let Some(terminal) = self.terminal.take() else {
                        return std::task::Poll::Ready(Some(Err(CdfError::data(
                            "foreign stream ended without a terminal status",
                        ))));
                    };
                    match terminal {
                        ForeignTerminalStatus::Succeeded { .. } => {
                            return std::task::Poll::Ready(None);
                        }
                        ForeignTerminalStatus::Failed { retryable, message } => {
                            let error = if retryable {
                                CdfError::transient(message)
                            } else {
                                CdfError::data(message)
                            };
                            return std::task::Poll::Ready(Some(Err(error)));
                        }
                        ForeignTerminalStatus::Cancelled => {
                            return std::task::Poll::Ready(Some(Err(CdfError::transient(
                                "foreign stream was cancelled",
                            ))));
                        }
                    }
                }
                std::task::Poll::Pending => return std::task::Poll::Pending,
            }
        }
    }
}

fn validate_optional_positive(label: &str, value: Option<u64>) -> Result<()> {
    if value == Some(0) {
        return Err(CdfError::contract(format!(
            "foreign producer {label} must be greater than zero when configured"
        )));
    }
    Ok(())
}

fn validate_token(label: &str, value: &str, max_len: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max_len
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(CdfError::contract(format!(
            "{label} must contain 1..={max_len} non-whitespace, control-free characters",
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{BatchId, PartitionId, ResourceId, SchemaHash, SegmentId};
    use cdf_package::PackageBuilder;
    use futures_executor::block_on;
    use futures_util::{StreamExt, TryStreamExt, stream};

    use super::*;

    #[test]
    fn descriptor_validates_capabilities_without_concrete_runtime_types() {
        let descriptor = mock_descriptor(ForeignTransferMode::ArrowCData);
        descriptor.validate().unwrap();
        assert!(descriptor.supports_transfer_mode(ForeignTransferMode::ArrowCData));
        assert!(!descriptor.supports_transfer_mode(ForeignTransferMode::RowCompat));
    }

    #[test]
    fn cancellation_wakes_and_unregisters_pending_foreign_work() {
        let cancellation = ForeignCancellation::default();
        let mut pending = Box::pin(cancellation.cancelled());
        let mut context = Context::from_waker(futures_util::task::noop_waker_ref());
        assert!(matches!(pending.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(cancellation.0.waiters.lock().unwrap().len(), 1);
        drop(pending);
        assert!(cancellation.0.waiters.lock().unwrap().is_empty());

        cancellation.cancel();
        block_on(cancellation.cancelled());
        assert!(cancellation.check().is_err());
    }

    #[test]
    fn mock_transfer_modes_traverse_as_incremental_batches() {
        for mode in [
            ForeignTransferMode::ArrowCData,
            ForeignTransferMode::ArrowIpcStream,
            ForeignTransferMode::RowCompat,
        ] {
            let stream = Box::pin(stream::iter(vec![
                Ok(ForeignStreamEvent::Control(
                    ForeignControlEvent::new(1, ForeignControlKind::Progress { rows: 0, bytes: 0 })
                        .unwrap(),
                )),
                Ok(ForeignStreamEvent::Outcome(mock_outcome(2, mode))),
                Ok(ForeignStreamEvent::Outcome(mock_outcome(3, mode))),
                Ok(ForeignStreamEvent::Terminal(
                    ForeignTerminalStatus::Succeeded {
                        final_position: None,
                    },
                )),
            ])) as ForeignEventStream;
            let batches =
                block_on(batch_stream_from_foreign_events(stream).try_collect::<Vec<_>>()).unwrap();
            assert_eq!(batches.len(), 2);
            assert_eq!(batches[0].header.row_count, 2);
            assert_eq!(batches[1].header.row_count, 2);
        }
    }

    #[test]
    fn mock_stream_reaches_package_segments_without_whole_stream_collection() {
        let stream = Box::pin(stream::iter(vec![
            Ok(ForeignStreamEvent::Outcome(mock_outcome(
                1,
                ForeignTransferMode::ArrowIpcStream,
            ))),
            Ok(ForeignStreamEvent::Outcome(mock_outcome(
                2,
                ForeignTransferMode::ArrowIpcStream,
            ))),
            Ok(ForeignStreamEvent::Terminal(
                ForeignTerminalStatus::Succeeded {
                    final_position: None,
                },
            )),
        ])) as ForeignEventStream;
        let temp = tempfile::tempdir().unwrap();
        let builder = PackageBuilder::create(temp.path(), "foreign-mock-package").unwrap();
        let mut batches = batch_stream_from_foreign_events(stream);
        let mut segment_count = 0_u64;
        let mut package_row_ord_start = 0_u64;
        while let Some(batch) = block_on(batches.next()).transpose().unwrap() {
            segment_count += 1;
            let record_batch = batch.record_batch().unwrap().clone();
            let row_count = record_batch.num_rows() as u64;
            let record_batch = cdf_package_contract::append_package_row_ord(
                vec![record_batch],
                package_row_ord_start,
            )
            .unwrap();
            builder
                .write_segment(
                    SegmentId::new(format!("seg-{segment_count:06}")).unwrap(),
                    package_row_ord_start,
                    &record_batch,
                )
                .unwrap();
            package_row_ord_start += row_count;
        }
        let manifest = builder.finish().unwrap();
        assert_eq!(segment_count, 2);
        assert_eq!(manifest.identity.segments.len(), 2);
        assert_eq!(manifest.identity.segments[0].row_count, 2);
        assert_eq!(manifest.identity.segments[1].row_count, 2);
    }

    #[test]
    fn batch_projection_does_not_collect_before_first_output() {
        let polls = Arc::new(AtomicUsize::new(0));
        let stream = CountingForeignStream {
            polls: Arc::clone(&polls),
            next: 0,
        };
        let mut batches = batch_stream_from_foreign_events(Box::pin(stream));
        let first = block_on(batches.next()).unwrap().unwrap();
        assert_eq!(first.header.row_count, 2);
        assert!(polls.load(Ordering::SeqCst) <= 1);
    }

    #[test]
    fn stream_summary_requires_exactly_one_terminal_status() {
        let missing_terminal = Box::pin(stream::iter(vec![Ok(ForeignStreamEvent::Outcome(
            mock_outcome(1, ForeignTransferMode::ArrowIpcStream),
        ))])) as ForeignEventStream;
        assert!(block_on(summarize_foreign_events(missing_terminal)).is_err());

        let duplicate_terminal = Box::pin(stream::iter(vec![
            Ok(ForeignStreamEvent::Terminal(
                ForeignTerminalStatus::Succeeded {
                    final_position: None,
                },
            )),
            Ok(ForeignStreamEvent::Terminal(
                ForeignTerminalStatus::Succeeded {
                    final_position: None,
                },
            )),
        ])) as ForeignEventStream;
        assert!(block_on(summarize_foreign_events(duplicate_terminal)).is_err());

        let post_terminal_outcome = Box::pin(stream::iter(vec![
            Ok(ForeignStreamEvent::Terminal(
                ForeignTerminalStatus::Succeeded {
                    final_position: None,
                },
            )),
            Ok(ForeignStreamEvent::Outcome(mock_outcome(
                2,
                ForeignTransferMode::ArrowCData,
            ))),
        ])) as ForeignEventStream;
        assert!(block_on(summarize_foreign_events(post_terminal_outcome)).is_err());
    }

    #[test]
    fn production_batch_projection_rejects_every_post_terminal_event() {
        let post_terminal_outcome = Box::pin(stream::iter(vec![
            Ok(ForeignStreamEvent::Terminal(
                ForeignTerminalStatus::Succeeded {
                    final_position: None,
                },
            )),
            Ok(ForeignStreamEvent::Outcome(mock_outcome(
                2,
                ForeignTransferMode::ArrowCData,
            ))),
        ])) as ForeignEventStream;
        assert!(
            block_on(
                batch_stream_from_foreign_events(post_terminal_outcome).try_collect::<Vec<_>>()
            )
            .is_err()
        );

        let duplicate_terminal = Box::pin(stream::iter(vec![
            Ok(ForeignStreamEvent::Terminal(
                ForeignTerminalStatus::Succeeded {
                    final_position: None,
                },
            )),
            Ok(ForeignStreamEvent::Terminal(
                ForeignTerminalStatus::Succeeded {
                    final_position: None,
                },
            )),
        ])) as ForeignEventStream;
        assert!(
            block_on(batch_stream_from_foreign_events(duplicate_terminal).try_collect::<Vec<_>>())
                .is_err()
        );
    }

    #[test]
    fn crate_contract_stays_executor_neutral_and_non_collecting() {
        let manifest = std::fs::read_to_string(manifest_path()).unwrap();
        let source = std::fs::read_to_string(source_path()).unwrap();
        for forbidden in forbidden_runtime_tokens() {
            assert!(
                !manifest.contains(&forbidden),
                "manifest must not depend on concrete runtime `{forbidden}`"
            );
            assert!(
                !source.to_ascii_lowercase().contains(&forbidden),
                "contract source must not expose concrete runtime `{forbidden}`"
            );
        }
        for forbidden in forbidden_collection_tokens() {
            assert!(
                !source.contains(&forbidden),
                "foreign stream contract must not expose eager batch collection `{forbidden}`"
            );
        }
    }

    struct CountingForeignStream {
        polls: Arc<AtomicUsize>,
        next: u8,
    }

    impl Stream for CountingForeignStream {
        type Item = Result<ForeignStreamEvent>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            let item = match self.next {
                0 => Some(Ok(ForeignStreamEvent::Outcome(mock_outcome(
                    1,
                    ForeignTransferMode::ArrowCData,
                )))),
                1 => Some(Ok(ForeignStreamEvent::Terminal(
                    ForeignTerminalStatus::Succeeded {
                        final_position: None,
                    },
                ))),
                _ => None,
            };
            self.next = self.next.saturating_add(1);
            Poll::Ready(item)
        }
    }

    fn mock_descriptor(mode: ForeignTransferMode) -> ForeignProducerDescriptor {
        ForeignProducerDescriptor {
            producer_id: ForeignProducerId::new("mock_foreign").unwrap(),
            protocol_version: ForeignProtocolVersion::new("1").unwrap(),
            transfer_modes: vec![mode],
            startup: ForeignStartupModel::InProcessAttached,
            lanes: ForeignLaneCapabilities {
                execution_lane: ForeignExecutionLane::Cpu,
                maximum_internal_parallelism: 1,
                backpressure: ForeignBackpressure::Pull,
            },
            memory: ForeignMemoryContract {
                payload_window_bytes: Some(4096),
                control_queue_bytes: Some(1024),
                diagnostic_queue_bytes: Some(1024),
                native_scratch_bytes: None,
                child_process_bytes: None,
            },
            cancellation: ForeignCancellationContract {
                cooperative_stop: true,
                interrupt_safe: true,
                force_termination_authorized: false,
                drains_on_cancel: true,
            },
            state: ForeignStateContract {
                emits_positions: true,
                emits_watermarks: false,
                emits_foreign_state: false,
                terminal_state_required: false,
            },
            security: ForeignSecurityContract {
                ambient_network: false,
                ambient_filesystem: false,
                secret_names: Vec::new(),
            },
        }
    }

    fn mock_outcome(sequence: u64, mode: ForeignTransferMode) -> ForeignBatchOutcome {
        ForeignBatchOutcome::new(
            sequence,
            mock_batch(sequence),
            mode,
            match mode {
                ForeignTransferMode::ArrowCData => {
                    ForeignCopyClassification::PayloadZeroCopyVerified
                }
                ForeignTransferMode::ArrowIpcStream | ForeignTransferMode::RowCompat => {
                    ForeignCopyClassification::payload_copy_known(64).unwrap()
                }
            },
        )
        .unwrap()
    }

    fn mock_batch(sequence: u64) -> Batch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![
                sequence as i64,
                sequence as i64 + 1,
            ]))],
        )
        .unwrap();
        Batch::from_record_batch(
            BatchId::new(format!("batch-{sequence}")).unwrap(),
            ResourceId::new("mock.resource").unwrap(),
            PartitionId::new("partition-0").unwrap(),
            SchemaHash::new(
                "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            )
            .unwrap(),
            batch,
        )
        .unwrap()
    }

    fn manifest_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml")
    }

    fn source_path() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs")
    }

    fn forbidden_runtime_tokens() -> Vec<String> {
        vec![
            ["py", "o3"].concat(),
            ["to", "kio"].concat(),
            ["wasm", "time"].concat(),
            ["data", "fusion"].concat(),
            ["cdf", "_cli"].concat(),
            ["cdf", "-runtime"].concat(),
            ["std", "::", "process"].concat(),
        ]
    }

    fn forbidden_collection_tokens() -> Vec<String> {
        vec![
            ["Vec", "<", "Batch", ">"].concat(),
            ["Vec", "<", "Record", "Batch", ">"].concat(),
            ["Vec", " < ", "Batch", " >"].concat(),
        ]
    }
}
