use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use cdf_kernel::{Batch, BatchStream, BoxFuture, CdfError, Result, SourceTransferReport};
use futures_core::Stream;

use crate::{
    control::{ForeignControlEvent, ForeignStreamSummary, ForeignTerminalStatus},
    descriptor::{ForeignCopyClassification, ForeignTransferMode},
};

pub type ForeignEventStream =
    Pin<Box<dyn Stream<Item = Result<ForeignStreamEvent>> + Send + 'static>>;

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

/// Runtime projection of one neutral foreign event stream.
///
/// Batches remain on the ordinary kernel path. Transfer/copy/control telemetry is aggregated
/// alongside them and becomes observable only after the stream's successful terminal boundary.
pub struct ForeignBatchProjection {
    pub batches: BatchStream,
    pub completion: BoxFuture<'static, Result<SourceTransferReport>>,
}

pub fn project_foreign_events(events: ForeignEventStream) -> ForeignBatchProjection {
    let completion = Arc::new(Mutex::new(None::<Result<SourceTransferReport>>));
    let stream_completion = Arc::clone(&completion);
    let batches = Box::pin(ForeignBatchStream {
        events,
        terminal: None,
        completed: false,
        report: SourceTransferReport::default(),
        completion: stream_completion,
    });
    ForeignBatchProjection {
        batches,
        completion: Box::pin(async move {
            completion
                .lock()
                .map_err(|_| CdfError::internal("foreign transfer report lock was poisoned"))?
                .take()
                .ok_or_else(|| {
                    CdfError::internal(
                        "foreign transfer report was requested before successful stream completion",
                    )
                })?
        }),
    }
}

pub fn batch_stream_from_foreign_events(events: ForeignEventStream) -> BatchStream {
    project_foreign_events(events).batches
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
    report: SourceTransferReport,
    completion: Arc<Mutex<Option<Result<SourceTransferReport>>>>,
}

impl Stream for ForeignBatchStream {
    type Item = Result<Batch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.completed {
            return Poll::Ready(None);
        }
        loop {
            match self.events.as_mut().poll_next(context) {
                Poll::Ready(Some(Ok(ForeignStreamEvent::Outcome(outcome)))) => {
                    if self.terminal.is_some() {
                        self.completed = true;
                        return Poll::Ready(Some(Err(CdfError::data(
                            "foreign stream emitted an outcome after its terminal status",
                        ))));
                    }
                    if let Err(error) = self.report.record_outcome(
                        outcome.transfer_mode,
                        outcome.batch.header.row_count,
                        outcome.batch.header.byte_count,
                        &outcome.copy,
                    ) {
                        self.completed = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                    return Poll::Ready(Some(Ok(outcome.batch)));
                }
                Poll::Ready(Some(Ok(ForeignStreamEvent::Control(_)))) => {
                    if self.terminal.is_some() {
                        self.completed = true;
                        return Poll::Ready(Some(Err(CdfError::data(
                            "foreign stream emitted a control event after its terminal status",
                        ))));
                    }
                    if let Err(error) = self.report.record_control() {
                        self.completed = true;
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Poll::Ready(Some(Ok(ForeignStreamEvent::Terminal(status)))) => {
                    if self.terminal.replace(status).is_some() {
                        self.completed = true;
                        return Poll::Ready(Some(Err(CdfError::data(
                            "foreign stream emitted more than one terminal status",
                        ))));
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    if self.terminal.is_some() {
                        self.completed = true;
                        return Poll::Ready(Some(Err(CdfError::data(
                            "foreign stream failed after its terminal status",
                        ))));
                    }
                    self.terminal = Some(ForeignTerminalStatus::Failed {
                        retryable: matches!(
                            error.kind,
                            cdf_kernel::ErrorKind::Transient | cdf_kernel::ErrorKind::RateLimited
                        ),
                        message: error.message,
                    });
                }
                Poll::Ready(None) => {
                    self.completed = true;
                    let Some(terminal) = self.terminal.take() else {
                        return Poll::Ready(Some(Err(CdfError::data(
                            "foreign stream ended without a terminal status",
                        ))));
                    };
                    match terminal {
                        ForeignTerminalStatus::Succeeded { .. } => {
                            let report = std::mem::take(&mut self.report);
                            let result = self
                                .completion
                                .lock()
                                .map_err(|_| {
                                    CdfError::internal("foreign transfer report lock was poisoned")
                                })
                                .map(|mut completion| {
                                    *completion = Some(Ok(report));
                                });
                            if let Err(error) = result {
                                return Poll::Ready(Some(Err(error)));
                            }
                            return Poll::Ready(None);
                        }
                        ForeignTerminalStatus::Failed { retryable, message } => {
                            let error = if retryable {
                                CdfError::transient(message)
                            } else {
                                CdfError::data(message)
                            };
                            return Poll::Ready(Some(Err(error)));
                        }
                        ForeignTerminalStatus::Cancelled => {
                            return Poll::Ready(Some(Err(CdfError::transient(
                                "foreign stream was cancelled",
                            ))));
                        }
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
