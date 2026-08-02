#![doc = "Executor-neutral foreign producer stream contract for cdf."]

mod cancellation;
mod control;
mod descriptor;
mod events;
mod producer;

pub use cancellation::{ForeignCancellation, ForeignCancellationFuture};
pub use control::{
    ForeignControlEvent, ForeignControlKind, ForeignDiagnosticSeverity, ForeignStreamSummary,
    ForeignTerminalStatus,
};
pub use descriptor::{
    ForeignBackpressure, ForeignCancellationContract, ForeignCopyClassification,
    ForeignExecutionLane, ForeignLaneCapabilities, ForeignMemoryContract,
    ForeignProducerDescriptor, ForeignProducerId, ForeignProtocolVersion, ForeignSchemaAcquisition,
    ForeignSecurityContract, ForeignStartupModel, ForeignStateContract, ForeignTransferMode,
};
pub use events::{
    ForeignBatchOutcome, ForeignBatchProjection, ForeignEventStream, ForeignStreamEvent,
    batch_stream_from_foreign_events, project_foreign_events, summarize_foreign_events,
};
pub use producer::{ForeignProducer, ForeignStreamOpen, ForeignStreamOpenRequest};

#[cfg(test)]
mod tests;
