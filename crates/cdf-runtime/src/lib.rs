#![doc = "Engine-neutral runtime contracts and extension registries for cdf."]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "extension-contract production code must propagate recoverable failures"
    )
)]

mod bounded_format;
mod bulk;
mod canonical_frontier;
mod capabilities;
mod capability_types;
mod content_reclamation;
mod context;
mod controlled_byte_source;
mod destination;
mod drain_epoch;
mod execution_host;
mod format;
mod graph;
mod observed_byte_source;
mod registry;
mod retry;
mod rolling_replay;
mod scheduler;
mod source;
mod source_add;
mod source_frontier;
mod source_registry;
mod staging;
mod staging_identity;
mod staging_lease;
mod stream_policy;
mod transformed_byte_source;
mod utilities;
mod watermark;
mod worker_protocol;

pub use bounded_format::{
    BoundedFormatRead, BoundedFormatRequest, FormatBatchStream, FormatStreamRead, MemoryByteSource,
    decode_bounded_format, decode_format_stream,
};
pub use bulk::{
    BulkFallbackMode, BulkOrdering, BulkPathDescriptor, BulkPathPreparation,
    BulkPathPreparationInput, BulkPathRejection, BulkSizeRange, PreparedBulkPath,
};
pub use canonical_frontier::{
    CanonicalBoxStream, CanonicalStreamCompletion, CanonicalStreamFrontier, CanonicalStreamOpener,
    canonical_stream_frontier, canonical_stream_frontier_with_completion,
};
pub use capabilities::{
    DestinationCommitPayloadMode, DestinationDescription, DestinationHealthProbe,
    DestinationHealthResult, DestinationHealthStatus, DestinationInspection,
    DestinationRuntimeCapabilities,
};
pub use capability_types::{DestinationIngressMode, DestinationWriterModel};
pub use cdf_memory::{
    FixedSpillBudget, SpillBudgetCoordinator, SpillBudgetSnapshot, SpillReservation,
};
pub use content_reclamation::{
    ConditionalContentDeleteOutcome, ConditionalContentDeleter, ContentReclamationReport,
};
pub use context::{DestinationPolicyProvider, DestinationResolutionContext};
pub use controlled_byte_source::ControlledByteSource;
pub use destination::{
    DestinationCommitOutcome, DestinationCommitPlanningInputs, DestinationCommitPlanningOutcome,
    DestinationCommitVerification, DestinationIngress, DestinationPlanningContext,
    DestinationReceiptReportingPolicy, DestinationRuntime, FinalizedPackageIngress,
    PreparedDestinationCommit, StagedSegmentIngress,
};
pub use drain_epoch::{
    DrainEpochClosure, DrainEpochController, DrainEpochDecision, DrainSafeFrontierObservation,
};
pub use execution_host::{
    BlockingLaneBinding, BlockingLaneSpec, BlockingTask, BlockingTaskStreamSender,
    BlockingValueTask, CancellationFuture, CpuFutureTask, CpuTaskSpec, ExecutionHost,
    ExecutionHostCapabilities, ExecutionServices, ExecutionTaskScope, InterruptionSafety, IoTask,
    IoValue, IoValueTask, LaneAffinity, RunCancellation, RunWorkPermit, RunWorkReport,
    RuntimeSchedulerReport, ScopedBlockingTask, ScopedTaskStream, SourceIoController,
    SourceIoControllerLimits, SourceIoControllerMode, SourceIoControllerReport,
    SourceIoRequestPermit, SourceRateAdmissionReport, TaskScopeReport, TaskStreamSender,
};
pub use format::{
    AccountedByteCursor, AccountedByteStream, AccountedChunksReader, AccountedPhysicalBatch,
    ByteExtent, ByteSource, ByteSourceCapabilities, ByteTransformDescriptor, ByteTransformDriver,
    ByteTransformId, ByteTransformRegistry, ByteTransformRequest, CompiledFormatBinding,
    ContentIdentity, DEFAULT_FORMAT_BATCH_ROWS, DecodePlanningRequest, DecodeSchemaAuthority,
    DecodeSchemaPlan, DecodeUnitPlan, ExactRangeCoalescingPolicy, ExactRangeReadBatch,
    FormatDecodeSession, FormatDetection, FormatDetectionConfidence, FormatDetectionProbe,
    FormatDiscoveryCapabilities, FormatDiscoveryKind, FormatDiscoveryRequest, FormatDriver,
    FormatDriverDescriptor, FormatErrorIsolation, FormatId, FormatProbe, FormatRegistry,
    FormatSourceAccess, GenerationStrength, MagicSignature, PhysicalDecodeRequest,
    PhysicalDecodeStream, PhysicalSchemaObservation, REMOTE_RANGE_COALESCING_POLICY, ReadOptions,
    SequentialReadRequest, TransformChecksumBehavior, TransformExpansionGuard,
    decode_unit_no_lookback_frontiers,
};
pub use graph::{
    AccountedGraphOutcomes, AccountedGraphPayload, CompiledOperatorGraph, GraphDataEnvelope,
    GraphEdgeDescriptor, GraphEdgeReceiver, GraphEdgeRuntimeConfig, GraphEdgeSender,
    GraphEdgeTransfer, GraphExecutorClass, GraphNodeDescriptor, GraphNodeKind, GraphOrdering,
    GraphOutcome, GraphSchemaAuthority, account_graph_batch, account_graph_outcomes, graph_edge,
};
pub use observed_byte_source::{ObservedByteSource, SourceIoObserver};
pub use registry::{DestinationDriver, DestinationRegistry, bind_destination_runtime};
pub use retry::{
    CompiledSourceRetry, SourceRetryDecision, SourceRetryEvidence, SourceRetryEvidenceView,
    SourceRetryExhaustion, SourceRetryHistoryEntry, SourceRetryJournal, SourceRetryState,
};
pub use rolling_replay::{RollingReplayLimits, RollingReplayStore, RollingReplayUnit};
pub use scheduler::{
    AdmissionCeilings, AdmissionLimits, AdmissionPermit, AdmissionRequest, AdmissionSnapshot,
    CanonicalPartitionBinding, CanonicalPartitionOrdinal, CanonicalPartitionSchedule,
    CanonicalUnitOrdinal, DecodeUnitConcurrencyResolution, EffectiveJobsResolution,
    FairAdmissionController, PartitionAdmissionTemplate, PartitionScheduleAuthority,
    RuntimeSchedulerResolution, ScheduledPartition, effective_container_cpu_slots,
    resolve_decode_unit_concurrency, resolve_effective_jobs, resolve_runtime_scheduler,
};
pub use source::{
    CompiledSourceCompilerBinding, CompiledSourceExecutionPlan, CompiledSourceIdentities,
    CompiledSourcePlan, CompiledSourcePlanInput, PreparedSourcePayload, PreparedSourcePayloadKey,
    PreparedSourcePayloads, SourceAttestationStrength, SourceBatchMemoryContract,
    SourceCompileContext, SourceCompileRequest, SourceContentDigest, SourceCursorPushdown,
    SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest, SourceDiscoverySession,
    SourceDriver, SourceDriverDescriptor, SourceDriverId, SourceEgressAuthorizer,
    SourceEgressRequest, SourceEgressScope, SourceEgressTarget, SourceEvidenceLocation,
    SourceExecutionCapabilities, SourceExecutorClass, SourceFrontierCapability, SourceHealthBudget,
    SourceHealthLimits, SourceHealthRequest, SourceHealthResult, SourceHealthSink,
    SourceHealthStatus, SourceHealthTarget, SourceRateLimit, SourceReferenceCompileRequest,
    SourceReferenceCompiler, SourceResolutionContext, SourceRetryGranularity, SourceRetryPolicy,
    SourceSchemaObservation, SourceStreamCapabilities, SourceWatermarkCapability,
    validate_source_evidence_identity,
};
pub use source_add::{
    PlannedSourceAdd, SourceAddCursor, SourceAddCursorOrdering, SourceAddPlanner,
    SourceAddPrivateFile, SourceAddProposal, SourceAddRequest,
};
pub use source_frontier::{
    CanonicalSourceFrontier, CanonicalSourcePartition, SourceFrontierReport,
    SourcePartitionOpenFuture, SourcePartitionOpener,
};
pub use source_registry::SourceRegistry;
pub use staging::{
    DurableLocalFile, DurableLocalFileAccess, DurableSegmentReader, StagedIngressCapabilities,
    StagedIngressRequest, StagedIngressSession, StagedIngressWorkload, StagedSegmentAck,
    StagedSegmentIdentity, StagedSegmentRequest, StagedSegmentStream, StagingAttemptBinding,
    StagingCleanupCandidate, StagingRecoveryMode, StagingSchedulingContext, StagingSnapshot,
    StagingVisibility, VerifiedFinalBinding,
};
pub use staging_identity::LoadAttemptId;
pub use staging_lease::{
    ExpiredStagingLeaseProof, ManagedExpiredStagingLeaseProof, ManagedStagingLease,
    ScopeStagingLeaseAuthority, StagingLease, StagingLeaseAuthority, StagingLeaseIdentity,
    StagingLeaseSupervisor, StagingLeaseTiming, StagingMutationGuard,
};
pub use stream_policy::{COMPILED_STREAM_POLICY_VERSION, CompiledStreamPolicy};
pub use transformed_byte_source::{TransformSourceConfig, TransformedByteSource};
pub use utilities::{
    absolute_under_root, artifact_hash, commit_request, destination_uri_scheme, local_uri_path,
    validate_artifact_hash, validate_destination_scheme,
};
pub use watermark::PartitionWatermarkTracker;
pub use worker_protocol::{
    AdmittedPartitionWorkerResult, AdmittedSegmentWorkerResult, IsolatedPartitionExecutor,
    IsolatedPartitionInvocation, IsolatedSegmentExecutor, IsolatedSegmentInvocation,
    LocalIsolatedSegmentHost, LocalIsolatedWorkerHost, PARTITION_ATTEMPT_VERSION,
    PARTITION_WORKER_RESULT_VERSION, PORTABLE_CHECKPOINT_STATE_VERSION,
    PORTABLE_PARTITION_TASK_VERSION, PORTABLE_SEGMENT_TASK_VERSION,
    PORTABLE_SOURCE_POSITION_VERSION, PartitionAttemptEnvelope, PartitionWorkerResult,
    PartitionWorkerResultInput, PortableExecutionBinding, PortablePartitionBinding,
    PortablePartitionTask, PortablePartitionTaskInput, PortableSegmentTask,
    PortableSegmentTaskInput, PortableSourceBinding, PortableWorkerTask,
    ReconstructedExecutionAuthority, ReconstructedSegmentTask, ReconstructedWorkerExecutionProgram,
    ReconstructedWorkerTaskAuthority, SEGMENT_WORKER_RESULT_VERSION, SegmentTaskReconstructor,
    SegmentWorkerResult, VerifiedCanonicalSegmentFacts, VerifiedWorkerArtifactFacts,
    VerifiedWorkerSourceFacts, WorkerAdmissionVerifier, WorkerArtifactKind,
    WorkerArtifactObjectState, WorkerArtifactReceipt, WorkerArtifactReference, WorkerArtifactRole,
    WorkerArtifactWriteAuthorization, WorkerArtifactWritePermit, WorkerArtifactWriteScope,
    WorkerArtifactWriteSession, WorkerAttemptPolicy, WorkerAuthorizedArtifactSink,
    WorkerCapabilityRequirements, WorkerCompatibility, WorkerComponentVersion, WorkerControlBudget,
    WorkerExecutionArtifacts, WorkerInputCheckpointBinding, WorkerLeaseState,
    WorkerObjectGenerationPrecondition, WorkerOutputPolicy, WorkerOutputVerifier, WorkerPosition,
    WorkerProcessedObservation, WorkerResourceBudget, WorkerResultCounts,
    WorkerRuntimeCapabilities, WorkerSourceAttestation, WorkerTelemetry, WorkerTerminalStatus,
    execute_local_isolated_partition, execute_local_isolated_segment,
};

pub mod foreign {
    pub use cdf_foreign_stream::{
        ForeignBackpressure, ForeignBatchOutcome, ForeignBatchProjection, ForeignCancellation,
        ForeignCancellationContract, ForeignCancellationFuture, ForeignControlEvent,
        ForeignControlKind, ForeignCopyClassification, ForeignDiagnosticSeverity,
        ForeignEventStream, ForeignExecutionLane, ForeignLaneCapabilities, ForeignMemoryContract,
        ForeignProducer, ForeignProducerDescriptor, ForeignProducerId, ForeignProtocolVersion,
        ForeignSchemaAcquisition, ForeignSecurityContract, ForeignStartupModel,
        ForeignStateContract, ForeignStreamEvent, ForeignStreamOpen, ForeignStreamOpenRequest,
        ForeignStreamSummary, ForeignTerminalStatus, ForeignTransferMode,
        batch_stream_from_foreign_events, project_foreign_events, summarize_foreign_events,
    };
}

pub type RuntimeSecretProvider =
    dyn cdf_http::SecretProvider + Send + Sync + std::panic::RefUnwindSafe;

#[cfg(test)]
#[allow(
    unused_imports,
    reason = "white-box tests intentionally aggregate the runtime contract surface"
)]
mod prelude {
    pub(crate) use std::{
        any::Any,
        path::{Path, PathBuf},
    };

    pub(crate) use arrow_schema::Schema;
    pub(crate) use cdf_kernel::{
        CapabilitySupport, CdfError, CommitPlan, CommitSession, DestinationCommitRequest,
        DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest, DestinationId,
        DestinationProtocol, DestinationSheet, DestinationSheetArtifact, Receipt,
        ReceiptVerification, ResourceStream, Result, SchemaHash, StateDelta, TargetName,
        WriteDisposition,
    };
    pub(crate) use cdf_package_contract::{PackageReplayInputs, SharedVerifiedPackageAccess};
    pub(crate) use serde::{Deserialize, Serialize};

    pub(crate) use crate::RuntimeSecretProvider;
    pub(crate) use crate::bulk::*;
    pub(crate) use crate::capabilities::*;
    pub(crate) use crate::context::*;
    pub(crate) use crate::destination::*;
    pub(crate) use crate::staging::*;
    pub(crate) use crate::utilities::*;
}

#[cfg(test)]
mod tests;
