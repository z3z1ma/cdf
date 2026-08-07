mod artifacts;
mod destinations;
mod hooks;
mod ledger;
mod orchestration;
mod planning;
mod promotion;
mod receipt_source;
mod receipts;
mod replay;
mod resources;
mod tracing_bridge;
mod types;
mod validation;

pub use cdf_state_sqlite::StateStorePathOwnership;
pub use destinations::{
    DestinationCommitPlanningInputs, DestinationCommitPlanningOutcome, DestinationPlanningContext,
    DestinationReceiptReportingPolicy, PreparedDestinationCommit, ProjectDestinationDescription,
    ProjectDestinationDriver, ProjectDestinationRegistry, ProjectDestinationRuntime,
    ProjectResolutionContext, ResolvedProjectDestination, absolute_under_root, commit_request,
    local_uri_path, resolve_project_run_destination,
};
pub use hooks::{ReceiptVerifiedHook, RuntimeStage, RuntimeStageHook};
#[cfg(test)]
pub(crate) use orchestration::load_late_data_carryover;
pub use orchestration::{
    run_project, run_project_with_scheduler_and_telemetry, run_project_with_telemetry,
};
pub use planning::{ProjectDestinationCommitPlan, ProjectDestinationSyntheticInput};
pub use promotion::{
    DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS, SCHEMA_PROMOTION_CORRECTION_PACKAGE_VERSION,
    SCHEMA_PROMOTION_CORRECTION_TARGET_AUTHORITY_VERSION,
    SCHEMA_PROMOTION_EXECUTION_ARTIFACT_VERSION, SchemaPromotionCorrectionPackageArtifact,
    SchemaPromotionExecutionFailpoint, SchemaPromotionExecutionPhase,
    SchemaPromotionExecutionPlanArtifact, SchemaPromotionExecutionReport,
    SchemaPromotionExecutionRequest, SchemaPromotionExecutionTargetReport,
    execute_schema_promotion, inspect_local_promotion_availability,
};
pub use receipt_source::ProjectReceiptSource;
pub use replay::{
    recover_package_from_artifacts, replay_package_from_artifacts,
    replay_package_from_artifacts_with_stage_hook,
};
pub use resources::{ProjectRunSource, WindowScopedResource};
pub use tracing_bridge::TracingRunEventSink;
pub use types::{
    FileManifestRunSummary, PackageArtifactRecoveryRequest, PackageArtifactReplayRequest,
    PackageReplayReport, ProjectDrainEpochReport, ProjectDrainRunReport, ProjectRunNoOpReason,
    ProjectRunNoOpReport, ProjectRunOutcome, ProjectRunReport, ProjectRunRequest,
    RunTelemetryConfig,
};
pub use validation::ensure_parent_directory as ensure_state_parent_directory;

#[cfg(test)]
pub(crate) use artifacts::{StateDeltaTestRequest, state_delta_from_run};
#[cfg(test)]
pub(crate) use replay::{
    PackageReplayHooks, PackageReplayStage, record_package_receipt_once,
    replay_package_with_runtime,
};

#[cfg(test)]
#[allow(
    unused_imports,
    reason = "white-box runtime tests intentionally aggregate internal orchestration contracts"
)]
mod prelude {
    pub(super) use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        path::{Component, Path, PathBuf},
    };

    pub(super) use arrow_schema::Schema;
    #[cfg(test)]
    pub(super) use cdf_engine::EngineRunOutputWithSegmentPositions;
    pub(super) use cdf_engine::{
        EngineExecutionConfig, EnginePackageDraft, EnginePlan,
        execute_to_package_with_segment_positions_and_pre_finalize,
        execute_to_package_with_streaming_hooks,
    };
    pub(super) use cdf_kernel::{
        CHECKPOINT_STATE_VERSION, CdfError, Checkpoint, CheckpointId, CheckpointStatus,
        CheckpointStore, CursorOrderingClaim, DestinationCommitRequest, DestinationId,
        EffectiveSchemaRuntime, FilePosition, IdempotencyToken, IncrementalShape, PackageHash,
        PartitionPlan, PipelineId, PlanId, QueryableResource, Receipt, ResourceCapabilities,
        ResourceDescriptor, ResourceId, ResourceStream, Result, RunEventAppend, RunEventDetails,
        RunEventKind, RunEventSink, RunEventValue, RunId, RunPhase, RunPhaseMetric, RunPhaseStatus,
        ScanPlan, ScanRequest, SchemaHash, ScopeKey, SegmentAck, SegmentId, SourcePosition,
        StateDelta, StateSegment, TargetName, WriteDisposition,
    };
    pub(super) use cdf_package::{PackageReader, VerifiedPackage, VerifiedPackageReader};
    pub(super) use cdf_package_contract::{
        DestinationCommitPlanPreimage, PackageReplayInputs, PackageStatus, SegmentEntry,
        StateDeltaPreimage,
    };
    pub(super) use cdf_runtime::{ExecutionServices, SourceRetryEvidence};
    pub(super) use cdf_state_sqlite::{
        RunLedgerSnapshot, SqliteCheckpointStore, SqliteRunLedger, StateStorePathOwnership,
    };
}
