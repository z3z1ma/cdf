mod artifacts;
mod destinations;
mod hooks;
mod ledger;
mod orchestration;
mod planning;
mod promotion;
mod receipts;
mod replay;
mod resources;
mod tracing_bridge;
mod types;
mod validation;

pub use destinations::*;
pub use hooks::{ReceiptVerifiedHook, RuntimeStage, RuntimeStageHook};
pub use orchestration::{
    run_project, run_project_with_scheduler_and_telemetry, run_project_with_telemetry,
};
pub use planning::*;
pub use promotion::*;
pub use replay::{
    recover_package_from_artifacts, replay_package_from_artifacts,
    replay_package_from_artifacts_with_stage_hook,
};
pub use resources::*;
pub use tracing_bridge::TracingRunEventSink;
pub use types::*;

#[cfg(test)]
pub(crate) use artifacts::{StateDeltaTestRequest, state_delta_from_run};
#[cfg(test)]
pub(crate) use replay::{
    PackageReplayHooks, PackageReplayStage, record_package_receipt_once,
    replay_package_with_runtime,
};

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
        EngineExecutionOptions, EnginePackageDraft, EnginePlan,
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
    pub(super) use cdf_state_sqlite::{RunLedgerSnapshot, SqliteCheckpointStore, SqliteRunLedger};
}
