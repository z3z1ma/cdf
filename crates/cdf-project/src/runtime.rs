mod artifacts;
mod destinations;
mod hooks;
mod ledger;
mod orchestration;
mod planning;
mod receipts;
mod replay;
mod resources;
mod types;
mod validation;

pub use destinations::*;
pub use hooks::{ReceiptVerifiedHook, RuntimeStage, RuntimeStageHook};
pub use orchestration::run_project;
pub use planning::*;
pub use replay::{
    recover_package_from_artifacts, recover_prepared_package, replay_package_from_artifacts,
    replay_package_from_artifacts_with_stage_hook, replay_prepared_package,
    replay_prepared_package_with_stage_hook,
};
pub use resources::*;
pub use types::*;

#[cfg(test)]
pub(crate) use artifacts::state_delta_from_run;
#[cfg(test)]
pub(crate) use orchestration::run_local_file_to_duckdb_checkpoint;
#[cfg(test)]
pub(crate) use replay::{
    PackageReplayHooks, PackageReplayStage, recover_package_with_runtime,
    replay_package_with_runtime,
};

mod prelude {
    pub(super) use std::{
        any::Any,
        collections::{BTreeMap, BTreeSet},
        fs,
        path::{Component, Path, PathBuf},
    };

    pub(super) use arrow_schema::{DataType, Schema, TimeUnit};
    pub(super) use cdf_declarative::{
        CompiledResource, CompiledResourcePlan, RestResource, SqlResource,
    };
    pub(super) use cdf_dest_duckdb::{DuckDbCommitRequest, DuckDbDestination};
    pub(super) use cdf_dest_parquet::{ParquetCommitRequest, ParquetDestination};
    pub(super) use cdf_dest_postgres::{
        MergeDedupPolicy, PostgresColumn, PostgresCommitRequest, PostgresDestination,
        PostgresExistingTable, PostgresIdentifier, PostgresLoadPlanInput, PostgresTarget,
        postgres_columns_for_schema,
    };
    #[cfg(test)]
    pub(super) use cdf_engine::EngineRunOutputWithSegmentPositions;
    pub(super) use cdf_engine::{
        EnginePackageDraft, EnginePlan, execute_to_package_with_segment_positions_and_pre_finalize,
    };
    pub(super) use cdf_http::SecretProvider;
    pub(super) use cdf_kernel::{
        CHECKPOINT_STATE_VERSION, CdfError, Checkpoint, CheckpointId, CheckpointStatus,
        CheckpointStore, CursorOrderingClaim, CursorPosition, CursorValue,
        DestinationCommitRequest, DestinationId, DestinationProtocol, IdempotencyToken,
        IncrementalShape, PackageHash, PartitionPlan, PipelineId, PlanId, QueryableResource,
        Receipt, ResourceCapabilities, ResourceDescriptor, ResourceId, ResourceStream, Result,
        RunEventAppend, RunEventDetails, RunEventKind, RunEventSink, RunEventValue, RunId,
        ScanPlan, ScanRequest, SchemaHash, SchemaSource, ScopeKey, SegmentAck, SegmentId,
        SourcePosition, StateDelta, StateSegment, TargetName, WriteDisposition,
    };
    pub(super) use cdf_package::{
        DestinationCommitPlanPreimage, PackageReader, PackageReplayInputs, PackageStatus,
        ReplayView, SegmentEntry, StateDeltaPreimage,
    };
    pub(super) use cdf_state_sqlite::{RunLedgerSnapshot, SqliteCheckpointStore, SqliteRunLedger};
}
