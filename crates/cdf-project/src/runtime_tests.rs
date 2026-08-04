use std::{
    collections::{BTreeMap, VecDeque},
    env, fmt, fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_contract::{
    AnomalyFact, ContractPolicy, DedupKeep, ObservedSchema, RowRule, compile_validation_program,
    identifier_policy_from_destination_rules,
};
use cdf_dest_duckdb::DuckDbDestination;
use cdf_dest_parquet::ParquetDestination;
use cdf_dest_postgres::{PostgresDestination, PostgresTarget};
use cdf_engine::{
    CompiledStreamAdmissionEvidence, EnginePlan, EnginePlanInput, EngineRunOutput,
    EngineRunOutputWithSegmentPositions, EngineSegmentPosition, ExecutionProfile,
    LineageInputObservation, LineageSummary, Planner, StreamAdmissionObservationEvidence,
    negotiate_scan_plan,
};
use cdf_http::{HttpRequest, HttpResponse, HttpTransport, SecretProvider, SecretUri, SecretValue};
use cdf_kernel::ExecutionExtent;
use cdf_kernel::{
    BackpressureSupport, CHECKPOINT_STATE_VERSION, CapabilitySupport, CdfError, Checkpoint,
    CheckpointId, CheckpointStatus, CheckpointStore, CommitCounts, CommitPlan, CommitSession,
    CommittedLogPosition, CompositePosition, ConcurrencyLimit, CursorOrderingClaim, CursorPosition,
    CursorSpec, CursorValue, DeliveryGuarantee, DestinationCommitRequest, DestinationId,
    DestinationProtocol, DestinationSheet, EstimateSupport, FileManifest, FilePosition,
    FilterCapabilities, IdempotencySupport, IdempotencyToken, IdentifierRules, IncrementalShape,
    LATE_DATA_CARRYOVER_VERSION, LateDataCarryoverRef, MigrationRecord, PackageHash, PageToken,
    PartitionId, PipelineId, PlanId, PostgresCommitPosition, PostgresLogScope,
    ProcessedObservationOutcome, ProcessedObservationPosition, PushdownFidelity, QueryableResource,
    Receipt, ReceiptId, ReceiptVerification, ReplaySupport, ResourceCapabilities,
    ResourceDescriptor, ResourceId, ResourceStream, Result, RewindReport, RewindRequest, RunEvent,
    RunEventSink, RunEventSinkResult, RunId, RunPhase, RunPhaseMetric, RunPhaseStatus, ScanRequest,
    SchemaHash, SchemaSource, ScopeKey, SegmentAck, SegmentId, SourcePosition, StateDelta,
    StateSegment, TargetName, TransactionSupport, TrustLevel, TypeMapping, TypeMappingFidelity,
    VerifyClause, WriteDisposition,
};

fn postgres_log_position(slot: &str, end_lsn: u64) -> SourcePosition {
    SourcePosition::committed_log(CommittedLogPosition::PostgreSql(PostgresCommitPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        scope: PostgresLogScope {
            system_identifier: "7421938841407953395".to_owned(),
            database_oid: 16_384,
            slot: slot.to_owned(),
            output_plugin: "pgoutput".to_owned(),
            semantics_sha256:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        },
        commit_lsn: end_lsn.saturating_sub(1).max(1),
        end_lsn,
        xid: 7,
    }))
}
use cdf_object_access::FileTransportFacade;
use cdf_package::{PackageBuilder, PackageReader, canonical_json_bytes};
use cdf_package_contract::{
    DEDUP_SUMMARY_FILE, DESTINATION_COMMIT_PLAN_FILE, DestinationCommitPlanPreimage, MANIFEST_FILE,
    PROCESSED_OBSERVATIONS_FILE, PackageManifest, PackageReplayInputs, PackageStatus,
    ProcessedObservationEvidenceArtifact, RECEIPTS_FILE, STATE_INPUT_CHECKPOINT_FILE,
    STATE_PROPOSED_DELTA_FILE, SegmentEntry, StateDeltaPreimage,
};
use cdf_semantic::SemanticCatalog;
use cdf_source_files::{FileRuntimeDependencies, FileSourceDriver};
use cdf_source_rest::RestSourceDriver;
use cdf_state_sqlite::{
    InMemoryScopeLeaseStore, RunEventDetails, RunEventKind, RunEventValue, SecretReference,
    SqliteCheckpointStore, SqliteRunLedger,
};
use postgres::{Client, NoTls};
use tempfile::TempDir;
use tracing::{
    Event, Id, Metadata, Subscriber,
    field::{Field as TracingField, Visit},
    span::{Attributes, Record},
};

use crate::{
    BackfillPlanRequest, DependencyTuple, DestinationCommitPlanningInputs,
    DestinationCommitPlanningOutcome, DestinationReceiptReportingPolicy, FileManifestRunSummary,
    PackageArtifactRecoveryRequest, PackageArtifactReplayRequest, PreparedDestinationCommit,
    ProjectDestinationDescription, ProjectDestinationDriver, ProjectDestinationRegistry,
    ProjectDestinationRuntime, ProjectReceiptSource, ProjectResolutionContext,
    ProjectRunNoOpReason, ProjectRunOutcome, ProjectRunReport, ProjectRunRequest, ProjectRunSource,
    ResolvedProjectDestination, RunTelemetryConfig, RuntimeStage, TracingRunEventSink,
    backfill_pipeline_id, generate_lockfile_with_destination_artifacts, parse_cdf_toml,
    plan_backfill, recover_package_from_artifacts, replay_package_from_artifacts,
    replay_package_from_artifacts_with_stage_hook, resolve_project_run_destination,
    run_project_with_scheduler_and_telemetry,
    run_project_with_telemetry as run_project_with_execution_services_and_telemetry,
    runtime::{
        PackageReplayHooks, PackageReplayStage, StateDeltaTestRequest, record_package_receipt_once,
        replay_package_with_runtime, state_delta_from_run,
    },
};

mod destination_replay;
mod live_adapters;
mod orchestration;
mod promotion;
mod support;
