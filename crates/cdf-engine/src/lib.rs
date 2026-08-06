#![doc = "Planning and execution boundary for cdf."]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "foundational production code must propagate recoverable failures"
    )
)]

mod dedup_spill;
mod execution;
mod expression;
mod expression_execution;
mod expression_memory;
mod graph_plan;
mod late_data;
mod memory;
mod output_schema;
mod planning;
mod residual_spill;
mod segmentation;
mod sql_analysis;
mod standalone_host;
mod statistics_pruning;
mod table_provider;
#[cfg(test)]
mod tests;
mod types;
mod variant_capture;
mod worker_task;

pub use execution::{
    DrainEpochExecution, DurableSegmentHook, DurableSegmentPayload, LateDataCarryoverInput,
    PackagePreFinalizeHook, PackageSegmentProgressHook, StreamingFinalizeHook,
    assemble_isolated_worker_package, execute_drain_epoch_with_hooks, execute_to_package,
    execute_to_package_with_progress_hook, execute_to_package_with_run_id,
    execute_to_package_with_segment_positions,
    execute_to_package_with_segment_positions_and_pre_finalize,
    execute_to_package_with_streaming_hooks, normalize_record_batch,
    preview_partition_selector_candidate, preview_resource,
};
pub use expression::{
    AnalyzedProjectionExpression, AnalyzedScalarExpression, ExpressionPath,
    ExpressionSourceLocation, compile_relational_expression_plan, lower_analyzed_scalar_expression,
};
pub use expression_execution::{
    BoundRelationalExpressionPlan, bind_relational_expression_plan,
    execute_bound_relational_expression_plan, execute_relational_expression_plan,
    execute_scalar_expression,
};
pub use graph_plan::compile_operator_graph;
pub use memory::DataFusionMemoryCoordinator;
pub use planning::{
    CDF_NATIVE_RESOURCE_ADAPTER_KIND, Planner, datafusion_filter_pushdown, negotiate_scan_plan,
    validate_plan_schema_authority,
};
pub use segmentation::{
    AdaptiveMicrobatchController, CanonicalSegment, CanonicalSegmentAssembler,
    CanonicalSegmentationPolicy, PositionJoin, join_positions,
};
pub use sql_analysis::{
    AnalyzedProjectQuery, ParsedProjectQuery, ParsedUpstreamRelation, ProjectSqlSpan,
    analyze_project_query, analyze_project_query_at, parse_project_query, parse_project_query_at,
};
pub use standalone_host::StandaloneExecutionHost;
pub use statistics_pruning::{
    StatisticsPruningContainerGrain, StatisticsPruningDecision, StatisticsPruningEvidence,
    StatisticsPruningLimits, StatisticsPruningOutcome, StatisticsPruningSummary,
    VerifiedPackageSegmentPruning, for_each_verified_package_segment_pruning,
};
pub use table_provider::{QueryableResourceTableProvider, queryable_resource_table_provider};
pub(crate) use types::CompiledSchemaAdmissionOutcome;
pub use types::{
    COMPILED_SCHEMA_ADMISSION_VERSION, CompiledArrowSchema, CompiledSchemaAdmissionPlan,
    CompiledSchemaQuarantineEvidence, CompiledStreamAdmissionEvidence, DEFAULT_PREVIEW_MAX_BATCHES,
    DEFAULT_PREVIEW_MAX_BYTES, DEFAULT_PREVIEW_MAX_ROWS, DrainPartitionResume,
    ENGINE_EXECUTION_EVIDENCE_VERSION, ENGINE_PARTITION_EVIDENCE_VERSION,
    EffectiveSchemaObservationCoercion, EffectiveSchemaPlanEvidence, EngineDrainEpoch,
    EngineDrainEpochOutcome, EngineExecutionConfig, EngineExecutionEvidence,
    EngineExecutionInvocation, EnginePackageDraft, EnginePartitionDrainEvidence,
    EnginePartitionEvidence, EnginePlan, EnginePlanInput, EnginePreviewLimits, EnginePreviewOutput,
    EnginePreviewSelectedPartition, EnginePreviewSelectionEvidence, EngineRunOutput,
    EngineRunOutputWithSegmentPositions, EngineSchemaAuthority, EngineSegmentPosition,
    EstimateExplain, ExecutionProfile, ExplainData, LineageInputObservation, LineageSummary,
    OperatorNode, PREVIEW_POLICY_BALANCED_STRATIFIED_V1, PartitionExplain,
    PhysicalObservationEvidence, PredicateExplain, SCHEMA_ADMISSION_CACHE_KEY_FIELDS,
    SchemaQuarantineObservationEvidence, SourceRetryProgressObserver, StreamAdmissionCompletion,
    StreamAdmissionObservationEvidence,
};
pub use worker_task::{
    AdmittedEnginePartitionEvidence, EngineIsolatedSegmentExecutor, EnginePartitionTaskInput,
    EngineSegmentTaskInput, EngineWorkerAdmissionVerifier, EngineWorkerArtifactAuthority,
    EngineWorkerOutputAuthority, ReconstructedEngineSegmentProgram,
    ReconstructedEngineWorkerProgram, VerifiedCanonicalSegmentArtifact,
    VerifiedEnginePartitionEvidenceArtifact, VerifiedPreparedSegmentArtifact,
    VerifiedWorkerCompilerArtifact, WorkerCompilerArtifactWriter,
    WorkerDecodeUnitAuthorityArtifact, WorkerNormalizationArtifact, WorkerSegmentAuthorityArtifact,
    compile_engine_partition_task, compile_engine_segment_task,
};
