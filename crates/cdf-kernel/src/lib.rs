#![doc = "Core types, traits, and artifact contracts for cdf."]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "foundational production code must propagate recoverable failures"
    )
)]

mod arrow_type;
mod async_types;
mod batch;
mod canonical_arrow;
mod checkpoint;
mod config;
mod content_reachability;
mod correction;
mod destination;
mod error;
mod execution_extent;
mod expression;
mod ids;
mod lease;
mod metadata;
mod position;
mod position_aggregation;
mod resource;
mod retention;
mod run_event;
mod schema_authority;
mod schema_fingerprint;
mod scope;
mod semantic;
mod source_materialization;
mod statistics;
mod stratified_selection;

pub use arrow_type::parse_arrow_field_type;
pub use async_types::{BatchStream, BoxFuture};
pub use batch::{
    Batch, BatchHeader, BatchPayload, CdcMetadata, PayloadRef, PhysicalObservationRepresentation,
    PreContractObservedValue, PreContractPhysicalReconciliation, PreContractQuarantineFact,
    PreContractResidualCandidate, RecordBatchPayload,
};
pub use canonical_arrow::{
    CanonicalArrowDateUnit, CanonicalArrowField, CanonicalArrowIntervalUnit, CanonicalArrowSchema,
    CanonicalArrowTimeUnit, CanonicalArrowType, CanonicalArrowUnionField, CanonicalArrowUnionMode,
};
pub use checkpoint::{
    CHECKPOINT_STATE_VERSION, Checkpoint, CheckpointStatus, CheckpointStore,
    LATE_DATA_CARRYOVER_VERSION, LateDataCarryoverRef, Receipt, RewindReport, RewindRequest,
    StateDelta, StateSegment, validate_late_data_carryover_refs,
};
pub use config::parse_human_byte_size;
pub use content_reachability::{
    CommittedContentMembership, CommittedContentRoot, CommittedContentRootCheck, ContentDigest,
    ContentPublicationClaim, ContentPublicationClaimState, ContentReachabilityStore,
    ContentReclamationCandidate, ContentReclamationProof, ContentReclamationReservation,
    ContentReclamationSnapshot, ContentRootIntent, ContentRootState,
    ExpiredContentPublicationClaim, ImmutableContentIdentity,
};
pub use correction::{
    CorrectionCommitSession, CorrectionStrategy, CorrectionStrategyCapability,
    DESTINATION_CORRECTION_CAPABILITIES_VERSION, DESTINATION_CORRECTION_RECEIPT_EVIDENCE_KEY,
    DESTINATION_CORRECTION_RECEIPT_EVIDENCE_VERSION,
    DESTINATION_CORRECTION_SIDECAR_RECEIPT_EVIDENCE_KEY,
    DESTINATION_CORRECTION_SIDECAR_RECEIPT_EVIDENCE_VERSION,
    DESTINATION_PROTOCOL_CAPABILITIES_VERSION, DestinationCorrectionCapabilities,
    DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest,
    DestinationCorrectionOperation, DestinationCorrectionOperationKind, DestinationCorrectionPlan,
    DestinationCorrectionReceiptEvidence, DestinationCorrectionRequest,
    DestinationCorrectionSidecarObjectEvidence, DestinationCorrectionSidecarReceiptEvidence,
    DestinationProtocolCapabilities, DestinationResidualReadback, DestinationSheetArtifact,
    OBJECT_KEY_RULES_VERSION, ObjectKeyPolicy, ObjectKeyRules, ResidualCorrectionOperation,
    RowProvenanceAddress, RowProvenanceCapabilities, correction_operations_digest,
};
pub use destination::{
    CommitBatch, CommitBatchIterator, CommitCounts, CommitPlan, CommitSegment,
    CommitSegmentIterator, CommitSession, ConcurrencyLimit, DeliveryGuarantee,
    DestinationCommitRequest, DestinationProtocol, DestinationSheet, IdempotencySupport,
    IdentifierRules, MigrationRecord, ReceiptVerification, SegmentAck, TransactionMetadata,
    TransactionSupport, TypeMapping, TypeMappingFidelity, VerifyClause,
};
pub use error::{CdfError, ErrorKind, Result, embedded_cdf_error, is_filesystem_loop};
pub use execution_extent::{
    DrainTermination, EPOCH_CLOSURE_EVIDENCE_VERSION, EPOCH_FRONTIER_VERSION,
    EXECUTION_EXTENT_VERSION, EpochClosureCause, EpochClosureEvidence, EpochClosureObservation,
    EpochClosureTrigger, EpochFrontier, EventTimeDomain, ExecutionExtent, LateDataAction,
    OperatorWatermarkBehavior, PARTITION_IDLENESS_CLAIM_VERSION, PARTITION_WATERMARK_STATE_VERSION,
    PartitionIdlenessClaim, PartitionWatermarkAggregation, PartitionWatermarkState,
    STREAM_EPOCH_POLICY_VERSION, SafeFrontierPolicy, StreamEpochPolicy, WATERMARK_CLAIM_VERSION,
    WatermarkAuthority, WatermarkClaim, WatermarkObservationContext, WatermarkPolicy,
    WatermarkValue, validate_partition_watermark_states,
};
pub use expression::{
    CDF_FUNCTION_NAMESPACE, CDF_FUNCTION_VERSION, DATAFUSION_SCALAR_CONFIG_IDENTITY,
    DATAFUSION_SCALAR_FEATURE_SET, DATAFUSION_SCALAR_IMPLEMENTATION_VERSION,
    DATAFUSION_SCALAR_NAMESPACE, DECLARATIVE_EXPRESSION_VERSION, DeclarativeExpression,
    DeclarativeExpressionLiteral, DeclarativeExpressionNode, DeclarativeFunctionReference,
    SCALAR_EXPRESSION_EXECUTOR_VERSION, SCALAR_EXPRESSION_IR_VERSION, ScalarBinaryOperator,
    ScalarCastMode, ScalarColumnDependency, ScalarDependencies, ScalarExpression,
    ScalarExpressionKind, ScalarExpressionNode, ScalarFunctionReference, ScalarFunctionVolatility,
    ScalarType, ScalarUnaryOperator,
};
pub use ids::{
    BatchId, CheckpointId, CommittedContentRootId, ContentClaimAttemptId, ContentDigestAlgorithm,
    ContentDigestValue, ContentObjectKey, ContentProviderGeneration, ContentPublicationClaimId,
    ContentReclamationCandidateSource, ContentReclamationReservationId, ContentRootShardRef,
    ContentStoreNamespace, ContractRef, DestinationId, DiscoveryManifestHash, EnvironmentName,
    IdempotencyToken, LeaseAuthorityDomainId, LeaseOwnerId, PackageHash, PartitionId, PipelineId,
    PlanId, PredicateId, ProjectId, PromotionId, ReceiptId, ResourceId, RunId, SchemaHash,
    SegmentId, SourceId, TargetName, ValidationProgramHash,
};
pub use lease::{
    ExpiredScopeLeaseProof, FencingToken, PromotionSettlementStore, ScopeLease, ScopeLeaseClock,
    ScopeLeaseStore,
};
pub use metadata::{
    NULL_ORIGIN_METADATA_KEY, PHYSICAL_TYPE_METADATA_KEY, SEMANTIC_METADATA_KEY,
    SOURCE_NAME_METADATA_KEY, null_origin, physical_type, semantic, source_name, with_cdf_metadata,
    with_null_origin, with_physical_type, with_semantic, with_source_name,
};
pub use position::{
    CommittedLogPosition, CommittedLogProtocol, CommittedLogScope, CompositePosition,
    CursorPosition, CursorValue, FileManifest, FilePosition, ForeignState,
    MongoChangeStreamResumeToken, MongoChangeStreamScope, MongoResumeMode, MongoResumeTokenSource,
    MongoWatchLevel, MySqlCommitPosition, MySqlLogScope, PageToken, PostgresCommitPosition,
    PostgresLogScope, ResumeTokenPosition, SOURCE_POSITION_VERSION, SourcePosition,
    SourcePositionKind, TableSnapshotPosition, TableSnapshotSelector,
};
pub use position_aggregation::{
    aggregate_position_set, aggregate_resource_closed_output_position,
    aggregate_resource_output_position, merge_file_position_evidence,
    merge_terminal_position_evidence, same_file_position_identity,
};
pub use resource::{
    BackpressureSupport, COMPILED_SCAN_INTENT_VERSION, CapabilitySupport, CompiledScanIntent,
    CompiledSourcePlanHash, CursorOrderingClaim, CursorSpec, DISCOVERY_MANIFEST_HASH_METADATA_KEY,
    DISCOVERY_MANIFEST_PATH_METADATA_KEY, DeduplicationSpec, DiscoveryCoverageEvidence,
    DiscoveryCoverageEvidenceInput, DiscoveryExecutorBudgetEvidence, DiscoveryManifestReference,
    EFFECTIVE_SCHEMA_EVIDENCE_VERSION, EffectiveSchemaCatalogEntry, EffectiveSchemaEvidence,
    EffectiveSchemaObservationEvidence, EffectiveSchemaRuntime, EstimateSupport,
    ExecutablePartition, FilterCapabilities, FreshnessSpec, IncrementalShape,
    InvocationTermination, OpenedPartitionStream, OrderBy, PLAN_PHYSICAL_SCHEMA_HASH_KEY,
    PLAN_SCHEMA_OBSERVATION_BINDING_KEY, PLAN_SCHEMA_OBSERVATION_ID_KEY,
    PLANNED_TASK_SET_REFERENCE_VERSION, PartitionAttestation, PartitionAttestationAttempt,
    PartitionAuthority, PartitionCompletion, PartitionOpenAttempt, PartitionPlan,
    PartitionRetrySafety, PartitionStreamPayload, PartitioningCapabilities, PhysicalSourcePlanHash,
    PlannedPartitionReader, PlannedSourceBytes, PlannedTaskSetReference,
    ProcessedObservationOutcome, ProcessedObservationPosition, PushdownFidelity, PushedPredicate,
    QueryableResource, ReplaySupport, ResourceCapabilities, ResourceDescriptor, ResourceStream,
    ScanPlan, ScanPredicate, ScanRequest, SchemaBaselineReference, SchemaObservationBinding,
    SchemaObservationFieldQuarantine, SchemaObservationScope, SchemaSnapshotReference,
    SchemaSource, SortDirection, SourceBoundaryCapabilities, SourceCopyClassification,
    SourceDiscoveryBinding, SourceExecutionLane, SourceIoMetrics, SourceReadMode,
    SourceReplayRetention, SourceReplayRetentionStatus, SourceSemanticsHash, SourceTransferMode,
    SourceTransferModeReport, SourceTransferReport, TerminalSchemaObservationQuarantine,
    TrustLevel, TypePolicyAllowances, WriteDisposition, aggregate_processed_observation_positions,
    bind_partition_schema_candidate, bind_partition_schema_observation,
    derive_partition_schema_observation_binding, discovery_manifest_from_metadata,
    insert_discovery_manifest_metadata, partition_schema_observation_binding,
    partition_schema_observation_id, validate_compiled_scan_intents,
    validate_scan_partition_observation_identities,
};
pub use retention::PayloadRetention;
pub use run_event::{
    PROMOTION_PUBLICATION_EVENT_VERSION, PromotionPublicationEvent, PromotionPublicationTarget,
    RunEvent, RunEventAppend, RunEventDetails, RunEventKind, RunEventSink, RunEventSinkResult,
    RunEventValue, RunPhase, RunPhaseContext, RunPhaseMetric, RunPhaseStatus,
    RunProgressObservation, RunProgressObservationKind, RunProgressSink, SecretReference,
};
pub use schema_authority::{
    MAX_SCHEMA_AUTHORITY_HISTORY_LIMIT, SchemaAuthorityCheck, SchemaAuthorityEstablishment,
    SchemaAuthorityEvent, SchemaAuthorityEventKind, SchemaAuthorityKey,
    SchemaAuthorityPrecondition, SchemaAuthorityStore, SchemaHead, SchemaHeadStatus,
    SchemaPromotionFence, SchemaVersion, SchemaVersionProvenance,
};
pub use schema_fingerprint::canonical_arrow_schema_hash;
pub use scope::{ScopeKey, ScopeKind};
pub use semantic::{
    CDF_PACKAGE_ROW_ORDINAL_SEMANTIC, SemanticParameterValue, SemanticReference,
    SemanticReferenceError,
};
pub use source_materialization::{SourceMaterializationRule, validate_source_materializations};
pub use statistics::{
    BatchStats, ColumnStats, IncompleteStatisticsReason, STATISTICS_MODEL_VERSION,
    StatisticsArrowField, StatisticsArrowType, StatisticsArrowUnionField, StatisticsCompleteness,
    StatisticsMetadata, TypedScalar,
};
pub use stratified_selection::{
    OrderedStratifiedHashV1, STRATIFIED_HASH_SELECTOR_V1, StratifiedHashBoundedIdentity,
    StratifiedHashCandidate, StratifiedHashIdentityStrength, StratifiedHashPlan,
    StratifiedHashSelection, StratifiedHashSelectionChange, StratifiedHashStratum,
    plan_stratified_hash_v1, stratified_hash_v1_score,
};

#[cfg(test)]
mod tests;
