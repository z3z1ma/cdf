#![doc = "Project configuration and orchestration boundary for cdf."]

#[cfg(test)]
#[allow(
    unused_imports,
    reason = "white-box crate tests aggregate project internals"
)]
use std::{
    collections::{BTreeMap, BTreeSet},
    env, fmt, fs,
    path::{Path, PathBuf},
};

#[cfg(test)]
#[allow(
    unused_imports,
    reason = "white-box crate tests aggregate project internals"
)]
use cdf_contract::NORMALIZER_NAMECASE_V1;
#[cfg(test)]
#[allow(
    unused_imports,
    reason = "white-box crate tests aggregate project internals"
)]
use cdf_http::{SecretProvider, SecretUri, SecretValue};
#[cfg(test)]
#[allow(
    unused_imports,
    reason = "white-box crate tests aggregate project internals"
)]
use cdf_kernel::{
    CdfError, DestinationSheet, ResourceCapabilities, ResourceDescriptor, Result, SchemaSource,
};
#[cfg(test)]
#[allow(
    unused_imports,
    reason = "white-box crate tests aggregate project internals"
)]
use cdf_runtime::SourceRegistry;
#[cfg(test)]
#[allow(
    unused_imports,
    reason = "white-box crate tests aggregate project internals"
)]
use cdf_semantic::SemanticCatalog;
#[cfg(test)]
#[allow(
    unused_imports,
    reason = "white-box crate tests aggregate project internals"
)]
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
#[cfg(test)]
#[allow(
    unused_imports,
    reason = "white-box crate tests aggregate project internals"
)]
use sha2::{Digest, Sha256};

pub const PROJECT_FILE_NAME: &str = "cdf.toml";
mod backfill;
mod compilation;
mod configuration;
mod discovery_manifest;
mod internal;
mod manifest;
mod models;
mod observation_cache;
mod portable_plan;
mod project_files;
mod project_inputs;
mod promotion;
mod query_compiler;
mod resource_selector;
mod resource_sql;
mod runtime;
#[cfg(test)]
mod runtime_tests;
mod scaffold;
mod schema_discovery;
mod schema_snapshot;
mod secrets;
mod semantic_uses;
mod static_validation;
#[cfg(test)]
mod test_destinations;
#[cfg(test)]
mod tests;

pub use backfill::{
    BACKFILL_PIPELINE_ID, BackfillPlan, BackfillPlanRequest, BackfillSlice, backfill_pipeline_id,
    plan_backfill,
};
pub use compilation::{
    COMPILATION_INDEX_RELATIVE_PATH, COMPILED_RESOURCE_ARTIFACT_VERSION,
    COMPILED_RESOURCE_DIRECTORY, CompilationArtifactReference, CompilationDiagnostic,
    CompilationIndex, CompilationIndexEntry, CompilationSnapshot, CompilationStatus,
    CompiledResourceArtifact, CompiledResourceArtifactRequest, CompiledSchemaAuthority,
    compile_resource_artifact, compiled_resource_artifact_path, effective_environment_binding_hash,
    hydrate_compiled_resource_artifact, load_compilation_snapshot, parse_compilation_index,
    parse_compiled_resource_artifact, validate_compilation_index_authority,
    validate_compiled_resource_artifact_current,
};
pub use configuration::{
    ProjectValidationReport, SecretCheck, SecretCheckStatus, parse_cdf_toml, validate_project,
};
pub use discovery_manifest::{
    DEFAULT_DISCOVERY_MAX_BYTES_PER_FILE, DEFAULT_DISCOVERY_MAX_CONCURRENT_PROBES,
    DEFAULT_DISCOVERY_MAX_RECORDS_PER_FILE, DEFAULT_DISCOVERY_MAX_TOTAL_IN_FLIGHT_BYTES,
    DISCOVERY_MANIFEST_ARTIFACT_VERSION, DISCOVERY_MANIFEST_SUFFIX, DiscoveryBoundedIdentity,
    DiscoveryCandidateEvidence, DiscoveryExecutorBudget, DiscoveryFileCoverage,
    DiscoveryIdentityStrength, DiscoveryManifestArtifact, DiscoveryManifestHashInput,
    DiscoveryManifestInput, DiscoveryManifestStore, DiscoveryMetadataScope,
    DiscoveryMetadataVariance, DiscoveryParticipation, DiscoverySchemaVerdict,
    DiscoverySchemaVerdictKind, DiscoverySelectorEvidence, DiscoverySelectorSelection,
    DiscoverySelectorStratum, DiscoveryWithinFileCoverage, STRATIFIED_HASH_SELECTOR_V1,
    discovery_manifest_relative_path,
};
pub use manifest::{
    CompiledArtifactInput, ContractSnapshot, DependencyTuple, ManifestDestinationBinding,
    ManifestDiagnostic, ManifestDiagnosticSeverity, ManifestField, ManifestInputContentHash,
    ManifestInputGeneration, ManifestInputKind, ManifestInputLocation, ManifestLineageEdge,
    ManifestLineageKind, ManifestLineageNode, ManifestResource, ManifestResourceOrigin,
    ManifestSemanticDefinition, ManifestSemanticFieldUsage, ManifestSemanticReferenceUsage,
    ManifestSemanticSource, ResourceCompilationHash, SemanticProfileHash, current_dependency_tuple,
};
pub use models::{
    DefaultsConfig, DestinationPolicy, DurationSpec, EffectiveEnvironment, EnvironmentConfig,
    ProjectConfig, ProjectMetadata, ProjectSourceConfig, ProjectSourceOverlay, RetentionPolicy,
    RetentionRule, TrustPreset, WriteDispositionPreset,
};
pub use observation_cache::{
    DEFAULT_OBSERVATION_CACHE_MAX_BYTES, DEFAULT_OBSERVATION_CACHE_MAX_ENTRIES,
    DEFAULT_OBSERVATION_CACHE_MAX_ENTRY_BYTES, OBSERVATION_CACHE_ARTIFACT_VERSION,
    ObservationCacheEntry, ObservationCacheKey, ObservationCacheLookup, ObservationCacheMissReason,
    ObservationCachePolicy, ObservationCacheStore, ObservationCacheStoreOutcome,
    StrongObservationSourceIdentity,
};
pub use portable_plan::{
    PORTABLE_PLAN_MAX_BYTES, PORTABLE_PLAN_VERSION, PortableDestinationBinding,
    PortableHostRequirements, PortableInlineArtifact, PortablePlanArtifact,
    PortablePlanFailurePolicy, PortablePlanResource, PortableSchemaAuthority,
    PortableTaskSetArtifact, parse_portable_plan, sha256 as portable_plan_sha256,
};
pub use project_files::{
    ProjectFileExpectation, ProjectFileGuard, ProjectFileTransactionReport, ProjectFileWrite,
    project_file_transaction_generation, publish_project_files_transactionally,
    publish_project_files_transactionally_guarded,
    publish_project_files_transactionally_guarded_without_recovery,
    publish_project_files_transactionally_without_recovery, recover_project_file_transaction,
};
pub use project_inputs::{
    ProjectResourceInput, ProjectResourceInventory, ProjectResourceName, ProjectResourceNamespace,
    ProjectResourcePath, ProjectSourceBinding, ProjectSourceConfigurationHash, ProjectSourceName,
    effective_project_source_config, inventory_project_resources,
};
pub use promotion::{
    CorrectionStrategySelection, CorrectionStrategySelectionRule,
    LocalPackagePromotionAvailability, LocalPackagePromotionEvidenceInventory,
    LocalPromotionAvailabilityStatus, LocalPromotionCollectionAction,
    LocalPromotionCollectionAssessment, LocalPromotionReceiptTarget, PromotionEvidenceInventory,
    SchemaPromotionCoercionVerdict, SchemaPromotionConflict, SchemaPromotionEvidenceAvailability,
    SchemaPromotionEvidenceInventoryFacts, SchemaPromotionEvidenceReport,
    SchemaPromotionFreshDiscovery, SchemaPromotionMigrationReport,
    SchemaPromotionPackageTargetAssociation, SchemaPromotionPathReport, SchemaPromotionPlanReport,
    SchemaPromotionPlanningAuthority, SchemaPromotionReceiptReport,
    SchemaPromotionReceiptVerification, SchemaPromotionResidualPathFacts,
    SchemaPromotionResourceAttribution, SchemaPromotionSnapshotPlan,
    SchemaPromotionTargetEvidenceReport, SchemaPromotionTargetReport, SchemaPromotionWrites,
    assess_local_promotion_collection, inspect_local_package_promotion_availability,
    plan_schema_promotion, recompute_schema_promotion_id, select_correction_strategy,
    validate_schema_promotion_plan_identity,
};
pub use query_compiler::{
    CompiledProjectResource, EffectiveResourceEnvelope, ProjectConfiguredSourceIdentity,
    ProjectInputSchemaAuthority, ProjectQueryCompilation, ResolutionOrigin, ResolvedResourceValue,
    compile_query_project_resources, compile_selected_query_project_resources,
    finalize_query_project_resource,
};
pub use resource_selector::{
    ProjectResourceSelection, ProjectResourceSelectionError, ProjectResourceSelectionResolution,
    resolve_project_resource_selection,
};
pub use resource_sql::{
    AuthoredDisposition, AuthoredResourceEnvelope, AuthoredResourceFile, AuthoredResourceForm,
    AuthoredSemanticBinding, SpannedResourceValue, parse_resource_file,
};
pub use runtime::{
    DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS, DestinationCommitPlanningInputs,
    DestinationCommitPlanningOutcome, DestinationPlanningContext,
    DestinationReceiptReportingPolicy, FileManifestRunSummary, PackageArtifactRecoveryRequest,
    PackageArtifactReplayRequest, PackageReplayReport, PreparedDestinationCommit,
    ProjectDestinationCommitPlan, ProjectDestinationDescription, ProjectDestinationDriver,
    ProjectDestinationRegistry, ProjectDestinationRuntime, ProjectDestinationSyntheticInput,
    ProjectDrainEpochReport, ProjectDrainRunReport, ProjectReceiptSource, ProjectResolutionContext,
    ProjectRunNoOpReason, ProjectRunNoOpReport, ProjectRunOutcome, ProjectRunReport,
    ProjectRunRequest, ProjectRunSource, ReceiptVerifiedHook, ResolvedProjectDestination,
    RunTelemetryConfig, RuntimeStage, RuntimeStageHook,
    SCHEMA_PROMOTION_CORRECTION_PACKAGE_VERSION,
    SCHEMA_PROMOTION_CORRECTION_TARGET_AUTHORITY_VERSION,
    SCHEMA_PROMOTION_EXECUTION_ARTIFACT_VERSION, SchemaPromotionCorrectionPackageArtifact,
    SchemaPromotionExecutionFailpoint, SchemaPromotionExecutionPhase,
    SchemaPromotionExecutionPlanArtifact, SchemaPromotionExecutionReport,
    SchemaPromotionExecutionRequest, SchemaPromotionExecutionTargetReport, StateStorePathOwnership,
    TracingRunEventSink, WindowScopedResource, absolute_under_root, commit_request,
    ensure_state_parent_directory, execute_schema_promotion, inspect_local_promotion_availability,
    local_uri_path, recover_package_from_artifacts, replay_package_from_artifacts,
    replay_package_from_artifacts_with_stage_hook, resolve_project_run_destination, run_project,
    run_project_with_scheduler_and_telemetry, run_project_with_telemetry,
};
pub use scaffold::{ProjectScaffoldOptions, ProjectScaffoldReport, write_local_project_scaffold};
pub use schema_discovery::{
    DiscoveredSchemaSnapshot, PreparedSchemaResource, ResourceSchemaDiscovery,
    ResourceSchemaDiscoveryArtifacts, SchemaDiscoveryExecutionOptions, SchemaDiscoveryWriteOutcome,
    VerifiedSchemaBaseline, apply_discovered_schema, apply_discovered_schema_constraints,
    compile_discovered_schema_artifacts, discover_resource_schema_with_source_registry,
    prepare_cached_resource_schema, prepare_cached_resource_schema_artifacts,
    write_schema_discovery_artifacts,
};
pub use schema_snapshot::{
    SCHEMA_SNAPSHOT_ARTIFACT_VERSION, SCHEMA_SNAPSHOT_DIR,
    SCHEMA_SNAPSHOT_PROMOTION_AUTHORITY_VERSION, SchemaSnapshotArtifact, SchemaSnapshotDataType,
    SchemaSnapshotDateUnit, SchemaSnapshotField, SchemaSnapshotHashInput,
    SchemaSnapshotIntervalUnit, SchemaSnapshotPromotionAuthority,
    SchemaSnapshotPromotionCoercionAuthority, SchemaSnapshotPromotionEvidenceAvailability,
    SchemaSnapshotPromotionPathAuthority, SchemaSnapshotPromotionTargetAssociationAuthority,
    SchemaSnapshotSchema, SchemaSnapshotStore, SchemaSnapshotTimeUnit, SchemaSnapshotUnionField,
    SchemaSnapshotUnionMode, schema_snapshot_relative_path,
};
pub use secrets::{DefaultSecretProvider, EnvSecretProvider, FileSecretProvider, SecretRef};
pub use static_validation::{
    LocalAuthorityStatus, ProjectStaticValidationCounts, ProjectStaticValidationDiagnostic,
    ProjectStaticValidationEffects, ProjectStaticValidationReport, ProjectStaticValidationResource,
    StaticValidationSeverity, validate_project_static,
};
