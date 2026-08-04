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
use cdf_declarative::{
    CompiledResource, DeclarativeDocument, compile_document, compile_document_with_project_root,
    parse_toml as parse_declarative_toml, parse_yaml as parse_declarative_yaml,
};
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
pub const LOCK_FILE_NAME: &str = "cdf.lock";
pub const LOCKFILE_VERSION: u16 = 2;

mod backfill;
mod discovery_manifest;
mod internal;
mod lock_cas;
mod lockfile;
mod manifest;
mod models;
mod observation_cache;
mod project_files;
mod project_inputs;
mod promotion;
mod query_compiler;
mod resource_sql;
mod runtime;
#[cfg(test)]
mod runtime_tests;
mod scaffold;
mod schema_discovery;
mod schema_snapshot;
mod secrets;
mod semantic_uses;
mod sources;
#[cfg(test)]
mod test_destinations;
#[cfg(test)]
mod tests;

pub use backfill::{
    BACKFILL_PIPELINE_ID, BackfillPlan, BackfillPlanRequest, BackfillSlice, backfill_pipeline_id,
    plan_backfill,
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
pub use lock_cas::{
    LockFileAtomicityCapabilities, LockFileAuthority, LockFileCasFailpoint, LockFileCasReport,
    LockFileMutationGuard, acquire_lock_file_mutation_guard, compare_and_swap_lock_file,
    compare_and_swap_lock_file_with_failpoint, lock_file_atomicity_capabilities,
    read_lock_file_authority, write_lock_file_guarded,
};
pub use lockfile::{
    CdfLock, CompiledProjectResource, ContractFreezeReport, ContractSnapshot,
    ContractSnapshotComparison, ContractSnapshotCounts, ContractSnapshotDrift,
    ContractSnapshotVerdict, ContractTestReport, DependencyTuple, LockDiff, LockDiffKind,
    LockedDestination, LockedResource, ProjectLock, ProjectResourceOrigin, ProjectValidationReport,
    SecretCheck, SecretCheckStatus, compile_project_declarative_resource_entries_with_root,
    compile_project_declarative_resource_entries_with_root_and_semantic_catalog,
    compile_project_declarative_resources, compile_project_declarative_resources_with_root,
    compile_project_declarative_resources_with_semantic_catalog, contract_snapshot_for_resource,
    contract_snapshot_for_resource_with_semantic_catalog, contract_snapshots_for_resources,
    contract_snapshots_for_resources_with_semantic_catalog, current_dependency_tuple,
    diff_lockfiles, freeze_contract_snapshots, generate_lockfile_with_destination_artifacts,
    lock_to_toml, parse_cdf_toml, parse_lock, pin_schema_snapshot_in_project_lockfile,
    test_contract_snapshots, test_contract_snapshots_with_semantic_catalog, validate_project,
};
pub use manifest::{
    AuthoredInputSetHash, DependencyTupleHash, EnvironmentBindingHash, LineageHash,
    ManifestDestinationBinding, ManifestDiagnostic, ManifestDiagnosticSeverity, ManifestField,
    ManifestInputContentHash, ManifestInputGeneration, ManifestInputKind, ManifestInputLocation,
    ManifestLineageEdge, ManifestLineageKind, ManifestLineageNode, ManifestResource,
    ManifestResourceOrigin, ManifestSemanticDefinition, ManifestSemanticFieldUsage,
    ManifestSemanticReferenceUsage, ManifestSemanticSource, PROJECT_MANIFEST_MAX_BYTES,
    PROJECT_MANIFEST_RELATIVE_PATH, PROJECT_MANIFEST_VERSION, ProjectCompilationMode,
    ProjectLockBindingHash, ProjectLockContentHash, ProjectLockSemanticHash, ProjectManifest,
    ProjectManifestAuthoredInput, ProjectManifestCompileRequest, ProjectManifestHash,
    ProjectManifestHashes, ProjectManifestHeader, ProjectManifestSnapshot, ResourceCompilationHash,
    SemanticProfileHash, SemanticSnapshotHash, compile_project_manifest,
    load_project_manifest_snapshot, parse_project_manifest, publish_project_manifest,
    publish_project_manifest_and_lock, validate_project_manifest_authority,
};
pub use models::{
    DefaultsConfig, DestinationPolicy, DurationSpec, EffectiveEnvironment, EnvironmentConfig,
    ProjectConfig, ProjectFreshness, ProjectMetadata, ProjectResource, ProjectSourceConfig,
    ProjectSourceOverlay, ResourceSourceKind, RetentionPolicy, RetentionRule, TrustPreset,
    WriteDispositionPreset,
};
pub use observation_cache::{
    DEFAULT_OBSERVATION_CACHE_MAX_BYTES, DEFAULT_OBSERVATION_CACHE_MAX_ENTRIES,
    DEFAULT_OBSERVATION_CACHE_MAX_ENTRY_BYTES, OBSERVATION_CACHE_ARTIFACT_VERSION,
    ObservationCacheEntry, ObservationCacheKey, ObservationCacheLookup, ObservationCacheMissReason,
    ObservationCachePolicy, ObservationCacheStore, ObservationCacheStoreOutcome,
    StrongObservationSourceIdentity,
};
pub use project_files::{
    ProjectFileExpectation, ProjectFileTransactionReport, ProjectFileWrite,
    project_file_transaction_generation, publish_project_files_transactionally,
    publish_project_files_transactionally_without_recovery, recover_project_file_transaction,
};
pub use project_inputs::{
    ProjectResourceInput, ProjectResourceInventory, ProjectResourceName, ProjectResourceNamespace,
    ProjectSourceBinding, ProjectSourceConfigurationHash, ProjectSourceName,
    inventory_project_resources,
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
    SchemaPromotionReceiptReport, SchemaPromotionReceiptVerification,
    SchemaPromotionResidualPathFacts, SchemaPromotionResourceAttribution,
    SchemaPromotionSnapshotPlan, SchemaPromotionTargetEvidenceReport, SchemaPromotionTargetReport,
    SchemaPromotionWrites, assess_local_promotion_collection,
    inspect_local_package_promotion_availability, plan_schema_promotion,
    recompute_schema_promotion_id, select_correction_strategy,
    validate_schema_promotion_plan_identity,
};
pub use query_compiler::{
    CompiledQueryProjectResource, EffectiveResourceEnvelope, ProjectConfiguredSourceIdentity,
    ProjectInputSchemaAuthority, ProjectQueryCompilation, ResolutionOrigin, ResolvedResourceValue,
    compile_query_project_resources, finalize_query_project_resource,
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
    SCHEMA_PROMOTION_EXECUTION_ARTIFACT_VERSION, SCHEMA_PROMOTION_RECOVERY_STATUS_VERSION,
    SchemaPromotionCorrectionPackageArtifact, SchemaPromotionExecutionFailpoint,
    SchemaPromotionExecutionPhase, SchemaPromotionExecutionPlanArtifact,
    SchemaPromotionExecutionReport, SchemaPromotionExecutionRequest,
    SchemaPromotionExecutionTargetReport, SchemaPromotionRecoveryStatus, StateStorePathOwnership,
    TracingRunEventSink, WindowScopedResource, absolute_under_root, commit_request,
    ensure_state_parent_directory, execute_schema_promotion, inspect_local_promotion_availability,
    load_resumable_schema_promotion, load_schema_promotion_recovery_status, local_uri_path,
    promotion_plan_relative_path, recover_package_from_artifacts, replay_package_from_artifacts,
    replay_package_from_artifacts_with_stage_hook, resolve_project_run_destination, run_project,
    run_project_with_scheduler_and_telemetry, run_project_with_telemetry,
};
pub use scaffold::{ProjectScaffoldOptions, ProjectScaffoldReport, write_local_project_scaffold};
pub use schema_discovery::{
    DiscoveredSchemaSnapshot, PreparedSchemaResource, ResourceSchemaDiscovery,
    ResourceSchemaDiscoveryArtifacts, SchemaDiscoveryExecutionOptions, SchemaDiscoveryWriteOutcome,
    VerifiedSchemaBaseline, apply_discovered_schema, apply_discovered_schema_constraints,
    compile_discovered_schema_artifacts, discover_resource_schema_with_source_registry,
    preflight_fixed_resource_schema_with_source_registry, prepare_pinned_resource_schema,
    prepare_pinned_resource_schema_artifacts, write_schema_discovery_artifacts,
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
pub use sources::{
    FileResourceSourceResolver, InMemoryResourceSourceResolver, ResolvedResourceSource,
    ResourceSourceResolver,
};
