use std::{
    collections::BTreeMap,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{
    DiscoveryBoundedIdentity, DiscoveryCandidateEvidence, DiscoveryExecutorBudget,
    DiscoveryFileCoverage, DiscoveryIdentityStrength, DiscoveryManifestArtifact,
    DiscoveryManifestInput, DiscoveryManifestStore, DiscoveryMetadataScope,
    DiscoveryMetadataVariance, DiscoveryParticipation, DiscoverySchemaVerdict,
    DiscoverySchemaVerdictKind, DiscoverySelectorCandidate, DiscoveryWithinFileCoverage,
    ObservationCacheEntry, ObservationCacheKey, ObservationCacheLookup, ObservationCacheMissReason,
    ObservationCacheStore, ObservationCacheStoreOutcome, SchemaSnapshotArtifact,
    SchemaSnapshotStore, StrongObservationSourceIdentity, plan_discovery_selection,
};
use cdf_contract::{
    AggregateFileSchemaVerdict, AggregateMetadataVariance, AggregateSchemaCandidate,
    ContractPolicy, IdentifierPolicy, NORMALIZER_NAMECASE_V1, RuleOutcome, SchemaEvolutionMode,
    normalize_arrow_schema, plan_aggregate_arrow_schema_join, plan_schema_reconciliation,
    reconcile_schema,
};
use cdf_declarative::CompiledResource;
use cdf_kernel::{
    CdfError, DISCOVERY_MANIFEST_HASH_METADATA_KEY, DISCOVERY_MANIFEST_PATH_METADATA_KEY,
    DiscoveryCoverageEvidence, DiscoveryCoverageEvidenceInput, DiscoveryExecutorBudgetEvidence,
    EffectiveSchemaCatalogEntry, EffectiveSchemaEvidence, EffectiveSchemaObservationEvidence,
    EffectiveSchemaRuntime, Result, SchemaBaselineReference, SchemaHash,
    SchemaObservationFieldQuarantine, SchemaObservationPolicy, SchemaSource,
    TerminalSchemaObservationQuarantine,
};
use cdf_memory::{
    BudgetTag, ConsumerKey, DeterministicMemoryCoordinator, MemoryClass, MemoryCoordinator,
    ReservationRequest,
};
use cdf_runtime::{
    CompiledSourcePlan, SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest,
    SourceDiscoverySession, SourceRegistry, SourceResolutionContext,
};

#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct PreparedSchemaResource {
    resource: CompiledResource,
    discovery_manifest: Option<DiscoveryManifestArtifact>,
}

impl PreparedSchemaResource {
    pub fn resource(&self) -> &CompiledResource {
        &self.resource
    }

    pub fn discovery_manifest(&self) -> Option<&DiscoveryManifestArtifact> {
        self.discovery_manifest.as_ref()
    }

    pub fn into_parts(self) -> (CompiledResource, Option<DiscoveryManifestArtifact>) {
        (self.resource, self.discovery_manifest)
    }
}

#[derive(Clone, Debug)]
pub struct ResourceSchemaDiscovery {
    pub normalized_schema: arrow_schema::SchemaRef,
    pub snapshot: DiscoveredSchemaSnapshot,
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct ResourceSchemaDiscoveryArtifacts {
    pub discovery: ResourceSchemaDiscovery,
    pub discovery_manifest: Option<DiscoveryManifestArtifact>,
    pub effective_schema_runtime: Option<EffectiveSchemaRuntime>,
}

impl ResourceSchemaDiscoveryArtifacts {
    pub fn new(
        discovery: ResourceSchemaDiscovery,
        discovery_manifest: Option<DiscoveryManifestArtifact>,
    ) -> Self {
        Self {
            discovery,
            discovery_manifest,
            effective_schema_runtime: None,
        }
    }

    pub fn canonical_artifact_files(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let snapshot = &self.discovery.snapshot.artifact;
        let linked_manifest = snapshot.discovery_manifest_reference()?;
        let mut files = Vec::new();
        match (linked_manifest, self.discovery_manifest.as_ref()) {
            (Some(reference), Some(manifest)) if reference == manifest.reference() => {
                if manifest.resource_id != snapshot.resource_id {
                    return Err(CdfError::data(format!(
                        "discovery manifest {} belongs to resource {} but schema snapshot belongs to {}",
                        manifest.path, manifest.resource_id, snapshot.resource_id
                    )));
                }
                files.push((manifest.path.clone(), manifest.canonical_bytes()?));
            }
            (None, None) => {}
            (Some(reference), Some(manifest)) => {
                return Err(CdfError::data(format!(
                    "schema snapshot references discovery manifest {} but prepared artifact is {}",
                    reference.path, manifest.path
                )));
            }
            (Some(reference), None) => {
                return Err(CdfError::data(format!(
                    "schema snapshot references missing discovery manifest {}",
                    reference.path
                )));
            }
            (None, Some(manifest)) => {
                return Err(CdfError::data(format!(
                    "prepared discovery manifest {} is not linked from its schema snapshot",
                    manifest.path
                )));
            }
        }
        files.push((snapshot.path.clone(), snapshot.canonical_bytes()?));
        Ok(files)
    }
}

/// Authority token proving that a schema snapshot and its linked discovery
/// evidence were hydrated and verified by [`SchemaSnapshotStore`].
///
/// Callers cannot construct this token from an arbitrary hash. Obtain it with
/// [`SchemaSnapshotStore::read_with_verified_baseline`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedSchemaBaseline {
    resource_id: cdf_kernel::ResourceId,
    snapshot: cdf_kernel::SchemaSnapshotReference,
    schema: arrow_schema::SchemaRef,
    baseline_observation_schema_catalog: BTreeMap<SchemaHash, cdf_kernel::CanonicalArrowSchema>,
}

impl VerifiedSchemaBaseline {
    pub(crate) fn from_hydrated_snapshot(
        resource_id: cdf_kernel::ResourceId,
        snapshot: cdf_kernel::SchemaSnapshotReference,
        schema: arrow_schema::SchemaRef,
        baseline_observation_schema_catalog: BTreeMap<SchemaHash, cdf_kernel::CanonicalArrowSchema>,
    ) -> Self {
        Self {
            resource_id,
            snapshot,
            schema,
            baseline_observation_schema_catalog,
        }
    }

    pub fn resource_id(&self) -> &cdf_kernel::ResourceId {
        &self.resource_id
    }

    pub fn schema_hash(&self) -> &SchemaHash {
        &self.snapshot.schema_hash
    }

    pub fn snapshot(&self) -> &cdf_kernel::SchemaSnapshotReference {
        &self.snapshot
    }

    pub fn schema(&self) -> &arrow_schema::SchemaRef {
        &self.schema
    }

    pub fn contains_baseline_observation_schema(&self, schema_hash: &SchemaHash) -> bool {
        self.baseline_observation_schema_catalog
            .contains_key(schema_hash)
    }
}

#[derive(Clone, Debug)]
enum RuntimeSchemaBaseline {
    Pinned(VerifiedSchemaBaseline),
    Declared {
        resource_id: cdf_kernel::ResourceId,
        reference: SchemaBaselineReference,
        schema: arrow_schema::SchemaRef,
    },
}

impl RuntimeSchemaBaseline {
    fn resource_id(&self) -> &cdf_kernel::ResourceId {
        match self {
            Self::Pinned(baseline) => baseline.resource_id(),
            Self::Declared { resource_id, .. } => resource_id,
        }
    }

    fn reference(&self) -> SchemaBaselineReference {
        match self {
            Self::Pinned(baseline) => SchemaBaselineReference::Pinned {
                snapshot: baseline.snapshot().clone(),
            },
            Self::Declared { reference, .. } => reference.clone(),
        }
    }

    fn schema(&self) -> &arrow_schema::SchemaRef {
        match self {
            Self::Pinned(baseline) => baseline.schema(),
            Self::Declared { schema, .. } => schema,
        }
    }

    fn contains_baseline_observation_schema(&self, schema_hash: &SchemaHash) -> bool {
        match self {
            Self::Pinned(baseline) => baseline.contains_baseline_observation_schema(schema_hash),
            Self::Declared { .. } => false,
        }
    }

    fn admits_evolution(&self) -> bool {
        matches!(self, Self::Pinned(_))
    }

    fn effective_schema_identity(&self, observed: &SchemaHash) -> SchemaHash {
        match self {
            Self::Pinned(_) => observed.clone(),
            Self::Declared { reference, .. } => reference.schema_hash().clone(),
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Default)]
pub struct SchemaDiscoveryExecutionOptions {
    budget: DiscoveryExecutorBudget,
    runtime_baseline: Option<RuntimeSchemaBaseline>,
    memory_coordinator: Option<Arc<dyn MemoryCoordinator>>,
    observation_cache: Option<ObservationCacheStore>,
}

impl std::fmt::Debug for SchemaDiscoveryExecutionOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchemaDiscoveryExecutionOptions")
            .field("budget", &self.budget)
            .field("runtime_baseline", &self.runtime_baseline)
            .field("memory_coordinator", &self.memory_coordinator.is_some())
            .field("observation_cache", &self.observation_cache.is_some())
            .finish()
    }
}

impl SchemaDiscoveryExecutionOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_budget(mut self, budget: DiscoveryExecutorBudget) -> Self {
        self.budget = budget;
        self
    }

    /// Binds discovery to a baseline previously hydrated and verified by the
    /// snapshot store. Arbitrary hashes are intentionally not accepted.
    pub fn with_verified_baseline(mut self, baseline: VerifiedSchemaBaseline) -> Self {
        self.runtime_baseline = Some(RuntimeSchemaBaseline::Pinned(baseline));
        self
    }

    pub fn with_memory_coordinator(mut self, coordinator: Arc<dyn MemoryCoordinator>) -> Self {
        self.memory_coordinator = Some(coordinator);
        self
    }

    pub fn with_observation_cache(mut self, cache: ObservationCacheStore) -> Self {
        self.observation_cache = Some(cache);
        self
    }

    pub fn budget(&self) -> &DiscoveryExecutorBudget {
        &self.budget
    }

    fn with_declared_baseline(mut self, resource: &CompiledResource) -> Result<Self> {
        let reference = resource
            .descriptor()
            .schema_source
            .baseline_reference()
            .ok_or_else(|| {
                CdfError::contract(
                    "declared runtime schema observation requires a declared schema baseline",
                )
            })?;
        if !matches!(&reference, SchemaBaselineReference::Declared { .. }) {
            return Err(CdfError::contract(
                "declared runtime schema observation received a non-declared baseline",
            ));
        }
        self.runtime_baseline = Some(RuntimeSchemaBaseline::Declared {
            resource_id: resource.descriptor().resource_id.clone(),
            reference,
            schema: resource.schema(),
        });
        Ok(self)
    }

    fn runtime_baseline(&self) -> Option<&RuntimeSchemaBaseline> {
        self.runtime_baseline.as_ref()
    }

    fn runtime_baseline_hash_for(
        &self,
        resource_id: &cdf_kernel::ResourceId,
    ) -> Result<Option<SchemaHash>> {
        match self.runtime_baseline.as_ref() {
            Some(baseline) if baseline.resource_id() != resource_id => {
                Err(CdfError::contract(format!(
                    "runtime schema baseline belongs to resource `{}` but discovery is for `{resource_id}`",
                    baseline.resource_id()
                )))
            }
            Some(baseline) => Ok(Some(baseline.reference().schema_hash().clone())),
            None => Ok(None),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SchemaDiscoveryWriteOutcome {
    pub manifest_written: bool,
    pub snapshot_written: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DiscoveredSchemaSnapshot {
    pub artifact: SchemaSnapshotArtifact,
    pub reference: cdf_kernel::SchemaSnapshotReference,
    pub source_identity: BTreeMap<String, String>,
}

/// Discovers a resource exclusively through its compiled source driver.
///
/// The project layer owns shared selection, reconciliation, snapshot, manifest,
/// and runtime evidence. The registered source owns inventory and bounded
/// physical observation; no concrete source type crosses this boundary.
pub fn discover_resource_schema_with_source_registry(
    resource: &CompiledResource,
    registry: &SourceRegistry,
    plan: &CompiledSourcePlan,
    context: &SourceResolutionContext<'_>,
    options: SchemaDiscoveryExecutionOptions,
) -> Result<ResourceSchemaDiscoveryArtifacts> {
    ensure_discover_schema_mode(resource)?;
    plan.validate_schema_authority(
        resource.descriptor(),
        resource.schema().as_ref(),
        resource.effective_schema_runtime(),
        resource.baseline_observation_schema_catalog(),
    )?;
    let session = registry.discovery_session(plan, context)?;
    let candidates = session
        .candidates()?
        .into_iter()
        .map(DiscoveryCandidate::from_registered)
        .collect::<Result<Vec<_>>>()?;
    discover_registered_resource_schema(resource, options, plan, session.as_ref(), candidates)
}

/// Explicitly observes a fixed-schema resource through its registered source.
///
/// This is a compiler-front-end preflight for commands such as `validate
/// --deep`; ordinary plan/run preparation must not call it. The returned
/// observations are classified against the fixed baseline and never change
/// the resource's schema epoch.
pub fn preflight_fixed_resource_schema_with_source_registry(
    project_root: &Path,
    resource: &CompiledResource,
    registry: &SourceRegistry,
    plan: &CompiledSourcePlan,
    context: &SourceResolutionContext<'_>,
    options: SchemaDiscoveryExecutionOptions,
) -> Result<ResourceSchemaDiscoveryArtifacts> {
    let (probe, options) = match &resource.descriptor().schema_source {
        SchemaSource::Declared { .. } => (
            resource.with_schema_source_and_schema(SchemaSource::Discover, resource.schema()),
            options.with_declared_baseline(resource)?,
        ),
        source if source.pinned_snapshot().is_some() => {
            let snapshot = source
                .pinned_snapshot()
                .expect("pinned snapshot checked above");
            let (_, baseline) =
                SchemaSnapshotStore::new(project_root).read_with_verified_baseline(snapshot)?;
            if baseline.resource_id() != &resource.descriptor().resource_id {
                return Err(CdfError::data(format!(
                    "pinned schema snapshot belongs to resource `{}` but preflight requested `{}`",
                    baseline.resource_id(),
                    resource.descriptor().resource_id
                )));
            }
            (
                resource.with_schema_source_and_schema(
                    SchemaSource::Discover,
                    Arc::clone(baseline.schema()),
                ),
                options.with_verified_baseline(baseline),
            )
        }
        _ => {
            return Err(CdfError::contract(format!(
                "fixed-schema preflight requires a declared or pinned schema for resource `{}`",
                resource.descriptor().resource_id
            )));
        }
    };
    let probe_plan = plan.clone().bind_schema_authority(
        probe.descriptor(),
        probe.schema().as_ref(),
        None,
        probe.baseline_observation_schema_catalog().to_vec(),
    )?;
    discover_resource_schema_with_source_registry(&probe, registry, &probe_plan, context, options)
}

/// Hydrates and verifies the fixed schema artifacts for a pinned resource.
///
/// Physical source observations belong to the extraction stream. Pinned
/// preparation therefore has no secret, transport, or format-runtime dependency.
pub fn prepare_pinned_resource_schema(
    project_root: &Path,
    resource: &CompiledResource,
) -> Result<CompiledResource> {
    Ok(
        prepare_pinned_resource_schema_artifacts(project_root, resource)?
            .into_parts()
            .0,
    )
}

pub fn prepare_pinned_resource_schema_artifacts(
    project_root: &Path,
    resource: &CompiledResource,
) -> Result<PreparedSchemaResource> {
    let snapshot = resource
        .descriptor()
        .schema_source
        .pinned_snapshot()
        .ok_or_else(|| {
            CdfError::contract("pinned schema preparation requires a schema snapshot")
        })?;
    let store = SchemaSnapshotStore::new(project_root);
    let (_, baseline) = store.read_with_verified_baseline(snapshot)?;
    if baseline.resource_id() != &resource.descriptor().resource_id {
        return Err(CdfError::data(format!(
            "pinned schema snapshot belongs to resource `{}` but preparation requested `{}`",
            baseline.resource_id(),
            resource.descriptor().resource_id
        )));
    }
    let discovery_manifest = snapshot
        .discovery_manifest()?
        .map(|reference| DiscoveryManifestStore::new(project_root).read(&reference))
        .transpose()?;
    let baseline_observation_schema_catalog = discovery_manifest
        .iter()
        .flat_map(|manifest| &manifest.candidates)
        .filter_map(|candidate| {
            candidate
                .physical_schema_hash
                .clone()
                .zip(candidate.physical_schema.as_ref())
        })
        .map(|(physical_schema_hash, schema)| {
            Ok(EffectiveSchemaCatalogEntry::new(
                physical_schema_hash,
                Arc::new(schema.to_arrow()?),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let prepared = resource
        .with_schema_source_and_schema(
            resource.descriptor().schema_source.clone(),
            baseline.schema().clone(),
        )
        .with_baseline_observation_schema_catalog(baseline_observation_schema_catalog);
    Ok(PreparedSchemaResource {
        resource: prepared,
        discovery_manifest,
    })
}

#[derive(Clone, Debug)]
struct SchemaProbe {
    location: String,
    size_bytes: u64,
    size_known: bool,
    modified_at_ms: Option<i64>,
    bounded_identity_value: String,
    physical_schema_hash: SchemaHash,
    probe_bytes: u64,
    probe_records: u64,
    source_bytes_read: u64,
    cache_status: String,
    schema: arrow_schema::SchemaRef,
    source_identity: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
struct DiscoveryCandidate {
    location: String,
    size_bytes: u64,
    size_known: bool,
    modified_at_ms: Option<i64>,
    cache_source: Option<StrongObservationSourceIdentity>,
    source: SourceDiscoveryCandidate,
}

impl DiscoveryCandidate {
    fn from_registered(candidate: SourceDiscoveryCandidate) -> Result<Self> {
        let size_known = candidate.size_bytes.is_some();
        let size_bytes = candidate.size_bytes.unwrap_or(0);
        let cache_source = registered_cache_source(&candidate)?;
        Ok(Self {
            location: candidate.evidence_location.as_str().to_owned(),
            size_bytes,
            size_known,
            modified_at_ms: candidate.modified_at_ms,
            cache_source,
            source: candidate,
        })
    }
}

fn registered_cache_source(
    candidate: &SourceDiscoveryCandidate,
) -> Result<Option<StrongObservationSourceIdentity>> {
    candidate.validate()?;
    let Some(size_bytes) = candidate.size_bytes else {
        return Ok(None);
    };
    let checksum = candidate
        .identity
        .get("sha256")
        .or_else(|| candidate.identity.get("checksum"))
        .cloned();
    let generation = ["etag", "version"]
        .into_iter()
        .filter_map(|key| {
            candidate
                .identity
                .get(key)
                .cloned()
                .map(|value| (key.to_owned(), value))
        })
        .collect::<BTreeMap<_, _>>();
    if checksum.is_none() && generation.is_empty() {
        return Ok(None);
    }
    StrongObservationSourceIdentity::new(
        candidate.evidence_location.as_str().to_owned(),
        size_bytes,
        checksum,
        generation,
    )
    .map(Some)
}

fn registered_observation_cache_key(
    plan: &CompiledSourcePlan,
    session: &dyn SourceDiscoverySession,
    candidate: &DiscoveryCandidate,
    budget: &DiscoveryExecutorBudget,
    admission_identity: &str,
) -> Result<Option<ObservationCacheKey>> {
    let Some(source) = candidate.cache_source.clone() else {
        return Ok(None);
    };
    let interpretation_hash = crate::internal::semantic_hash(&serde_json::json!({
        "redacted_options_hash": plan.redacted_options_hash,
        "physical_plan_hash": plan.physical_plan_hash,
    }))?;
    let observation_contract_hash = crate::internal::semantic_hash(&serde_json::json!({
        "discovery_kind": session.kind(),
        "maximum_bytes": budget.max_bytes_per_file(),
        "maximum_records": budget.max_records_per_file(),
    }))?;
    ObservationCacheKey::new(
        source,
        plan.driver.driver_id.as_str(),
        plan.driver.driver_version.clone(),
        interpretation_hash,
        observation_contract_hash,
        NORMALIZER_NAMECASE_V1,
        admission_identity,
    )
    .map(Some)
}

fn registered_snapshot_metadata(plan: &CompiledSourcePlan) -> Result<BTreeMap<String, String>> {
    Ok(BTreeMap::from([
        ("probe".to_owned(), "registered-source-discovery".to_owned()),
        (
            "source_driver".to_owned(),
            plan.driver.driver_id.as_str().to_owned(),
        ),
        (
            "source_driver_version".to_owned(),
            plan.driver.driver_version.clone(),
        ),
        (
            "source_plan_hash".to_owned(),
            plan.discovery_binding_hash()?,
        ),
        (
            "cdf:normalizer".to_owned(),
            NORMALIZER_NAMECASE_V1.to_owned(),
        ),
    ]))
}

fn discover_registered_resource_schema(
    resource: &CompiledResource,
    options: SchemaDiscoveryExecutionOptions,
    plan: &CompiledSourcePlan,
    session: &dyn SourceDiscoverySession,
    candidates: Vec<DiscoveryCandidate>,
) -> Result<ResourceSchemaDiscoveryArtifacts> {
    ensure_discover_schema_mode(resource)?;
    let source_label = plan.driver.driver_id.as_str();
    let baseline_schema_hash =
        options.runtime_baseline_hash_for(&resource.descriptor().resource_id)?;
    if candidates.is_empty() {
        return Err(CdfError::data(format!(
            "{} discovery for resource `{}` matched no candidates",
            source_label,
            resource.descriptor().resource_id
        )));
    }

    let selector_candidates = candidates
        .iter()
        .map(|candidate| DiscoverySelectorCandidate {
            canonical_location: candidate.location.clone(),
            identity: selector_candidate_identity(candidate),
        })
        .collect::<Vec<_>>();
    let selection = plan_discovery_selection(
        &resource.descriptor().resource_id,
        resource.schema_discovery_sample_files(),
        &selector_candidates,
    )?;
    let file_coverage_label = match selection.file_coverage {
        DiscoveryFileCoverage::AllFiles => "all_files",
        DiscoveryFileCoverage::SampledFiles => "sampled_files",
    };
    let within_file_coverage = match session.kind() {
        SourceDiscoveryKind::SchemaMetadata => DiscoveryWithinFileCoverage::FormatMetadata,
        SourceDiscoveryKind::BoundedContent => DiscoveryWithinFileCoverage::BoundedContent,
        SourceDiscoveryKind::FullContent => DiscoveryWithinFileCoverage::FullContent,
    };
    let within_file_coverage_label = match within_file_coverage {
        DiscoveryWithinFileCoverage::FormatMetadata => "format_metadata",
        DiscoveryWithinFileCoverage::BoundedContent => "bounded_content",
        DiscoveryWithinFileCoverage::FullContent => "full_content",
    };
    let contract = contract_policy_for_resource(resource);
    let policy_version = crate::internal::semantic_hash(&contract)?;
    let admission_identity = crate::internal::semantic_hash(&serde_json::json!({
        "resource_id": resource.descriptor().resource_id.as_str(),
        "baseline_schema_hash": baseline_schema_hash.as_ref().map(ToString::to_string),
        "policy_version": policy_version.clone(),
        "normalizer_version": NORMALIZER_NAMECASE_V1,
    }))?;

    let scheduled_candidates = candidates
        .iter()
        .filter(|candidate| selection.selects(&candidate.location))
        .collect::<Vec<_>>();
    let weights = scheduled_candidates
        .iter()
        .map(|candidate| {
            if candidate.size_known {
                candidate
                    .size_bytes
                    .max(1)
                    .min(options.budget.max_bytes_per_file())
            } else {
                options.budget.max_bytes_per_file()
            }
        })
        .collect::<Vec<_>>();
    let cache_keys = scheduled_candidates
        .iter()
        .map(|candidate| {
            registered_observation_cache_key(
                plan,
                session,
                candidate,
                &options.budget,
                &admission_identity,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let probe_results = run_weighted_probe_jobs(
        &weights,
        &options.budget,
        options.memory_coordinator.clone(),
        |index| {
            probe_discovery_candidate(
                session,
                scheduled_candidates[index],
                &options.budget,
                options.observation_cache.as_ref(),
                cache_keys[index].as_ref(),
            )
        },
    )?;
    let mut probes = Vec::with_capacity(scheduled_candidates.len());
    let mut probe_reports = Vec::with_capacity(scheduled_candidates.len());
    let mut failed = false;
    for (candidate, result) in scheduled_candidates.into_iter().zip(probe_results) {
        match result {
            Ok(probe) => {
                probe_reports.push(format!(
                    "{}: observed {} bytes and {} records",
                    probe.location, probe.probe_bytes, probe.probe_records
                ));
                probes.push(probe);
            }
            Err(error) => {
                failed = true;
                probe_reports.push(format!("{}: failed: {}", candidate.location, error));
            }
        }
    }
    if failed {
        return Err(CdfError::data(format!(
            "{file_coverage_label} + {within_file_coverage_label} {} discovery failed for resource `{}` after evaluating every selected candidate without substitution: {}",
            source_label,
            resource.descriptor().resource_id,
            probe_reports.join("; ")
        )));
    }

    let selected_probes = probes
        .iter()
        .filter(|probe| selection.selects(&probe.location))
        .collect::<Vec<_>>();
    let aggregate_candidates = selected_probes
        .iter()
        .map(|probe| {
            AggregateSchemaCandidate::new(probe.location.clone(), probe.schema.as_ref().clone())
        })
        .collect::<Vec<_>>();
    let file_aggregate = plan_aggregate_arrow_schema_join(&aggregate_candidates)?;
    if options.runtime_baseline.is_none() && !file_aggregate.is_compatible() {
        let file_reports = file_aggregate
            .files
            .iter()
            .map(aggregate_file_report)
            .collect::<Vec<_>>()
            .join("; ");
        let incompatibilities = file_aggregate
            .incompatibilities
            .iter()
            .map(|incompatibility| {
                format!(
                    "{} at {}: {}",
                    incompatibility.location,
                    incompatibility.field_path.join("."),
                    incompatibility.reason
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        return Err(CdfError::contract(format!(
            "{} for resource `{}` found incompatible files; candidate verdicts: {file_reports}; incompatibilities: {incompatibilities}",
            if selection.file_coverage == DiscoveryFileCoverage::SampledFiles {
                "initial sampled schema pin"
            } else {
                "initial all-files schema pin"
            },
            resource.descriptor().resource_id,
        )));
    }

    let initial_observed_schema = match selected_probes.as_slice() {
        [probe] => probe.schema.as_ref(),
        _ => &file_aggregate.schema,
    };
    let (effective_schema, terminal_quarantines) =
        if let Some(baseline) = options.runtime_baseline() {
            let admit_compatible_evolution = baseline.admits_evolution()
                && selection.file_coverage == DiscoveryFileCoverage::AllFiles;
            classify_runtime_schema_observations(
                &probes,
                baseline,
                &contract,
                admit_compatible_evolution,
            )?
        } else {
            (
                normalize_arrow_schema(initial_observed_schema, &IdentifierPolicy::default())?,
                Vec::new(),
            )
        };
    let normalized = effective_schema;
    let normalized = Arc::new(normalized);
    let metadata = registered_snapshot_metadata(plan)?;
    let effective = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        normalized.as_ref(),
        metadata.clone(),
    )?;
    let manifest_candidates = candidates
        .iter()
        .map(|candidate| {
            if !selection.selects(&candidate.location) {
                return Ok(unobserved_manifest_candidate(candidate, source_label));
            }
            let probe = probes
                .iter()
                .find(|probe| probe.location == candidate.location)
                .ok_or_else(|| {
                    CdfError::internal(format!(
                        "selected discovery candidate `{}` was not observed",
                        candidate.location
                    ))
                })?;
            let verdict = file_aggregate
                .files
                .iter()
                .find(|verdict| verdict.location == probe.location)
                .ok_or_else(|| {
                    CdfError::internal(format!(
                        "aggregate schema report omitted candidate `{}`",
                        probe.location
                    ))
                })?;
            manifest_candidate(
                probe,
                verdict,
                source_label,
                (selection.file_coverage == DiscoveryFileCoverage::SampledFiles)
                    .then(|| selector_candidate_identity(candidate)),
                terminal_quarantines
                    .iter()
                    .find(|item| item.observation_id() == probe.location),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let effective_schema_identity = options
        .runtime_baseline()
        .map(|baseline| baseline.effective_schema_identity(&effective.schema_hash))
        .unwrap_or_else(|| effective.schema_hash.clone());
    let manifest = DiscoveryManifestArtifact::new(DiscoveryManifestInput {
        resource_id: resource.descriptor().resource_id.to_string(),
        baseline_schema_hash,
        // This is intentionally the schema-only v1 identity. The final v2
        // snapshot binds this manifest, so using that final hash here would be
        // circular. The manifest hash and linked v2 snapshot hash remain the
        // authoritative identities of the complete discovery evidence.
        effective_schema_hash: Some(effective_schema_identity),
        file_coverage: selection.file_coverage.clone(),
        within_file_coverage,
        selector: selection.selector.clone(),
        budget: options.budget.clone(),
        normalizer_version: NORMALIZER_NAMECASE_V1.to_owned(),
        policy_version,
        candidates: manifest_candidates,
    })?;
    let artifact = SchemaSnapshotArtifact::new_with_discovery_manifest(
        &resource.descriptor().resource_id,
        normalized.as_ref(),
        metadata,
        manifest.reference(),
    )?;
    let total_probe_bytes = selected_probes.iter().try_fold(0_u64, |total, probe| {
        total.checked_add(probe.probe_bytes).ok_or_else(|| {
            CdfError::data(format!(
                "{file_coverage_label} + {within_file_coverage_label} {} discovery byte accounting overflowed for resource `{}` while adding `{}`; reduce the matched candidate set or probe budget",
                source_label,
                resource.descriptor().resource_id,
                probe.location
            ))
        })
    })?;
    let total_probe_records = selected_probes.iter().try_fold(0_u64, |total, probe| {
        total.checked_add(probe.probe_records).ok_or_else(|| {
            CdfError::data(format!(
                "{file_coverage_label} + {within_file_coverage_label} {} discovery record accounting overflowed for resource `{}` while adding `{}`; reduce the matched candidate set or record budget",
                source_label,
                resource.descriptor().resource_id,
                probe.location
            ))
        })
    })?;
    let total_source_bytes = selected_probes.iter().try_fold(0_u64, |total, probe| {
        total.checked_add(probe.source_bytes_read).ok_or_else(|| {
            CdfError::data(format!(
                "{file_coverage_label} + {within_file_coverage_label} {} discovery source-byte accounting overflowed for resource `{}` while adding `{}`",
                source_label,
                resource.descriptor().resource_id,
                probe.location
            ))
        })
    })?;
    let cache_hits = selected_probes
        .iter()
        .filter(|probe| probe.cache_status == "hit")
        .count();
    let cache_misses = selected_probes
        .iter()
        .filter(|probe| probe.cache_status.starts_with("miss_"))
        .count();
    let cache_bypasses = selected_probes.len() - cache_hits - cache_misses;
    let mut source_identity = BTreeMap::from([
        ("source_driver".to_owned(), source_label.to_owned()),
        ("transport".to_owned(), source_label.to_owned()),
        ("file_coverage".to_owned(), file_coverage_label.to_owned()),
        (
            "within_file_coverage".to_owned(),
            within_file_coverage_label.to_owned(),
        ),
        ("matched_files".to_owned(), candidates.len().to_string()),
        (
            "selected_files".to_owned(),
            selection.selected_count().to_string(),
        ),
        (
            "unobserved_files".to_owned(),
            (candidates.len() - selection.selected_count()).to_string(),
        ),
        ("probe_bytes_read".to_owned(), total_probe_bytes.to_string()),
        (
            "probe_records_read".to_owned(),
            total_probe_records.to_string(),
        ),
        (
            "discovery_source_bytes_read".to_owned(),
            total_source_bytes.to_string(),
        ),
        ("observation_cache_hits".to_owned(), cache_hits.to_string()),
        (
            "observation_cache_misses".to_owned(),
            cache_misses.to_string(),
        ),
        (
            "observation_cache_bypasses".to_owned(),
            cache_bypasses.to_string(),
        ),
        (
            "discovery_manifest_hash".to_owned(),
            manifest.manifest_hash.to_string(),
        ),
        ("discovery_manifest_path".to_owned(), manifest.path.clone()),
    ]);
    if let Some(sample_files) = resource.schema_discovery_sample_files() {
        source_identity.insert("sample_files".to_owned(), sample_files.to_string());
    }
    if let Some(selector) = &selection.selector {
        source_identity.insert("selector".to_owned(), selector.selector.clone());
    }
    if let [probe] = selected_probes.as_slice()
        && candidates.len() == 1
    {
        source_identity.extend(namespaced_driver_evidence(&probe.source_identity)?);
        source_identity.insert("path".to_owned(), probe.location.clone());
    }
    let discovery = ResourceSchemaDiscovery {
        normalized_schema: normalized,
        snapshot: DiscoveredSchemaSnapshot {
            reference: artifact.reference(),
            artifact,
            source_identity,
        },
    };
    let effective_schema_runtime = {
        let effective_schema_hash = manifest.effective_schema_hash.clone().ok_or_else(|| {
            CdfError::internal(
                "registered source discovery manifest omitted its effective schema snapshot hash",
            )
        })?;
        let mut observations = probes
            .iter()
            .map(|probe| {
                EffectiveSchemaObservationEvidence::new(
                    probe.location.clone(),
                    probe.physical_schema_hash.clone(),
                )
            })
            .collect::<Vec<_>>();
        observations.sort_by(|left, right| left.observation_id.cmp(&right.observation_id));
        let mut schema_catalog = probes
            .iter()
            .map(|probe| {
                EffectiveSchemaCatalogEntry::new(
                    probe.physical_schema_hash.clone(),
                    Arc::clone(&probe.schema),
                )
            })
            .collect::<Vec<_>>();
        schema_catalog
            .sort_by(|left, right| left.physical_schema_hash.cmp(&right.physical_schema_hash));
        schema_catalog
            .dedup_by(|left, right| left.physical_schema_hash == right.physical_schema_hash);
        let baseline = options
            .runtime_baseline()
            .map(RuntimeSchemaBaseline::reference)
            .unwrap_or_else(|| SchemaBaselineReference::Pinned {
                snapshot: discovery.snapshot.reference.clone(),
            });
        let mut evidence = EffectiveSchemaEvidence::new(
            baseline,
            effective_schema_hash,
            manifest.reference(),
            observations,
        )?;
        evidence = evidence.with_discovery_coverage(DiscoveryCoverageEvidence::new(
            DiscoveryCoverageEvidenceInput {
                file_coverage: file_coverage_label.to_owned(),
                within_file_coverage: within_file_coverage_label.to_owned(),
                selector: manifest
                    .selector
                    .as_ref()
                    .map(|selector| selector.selector.clone()),
                sample_files: manifest
                    .selector
                    .as_ref()
                    .map(|selector| selector.sample_files),
                matched_files: u64::try_from(candidates.len())
                    .map_err(|_| CdfError::contract("discovery matched count exceeds u64"))?,
                selected_files: u64::try_from(selection.selected_count())
                    .map_err(|_| CdfError::contract("discovery selected count exceeds u64"))?,
                observed_bytes: total_probe_bytes,
                observed_records: total_probe_records,
            },
        )?)?;
        Some(
            EffectiveSchemaRuntime::new(evidence, schema_catalog)?
                .with_terminal_quarantines(terminal_quarantines)?
                .with_discovery_executor_budget(DiscoveryExecutorBudgetEvidence::new(
                    options.budget.max_bytes_per_file(),
                    options.budget.max_records_per_file(),
                    options.budget.max_total_in_flight_bytes(),
                    options.budget.max_concurrent_probes(),
                )?)?,
        )
    };
    Ok(ResourceSchemaDiscoveryArtifacts {
        discovery,
        discovery_manifest: Some(manifest),
        effective_schema_runtime,
    })
}

fn namespaced_driver_evidence(
    evidence: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>> {
    cdf_runtime::validate_source_evidence_identity(evidence)?;
    evidence
        .iter()
        .map(|(key, value)| {
            if key.is_empty() || key.chars().any(char::is_control) {
                return Err(CdfError::contract(
                    "source driver evidence keys must be nonempty and control-free",
                ));
            }
            Ok((format!("driver.{key}"), value.clone()))
        })
        .collect()
}

fn run_weighted_probe_jobs<T, F>(
    weights: &[u64],
    budget: &DiscoveryExecutorBudget,
    coordinator: Option<Arc<dyn MemoryCoordinator>>,
    operation: F,
) -> Result<Vec<T>>
where
    T: Send,
    F: Fn(usize) -> T + Sync,
{
    if weights.is_empty() {
        return Ok(Vec::new());
    }
    let tag = BudgetTag::new("discovery.metadata")?;
    let coordinator = match coordinator {
        Some(coordinator) => coordinator,
        None => Arc::new(DeterministicMemoryCoordinator::new(
            budget.max_total_in_flight_bytes(),
            BTreeMap::from([(tag.clone(), budget.max_total_in_flight_bytes())]),
        )?) as Arc<dyn MemoryCoordinator>,
    };
    let next = AtomicUsize::new(0);
    let results = Mutex::new(Vec::with_capacity(weights.len()));
    let worker_count = usize::try_from(budget.max_concurrent_probes())
        .map_err(|_| CdfError::contract("discovery concurrency exceeds usize"))?
        .min(weights.len());
    std::thread::scope(|scope| -> Result<()> {
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let coordinator = Arc::clone(&coordinator);
            let tag = tag.clone();
            let operation = &operation;
            let results = &results;
            let next = &next;
            handles.push(scope.spawn(move || -> Result<()> {
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let Some(weight) = weights.get(index).copied() else {
                        return Ok(());
                    };
                    let request = ReservationRequest::new(
                        ConsumerKey::new(
                            format!("discovery-probe-{index}"),
                            MemoryClass::Discovery,
                        )?,
                        weight,
                    )?
                    .with_subcap(tag.clone())
                    .as_minimum_working_set();
                    let lease = cdf_memory::reserve_blocking(Arc::clone(&coordinator), &request)?;
                    let result = operation(index);
                    drop(lease);
                    results
                        .lock()
                        .map_err(|_| CdfError::internal("discovery result mutex was poisoned"))?
                        .push((index, result));
                }
            }));
        }
        for handle in handles {
            handle
                .join()
                .map_err(|_| CdfError::internal("discovery probe worker panicked"))??;
        }
        Ok(())
    })?;
    let mut results = results
        .into_inner()
        .map_err(|_| CdfError::internal("discovery result mutex was poisoned"))?;
    results.sort_by_key(|(index, _)| *index);
    Ok(results.into_iter().map(|(_, result)| result).collect())
}

fn contract_policy_for_resource(resource: &CompiledResource) -> ContractPolicy {
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    let allowances = resource.type_policy_allowances();
    policy.types.coerce_types = allowances.coerce_types;
    policy.types.allow_lossy_mapping = allowances.allow_lossy_mapping;
    policy
}

fn probe_discovery_candidate(
    session: &dyn SourceDiscoverySession,
    candidate: &DiscoveryCandidate,
    budget: &DiscoveryExecutorBudget,
    cache: Option<&ObservationCacheStore>,
    cache_key: Option<&ObservationCacheKey>,
) -> Result<SchemaProbe> {
    let mut cache_status = if cache_key.is_some() {
        "disabled".to_owned()
    } else {
        "bypass_weak_identity".to_owned()
    };
    if let (Some(cache), Some(cache_key)) = (cache, cache_key) {
        match cache.lookup(cache_key) {
            ObservationCacheLookup::Hit(entry) => {
                let entry = *entry;
                return schema_probe_from_parts(
                    candidate,
                    Arc::new(entry.arrow_schema()?),
                    entry.source_identity,
                    entry.observed_bytes,
                    entry.observed_records,
                    0,
                    "hit".to_owned(),
                );
            }
            ObservationCacheLookup::Miss(reason) => {
                cache_status = observation_cache_miss_label(reason).to_owned();
            }
        }
    }
    let observation = session.observe(
        &candidate.source,
        &SourceDiscoveryRequest::new(budget.max_bytes_per_file(), budget.max_records_per_file())?,
    )?;
    let schema = Arc::new(observation.schema);
    let source_identity = observation.source_identity;
    let probe_bytes = observation.bytes_read;
    let probe_records = observation.records_read;
    if let (Some(cache), Some(cache_key)) = (cache, cache_key) {
        let entry = ObservationCacheEntry::new(
            cache_key.clone(),
            schema.as_ref(),
            source_identity.clone(),
            probe_bytes,
            probe_records,
        )?;
        cache_status.push_str(observation_cache_store_suffix(cache.store(&entry)));
    }
    schema_probe_from_parts(
        candidate,
        schema,
        source_identity,
        probe_bytes,
        probe_records,
        probe_bytes,
        cache_status,
    )
}

fn schema_probe_from_parts(
    candidate: &DiscoveryCandidate,
    schema: arrow_schema::SchemaRef,
    source_identity: BTreeMap<String, String>,
    probe_bytes: u64,
    probe_records: u64,
    source_bytes_read: u64,
    cache_status: String,
) -> Result<SchemaProbe> {
    let modified_at_ms = source_identity
        .get("modified_unix_millis")
        .map(|value| {
            value.parse::<i64>().map_err(|error| {
                CdfError::data(format!(
                    "invalid modification time `{value}` for `{}`: {error}",
                    candidate.location
                ))
            })
        })
        .transpose()?
        .or(candidate.modified_at_ms);
    let physical_schema_hash = cdf_kernel::canonical_arrow_schema_hash(schema.as_ref())?;
    let fingerprint = source_identity
        .get("footer_sha256")
        .or_else(|| source_identity.get("schema_hash"))
        .cloned()
        .unwrap_or_else(|| physical_schema_hash.to_string());
    Ok(SchemaProbe {
        location: candidate.location.clone(),
        size_bytes: candidate.size_bytes,
        size_known: candidate.size_known,
        modified_at_ms,
        bounded_identity_value: fingerprint,
        physical_schema_hash,
        probe_bytes,
        probe_records,
        source_bytes_read,
        cache_status,
        schema,
        source_identity,
    })
}

fn observation_cache_miss_label(reason: ObservationCacheMissReason) -> &'static str {
    match reason {
        ObservationCacheMissReason::Absent => "miss_absent",
        ObservationCacheMissReason::CorruptOrUnsupported => "miss_corrupt_or_unsupported",
        ObservationCacheMissReason::Oversized => "miss_oversized",
        ObservationCacheMissReason::Unavailable => "miss_unavailable",
    }
}

fn observation_cache_store_suffix(outcome: ObservationCacheStoreOutcome) -> &'static str {
    match outcome {
        ObservationCacheStoreOutcome::Stored => ":stored",
        ObservationCacheStoreOutcome::AlreadyPresent => ":already_present",
        ObservationCacheStoreOutcome::SkippedOversized => ":store_skipped_oversized",
        ObservationCacheStoreOutcome::Unavailable => ":store_unavailable",
    }
}

fn classify_runtime_schema_observations(
    probes: &[SchemaProbe],
    baseline: &RuntimeSchemaBaseline,
    contract: &ContractPolicy,
    admit_compatible_evolution: bool,
) -> Result<(
    arrow_schema::Schema,
    Vec<TerminalSchemaObservationQuarantine>,
)> {
    let mut effective = baseline.schema().as_ref().clone();
    let physical_type_policy = contract.types.clone();
    let mut quarantines = Vec::new();
    for probe in probes {
        if matches!(&contract.schema.mode, SchemaEvolutionMode::Freeze)
            && baseline.contains_baseline_observation_schema(&probe.physical_schema_hash)
        {
            continue;
        }
        let report = plan_aggregate_arrow_schema_join(&[
            AggregateSchemaCandidate::new("__cdf_verified_effective__", effective.clone()),
            AggregateSchemaCandidate::new(probe.location.clone(), probe.schema.as_ref().clone()),
        ])?;
        let freeze_deviation = if matches!(&contract.schema.mode, SchemaEvolutionMode::Freeze) {
            let joined = normalize_arrow_schema(&report.schema, &IdentifierPolicy::default())?;
            !same_effective_fields(&joined, baseline.schema().as_ref())
        } else {
            false
        };
        let constrained = plan_schema_reconciliation(
            probe.schema.as_ref(),
            baseline.schema().as_ref(),
            &physical_type_policy,
        )?;
        if report.is_compatible()
            && !freeze_deviation
            && admit_compatible_evolution
            && matches!(&contract.schema.mode, SchemaEvolutionMode::Evolve)
        {
            effective = normalize_arrow_schema(&report.schema, &IdentifierPolicy::default())?;
            continue;
        }
        if constrained.errors.is_empty() {
            let projects_every_observed_field = constrained
                .plan
                .fields
                .iter()
                .all(|field| field.decision != cdf_contract::FieldCoercionDecision::Extra);
            if projects_every_observed_field || (report.is_compatible() && !freeze_deviation) {
                continue;
            }
        }

        let mut fields = constrained
            .errors
            .iter()
            .map(|item| {
                let path = vec![item.source_name.clone()];
                let reason = if item.operator_fixes.is_empty() {
                    item.message.clone()
                } else {
                    format!("{}; {}", item.message, item.operator_fixes.join("; "))
                };
                SchemaObservationFieldQuarantine::new_field_path(
                    path.clone(),
                    canonical_field_at_path(probe.schema.as_ref(), &path)?,
                    canonical_field_at_path(&effective, &path)?,
                    reason,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        if fields.is_empty() {
            fields = report
                .incompatibilities
                .iter()
                .map(|item| {
                    SchemaObservationFieldQuarantine::new_field_path(
                        item.field_path.clone(),
                        canonical_field_at_path(probe.schema.as_ref(), &item.field_path)?,
                        canonical_field_at_path(&effective, &item.field_path)?,
                        item.reason.clone(),
                    )
                })
                .collect::<Result<Vec<_>>>()?;
        }
        if fields.is_empty() {
            for verdict in report.files.iter().flat_map(|file| &file.fields) {
                if verdict.decision != cdf_contract::AggregateFieldDecision::Preserved
                    || verdict.observed_nullable != Some(verdict.effective_nullable)
                    || !verdict.metadata_variance.is_empty()
                {
                    fields.push(SchemaObservationFieldQuarantine::new_field_path(
                        verdict.field_path.clone(),
                        canonical_field_at_path(probe.schema.as_ref(), &verdict.field_path)?,
                        canonical_field_at_path(&effective, &verdict.field_path)?,
                        verdict.reason.clone(),
                    )?);
                }
            }
        }
        if fields.is_empty() {
            fields.push(SchemaObservationFieldQuarantine::whole_schema(
                "schema or field metadata differs from the frozen baseline",
            )?);
        }
        fields.sort_by(|left, right| {
            schema_observation_scope_sort_key(left.scope())
                .cmp(&schema_observation_scope_sort_key(right.scope()))
        });
        fields.dedup();
        let (rule_id, policy, remediation) = match &contract.schema.mode {
            SchemaEvolutionMode::Freeze => (
                "schema-observation:freeze-deviation",
                SchemaObservationPolicy::Freeze,
                "restore the pinned schema for this input, explicitly repin after review, or change the resource contract to evolve",
            ),
            SchemaEvolutionMode::Evolve => (
                "schema-observation:incompatible",
                SchemaObservationPolicy::Evolve,
                "publish a compatible source type, declare an allowed coercion, or repin the schema after review",
            ),
        };
        quarantines.push(TerminalSchemaObservationQuarantine::new(
            probe.location.clone(),
            probe.physical_schema_hash.clone(),
            rule_id,
            "schema_observation_quarantined",
            policy,
            remediation,
            fields,
        )?);
    }
    Ok((effective, quarantines))
}

fn schema_observation_scope_sort_key(scope: &cdf_kernel::SchemaObservationScope) -> String {
    match scope {
        cdf_kernel::SchemaObservationScope::FieldPath { path } => {
            format!("field:{}", path.join("\u{0}"))
        }
        cdf_kernel::SchemaObservationScope::WholeSchema => "schema".to_owned(),
        _ => "unknown".to_owned(),
    }
}

fn canonical_field_at_path(
    schema: &arrow_schema::Schema,
    path: &[String],
) -> Result<Option<cdf_kernel::CanonicalArrowField>> {
    let Some((first, rest)) = path.split_first() else {
        return Ok(None);
    };
    let Some(mut field) = schema
        .fields()
        .iter()
        .find(|field| schema_field_component_matches(field.as_ref(), first))
        .cloned()
    else {
        return Ok(None);
    };
    for component in rest {
        let next = match field.data_type() {
            arrow_schema::DataType::Struct(fields) => fields
                .iter()
                .find(|field| schema_field_component_matches(field.as_ref(), component))
                .cloned(),
            arrow_schema::DataType::List(child)
            | arrow_schema::DataType::LargeList(child)
            | arrow_schema::DataType::FixedSizeList(child, _) => {
                if schema_field_component_matches(child.as_ref(), component) {
                    Some(child.clone())
                } else if let arrow_schema::DataType::Struct(fields) = child.data_type() {
                    fields
                        .iter()
                        .find(|field| schema_field_component_matches(field.as_ref(), component))
                        .cloned()
                } else {
                    None
                }
            }
            arrow_schema::DataType::Map(entries, _) => {
                if schema_field_component_matches(entries.as_ref(), component) {
                    Some(entries.clone())
                } else if let arrow_schema::DataType::Struct(fields) = entries.data_type() {
                    fields
                        .iter()
                        .find(|field| schema_field_component_matches(field.as_ref(), component))
                        .cloned()
                } else {
                    None
                }
            }
            _ => None,
        };
        let Some(next) = next else {
            return Ok(None);
        };
        field = next;
    }
    cdf_kernel::CanonicalArrowField::from_arrow(field.as_ref())
        .map(Some)
        .map_err(|error| CdfError::data(format!("encode exact Arrow field evidence: {error}")))
}

fn schema_field_component_matches(field: &arrow_schema::Field, component: &str) -> bool {
    field.name() == component
        || cdf_kernel::source_name(field) == Some(component)
        || cdf_contract::normalize_identifier(field.name(), &IdentifierPolicy::default())
            .is_ok_and(|normalized| normalized == component)
}

fn manifest_candidate(
    probe: &SchemaProbe,
    verdict: &AggregateFileSchemaVerdict,
    transport: &str,
    selector_identity: Option<DiscoveryBoundedIdentity>,
    terminal_quarantine: Option<&TerminalSchemaObservationQuarantine>,
) -> Result<DiscoveryCandidateEvidence> {
    let outcome = if verdict
        .fields
        .iter()
        .any(|field| field.outcome == RuleOutcome::Coerced)
    {
        "coerced"
    } else {
        "pass"
    };
    let field_verdicts = match terminal_quarantine {
        Some(item) => serde_json::to_string(item.fields()),
        None => serde_json::to_string(&verdict.fields),
    }
    .map_err(|error| CdfError::internal(error.to_string()))?;
    Ok(DiscoveryCandidateEvidence {
        transport: transport.to_owned(),
        canonical_location: probe.location.clone(),
        identity: selector_identity.unwrap_or_else(|| DiscoveryBoundedIdentity {
            size_bytes: probe.size_known.then_some(probe.size_bytes),
            modified_at_ms: probe.modified_at_ms,
            value: Some(probe.bounded_identity_value.clone()),
            strength: DiscoveryIdentityStrength::BoundedObservation,
        }),
        participation: DiscoveryParticipation::Observed,
        metadata_variance: manifest_metadata_variance(verdict),
        physical_schema_hash: Some(probe.physical_schema_hash.clone()),
        physical_schema: Some(cdf_kernel::CanonicalArrowSchema::from_arrow(
            probe.schema.as_ref(),
        )?),
        probe_bytes: Some(probe.probe_bytes),
        probe_records: Some(probe.probe_records),
        schema_verdict: Some(DiscoverySchemaVerdict {
            kind: if terminal_quarantine.is_some() {
                DiscoverySchemaVerdictKind::Quarantined
            } else {
                DiscoverySchemaVerdictKind::Admitted
            },
            rule: terminal_quarantine
                .map(|item| item.rule_id().to_owned())
                .unwrap_or_else(|| "aggregate-schema-join-v1".to_owned()),
            details: BTreeMap::from([
                (
                    "outcome".to_owned(),
                    terminal_quarantine
                        .map(|_| "quarantined")
                        .unwrap_or(outcome)
                        .to_owned(),
                ),
                ("field_verdicts".to_owned(), field_verdicts),
            ]),
        }),
    })
}

fn selector_candidate_identity(candidate: &DiscoveryCandidate) -> DiscoveryBoundedIdentity {
    DiscoveryBoundedIdentity {
        size_bytes: candidate.size_known.then_some(candidate.size_bytes),
        modified_at_ms: candidate.modified_at_ms,
        value: None,
        strength: DiscoveryIdentityStrength::Unavailable,
    }
}

fn unobserved_manifest_candidate(
    candidate: &DiscoveryCandidate,
    transport: &str,
) -> DiscoveryCandidateEvidence {
    DiscoveryCandidateEvidence {
        transport: transport.to_owned(),
        canonical_location: candidate.location.clone(),
        identity: selector_candidate_identity(candidate),
        participation: DiscoveryParticipation::Unobserved,
        metadata_variance: Vec::new(),
        physical_schema_hash: None,
        physical_schema: None,
        probe_bytes: None,
        probe_records: None,
        schema_verdict: None,
    }
}

fn manifest_metadata_variance(
    verdict: &AggregateFileSchemaVerdict,
) -> Vec<DiscoveryMetadataVariance> {
    let mut variance = verdict
        .schema_metadata_variance
        .iter()
        .map(|item| manifest_variance(DiscoveryMetadataScope::Schema, "", item))
        .collect::<Vec<_>>();
    variance.extend(verdict.fields.iter().flat_map(|field| {
        field.metadata_variance.iter().map(|item| {
            manifest_variance(
                DiscoveryMetadataScope::Field,
                &field.field_path.join("."),
                item,
            )
        })
    }));
    variance
}

fn manifest_variance(
    scope: DiscoveryMetadataScope,
    path: &str,
    variance: &AggregateMetadataVariance,
) -> DiscoveryMetadataVariance {
    DiscoveryMetadataVariance {
        scope,
        path: path.to_owned(),
        key: variance.key.clone(),
        observed_values: variance.candidate_values.clone(),
    }
}

fn aggregate_file_report(verdict: &AggregateFileSchemaVerdict) -> String {
    let fatal = verdict
        .fields
        .iter()
        .filter(|field| field.outcome == RuleOutcome::Fatal)
        .count();
    let coerced = verdict
        .fields
        .iter()
        .filter(|field| field.outcome == RuleOutcome::Coerced)
        .count();
    format!(
        "{}: {} fatal, {} coerced, {} field verdicts",
        verdict.location,
        fatal,
        coerced,
        verdict.fields.len()
    )
}

pub fn compile_discovered_schema_artifacts(
    resource: &CompiledResource,
    artifacts: &mut ResourceSchemaDiscoveryArtifacts,
) -> Result<CompiledResource> {
    let (mut prepared, discovery) =
        apply_discovered_schema_constraints(resource, artifacts.discovery.clone())?;
    artifacts.discovery = discovery;
    let Some(runtime) = artifacts.effective_schema_runtime.as_ref() else {
        return Ok(prepared);
    };
    prepared = prepared.with_baseline_observation_schema_catalog(runtime.schema_catalog.clone());
    let mut evidence = EffectiveSchemaEvidence::new(
        SchemaBaselineReference::Pinned {
            snapshot: artifacts.discovery.snapshot.reference.clone(),
        },
        artifacts.discovery.snapshot.reference.schema_hash.clone(),
        runtime.evidence.discovery_manifest.clone(),
        runtime.evidence.observations.clone(),
    )?;
    if let Some(coverage) = runtime.evidence.discovery_coverage.clone() {
        evidence = evidence.with_discovery_coverage(coverage)?;
    }
    let mut rebound = EffectiveSchemaRuntime::new(evidence, runtime.schema_catalog.clone())?
        .with_terminal_quarantines(runtime.terminal_quarantines.clone())?;
    if let Some(budget) = runtime.discovery_executor_budget.clone() {
        rebound = rebound.with_discovery_executor_budget(budget)?;
    }
    artifacts.effective_schema_runtime = Some(rebound.clone());
    prepared = prepared.with_effective_schema(prepared.schema(), rebound)?;
    Ok(prepared)
}

pub fn apply_discovered_schema_constraints(
    resource: &CompiledResource,
    mut discovery: ResourceSchemaDiscovery,
) -> Result<(CompiledResource, ResourceSchemaDiscovery)> {
    let SchemaSource::Hints {
        source,
        hints_hash,
        snapshot: None,
    } = &resource.descriptor().schema_source
    else {
        return Ok((
            apply_discovered_schema(resource, discovery.clone()),
            discovery,
        ));
    };
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone()).types;
    let allowances = resource.type_policy_allowances();
    policy.coerce_types = allowances.coerce_types;
    policy.allow_lossy_mapping = allowances.allow_lossy_mapping;
    let reconciled = reconcile_schema(
        discovery.normalized_schema.as_ref(),
        resource.schema().as_ref(),
        &policy,
    )?;
    let schema = Arc::new(reconciled.schema);
    let old = &discovery.snapshot.artifact;
    let artifact = match old.discovery_manifest_reference()? {
        Some(manifest) => {
            let mut metadata = old.metadata.clone();
            metadata.remove(DISCOVERY_MANIFEST_HASH_METADATA_KEY);
            metadata.remove(DISCOVERY_MANIFEST_PATH_METADATA_KEY);
            SchemaSnapshotArtifact::new_with_discovery_manifest(
                &resource.descriptor().resource_id,
                schema.as_ref(),
                metadata,
                manifest,
            )?
        }
        None => SchemaSnapshotArtifact::new(
            &resource.descriptor().resource_id,
            schema.as_ref(),
            old.metadata.clone(),
        )?,
    };
    discovery.normalized_schema = Arc::clone(&schema);
    discovery.snapshot.artifact = artifact.clone();
    discovery.snapshot.reference = artifact.reference();
    Ok((
        resource.with_schema_source_and_schema(
            SchemaSource::Hints {
                source: source.clone(),
                hints_hash: hints_hash.clone(),
                snapshot: Some(artifact.reference()),
            },
            schema,
        ),
        discovery,
    ))
}

pub fn write_schema_discovery_artifacts(
    project_root: impl AsRef<Path>,
    artifacts: &ResourceSchemaDiscoveryArtifacts,
) -> Result<SchemaDiscoveryWriteOutcome> {
    let project_root = project_root.as_ref();
    let manifest_written = match &artifacts.discovery_manifest {
        Some(manifest) => DiscoveryManifestStore::new(project_root).write_if_changed(manifest)?,
        None => false,
    };
    let snapshot_written = SchemaSnapshotStore::new(project_root)
        .write_if_changed(&artifacts.discovery.snapshot.artifact)?;
    Ok(SchemaDiscoveryWriteOutcome {
        manifest_written,
        snapshot_written,
    })
}

pub fn apply_discovered_schema(
    resource: &CompiledResource,
    discovery: ResourceSchemaDiscovery,
) -> CompiledResource {
    resource.with_schema_source_and_schema(
        SchemaSource::Discovered {
            snapshot: discovery.snapshot.reference.clone(),
        },
        Arc::clone(&discovery.normalized_schema),
    )
}

fn same_effective_fields(left: &arrow_schema::Schema, right: &arrow_schema::Schema) -> bool {
    left.fields().len() == right.fields().len()
        && left
            .fields()
            .iter()
            .zip(right.fields())
            .all(|(left, right)| {
                let left_source =
                    cdf_kernel::source_name(left.as_ref()).unwrap_or_else(|| left.name());
                let right_source =
                    cdf_kernel::source_name(right.as_ref()).unwrap_or_else(|| right.name());
                left.name() == right.name()
                    && left_source == right_source
                    && left.data_type() == right.data_type()
                    && left.is_nullable() == right.is_nullable()
            })
}

fn ensure_discover_schema_mode(resource: &CompiledResource) -> Result<()> {
    if matches!(
        resource.descriptor().schema_source,
        SchemaSource::Discover | SchemaSource::Hints { snapshot: None, .. }
    ) {
        return Ok(());
    }
    Err(CdfError::contract(format!(
        "cdf schema discover supports resources in discover schema mode; resource `{}` already has a declared or pinned schema",
        resource.descriptor().resource_id
    )))
}

#[cfg(test)]
mod terminal_evidence_tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use arrow_schema::{DataType, Field, Fields, Schema};

    use super::{
        DiscoveryExecutorBudget, canonical_field_at_path, namespaced_driver_evidence,
        registered_cache_source, run_weighted_probe_jobs,
    };
    use cdf_runtime::SourceDiscoveryCandidate;

    fn nested_schema(children: Vec<Field>) -> Schema {
        Schema::new(vec![Field::new(
            "payload",
            DataType::Struct(Fields::from(children)),
            true,
        )])
    }

    #[test]
    fn nested_added_and_removed_children_are_exact_optional_field_evidence() {
        let narrow = nested_schema(vec![Field::new("kept", DataType::Int64, true)]);
        let wide = nested_schema(vec![
            Field::new("kept", DataType::Int64, true),
            Field::new(
                "nested_change",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            ),
        ]);
        let path = vec!["payload".to_owned(), "nested_change".to_owned()];

        let added = canonical_field_at_path(&wide, &path).unwrap().unwrap();
        assert_eq!(added.name, "nested_change");
        assert!(!added.nullable);
        assert!(canonical_field_at_path(&narrow, &path).unwrap().is_none());

        let removed = canonical_field_at_path(&wide, &path).unwrap().unwrap();
        assert_eq!(removed, added);
        assert!(canonical_field_at_path(&narrow, &path).unwrap().is_none());
    }

    #[test]
    fn weighted_discovery_scheduler_uses_parallel_slots_only_within_byte_cap() {
        let active = AtomicUsize::new(0);
        let peak = AtomicUsize::new(0);
        let budget = DiscoveryExecutorBudget::new(64, 1_000, 128, 8).unwrap();
        let output = run_weighted_probe_jobs(&[64; 8], &budget, None, |index| {
            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(current, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(10));
            active.fetch_sub(1, Ordering::SeqCst);
            index
        })
        .unwrap();
        assert_eq!(output, (0..8).collect::<Vec<_>>());
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn driver_evidence_is_namespaced_and_cannot_replace_framework_authority() {
        let driver = BTreeMap::from([
            ("source_driver".to_owned(), "attacker".to_owned()),
            ("transport".to_owned(), "attacker".to_owned()),
            (
                "discovery_manifest_hash".to_owned(),
                "sha256:attacker".to_owned(),
            ),
        ]);
        let mut framework = BTreeMap::from([
            ("source_driver".to_owned(), "files".to_owned()),
            ("transport".to_owned(), "http".to_owned()),
            (
                "discovery_manifest_hash".to_owned(),
                "sha256:framework".to_owned(),
            ),
        ]);
        framework.extend(namespaced_driver_evidence(&driver).unwrap());

        assert_eq!(framework["source_driver"], "files");
        assert_eq!(framework["transport"], "http");
        assert_eq!(framework["discovery_manifest_hash"], "sha256:framework");
        assert_eq!(framework["driver.source_driver"], "attacker");
        assert_eq!(framework["driver.transport"], "attacker");
        assert_eq!(
            framework["driver.discovery_manifest_hash"],
            "sha256:attacker"
        );

        let secret = namespaced_driver_evidence(&BTreeMap::from([(
            "api_token".to_owned(),
            "opaque-super-secret".to_owned(),
        )]))
        .unwrap_err();
        assert!(!secret.message.contains("opaque-super-secret"));
    }

    #[test]
    fn malformed_strong_identity_fails_instead_of_bypassing_the_cache() {
        let mut candidate = SourceDiscoveryCandidate::new(
            "https://example.test/events.parquet",
            None,
            None,
            BTreeMap::new(),
        )
        .unwrap();
        candidate.identity.insert("etag".to_owned(), String::new());
        let error = registered_cache_source(&candidate).unwrap_err();
        assert!(error.message.contains("invalid or sensitive key or value"));
    }
}
