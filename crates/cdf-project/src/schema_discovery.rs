use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

use cdf_contract::{
    AggregateFileSchemaVerdict, AggregateMetadataVariance, AggregateSchemaCandidate,
    ContractPolicy, IdentifierPolicy, NORMALIZER_NAMECASE_V1, RuleOutcome, SchemaEvolutionMode,
    normalize_arrow_schema, plan_aggregate_arrow_schema_join, reconcile_schema,
};
use cdf_declarative::{
    CompiledResource, CompiledResourcePlan, FileFormatDeclaration, FileRuntimeDependencies,
    FileTransportLocation, FileTransportResource, discover_local_arrow_ipc_schema_bounded,
    discover_local_parquet_schema_bounded, discover_rest_sample_schema,
    discover_transport_parquet_schema, local_file_discovery_candidates, physical_arrow_schema_hash,
    postgres_table_target_for_sql_plan,
};
use cdf_dest_postgres::{POSTGRES_CATALOG_DISCOVERY_PROBE, discover_postgres_table_catalog_schema};
use cdf_http::{HttpTransport, SecretProvider};
use cdf_kernel::{
    CdfError, DiscoveryCoverageEvidence, DiscoveryExecutorBudgetEvidence,
    EffectiveSchemaCatalogEntry, EffectiveSchemaEvidence, EffectiveSchemaObservationEvidence,
    EffectiveSchemaRuntime, PartitionId, PartitionPlan, ResourceDescriptor, ResourceStream, Result,
    ScanRequest, SchemaHash, SchemaObservationFieldQuarantine, SchemaObservationPolicy,
    SchemaSource, ScopeKey, TerminalSchemaObservationQuarantine,
};

use crate::{
    DiscoveredParquetSchemaSnapshot, DiscoveryBoundedIdentity, DiscoveryCandidateEvidence,
    DiscoveryCoverageMode, DiscoveryExecutorBudget, DiscoveryIdentityStrength,
    DiscoveryManifestArtifact, DiscoveryManifestInput, DiscoveryManifestStore,
    DiscoveryMetadataScope, DiscoveryMetadataVariance, DiscoveryParticipation,
    DiscoverySchemaVerdict, DiscoverySchemaVerdictKind, DiscoverySelectorCandidate,
    SCHEMA_DISCOVERY_FORMAT_ARROW_IPC, SCHEMA_DISCOVERY_FORMAT_PARQUET,
    SCHEMA_DISCOVERY_PROBE_ARROW_IPC_FILE_SCHEMA, SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER,
    SchemaSnapshotArtifact, SchemaSnapshotStore, plan_discovery_selection,
};

#[derive(Clone, Debug)]
pub struct PreparedDiscoveredResource {
    pub resource: CompiledResource,
    pub discovery: Option<ResourceSchemaDiscovery>,
}

#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct PreparedEffectiveSchemaResource {
    resource: CompiledResource,
    discovery_manifest: Option<DiscoveryManifestArtifact>,
}

impl PreparedEffectiveSchemaResource {
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
    baseline_observation_schema_hashes: BTreeSet<SchemaHash>,
}

impl VerifiedSchemaBaseline {
    pub(crate) fn from_hydrated_snapshot(
        resource_id: cdf_kernel::ResourceId,
        snapshot: cdf_kernel::SchemaSnapshotReference,
        schema: arrow_schema::SchemaRef,
        baseline_observation_schema_hashes: BTreeSet<SchemaHash>,
    ) -> Self {
        Self {
            resource_id,
            snapshot,
            schema,
            baseline_observation_schema_hashes,
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
        self.baseline_observation_schema_hashes
            .contains(schema_hash)
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct SchemaDiscoveryExecutionOptions {
    budget: DiscoveryExecutorBudget,
    verified_baseline: Option<VerifiedSchemaBaseline>,
    runtime_effective_schema: bool,
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
        self.verified_baseline = Some(baseline);
        self
    }

    pub fn for_runtime_effective_schema(mut self) -> Self {
        self.runtime_effective_schema = true;
        self
    }

    pub fn budget(&self) -> &DiscoveryExecutorBudget {
        &self.budget
    }

    pub fn verified_baseline(&self) -> Option<&VerifiedSchemaBaseline> {
        self.verified_baseline.as_ref()
    }

    fn verified_baseline_hash_for(
        &self,
        resource_id: &cdf_kernel::ResourceId,
    ) -> Result<Option<SchemaHash>> {
        match self.verified_baseline.as_ref() {
            Some(baseline) if baseline.resource_id() != resource_id => {
                Err(CdfError::contract(format!(
                    "verified discovery baseline belongs to resource `{}` but discovery is for `{resource_id}`",
                    baseline.resource_id()
                )))
            }
            Some(baseline) => Ok(Some(baseline.schema_hash().clone())),
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

#[derive(Clone, Debug)]
pub struct LocalParquetSchemaDiscovery {
    pub normalized_schema: arrow_schema::SchemaRef,
    pub snapshot: DiscoveredParquetSchemaSnapshot,
    pub partition: PartitionPlan,
}

pub fn discover_resource_schema(
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
) -> Result<ResourceSchemaDiscovery> {
    Ok(
        discover_resource_schema_artifacts(resource, secret_provider, Default::default())?
            .discovery,
    )
}

pub fn discover_resource_schema_artifacts(
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    options: SchemaDiscoveryExecutionOptions,
) -> Result<ResourceSchemaDiscoveryArtifacts> {
    discover_resource_schema_artifacts_inner(resource, secret_provider, None, None, options)
}

pub fn discover_resource_schema_with_rest_transport(
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    rest_transport: &mut dyn HttpTransport,
) -> Result<ResourceSchemaDiscovery> {
    Ok(discover_resource_schema_artifacts_inner(
        resource,
        secret_provider,
        Some(rest_transport),
        None,
        Default::default(),
    )?
    .discovery)
}

pub fn discover_resource_schema_with_file_dependencies(
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    file_dependencies: FileRuntimeDependencies,
) -> Result<ResourceSchemaDiscovery> {
    Ok(discover_resource_schema_with_file_dependencies_artifacts(
        resource,
        secret_provider,
        file_dependencies,
        Default::default(),
    )?
    .discovery)
}

pub fn discover_resource_schema_with_file_dependencies_artifacts(
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    file_dependencies: FileRuntimeDependencies,
    options: SchemaDiscoveryExecutionOptions,
) -> Result<ResourceSchemaDiscoveryArtifacts> {
    discover_resource_schema_artifacts_inner(
        resource,
        secret_provider,
        None,
        Some(file_dependencies),
        options,
    )
}

/// Prepares a pinned resource for execution against runtime schema observations.
///
/// The discovery/compiler adapter owns the decision to observe a source. Generic
/// command orchestration calls this once and does not branch on concrete formats.
pub fn prepare_pinned_resource_effective_schema(
    project_root: &Path,
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
) -> Result<CompiledResource> {
    Ok(
        prepare_pinned_resource_effective_schema_artifacts(
            project_root,
            resource,
            secret_provider,
        )?
        .into_parts()
        .0,
    )
}

pub fn prepare_pinned_resource_effective_schema_artifacts(
    project_root: &Path,
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
) -> Result<PreparedEffectiveSchemaResource> {
    let should_observe = matches!(
        resource.plan(),
        CompiledResourcePlan::Files(plan)
            if !is_http_root(&plan.root)
                && matches!(
                    plan.format,
                    FileFormatDeclaration::Parquet | FileFormatDeclaration::ArrowIpc
                )
    );
    if !should_observe {
        return Ok(PreparedEffectiveSchemaResource {
            resource: resource.clone(),
            discovery_manifest: None,
        });
    }
    let snapshot = resource
        .descriptor()
        .schema_source
        .pinned_snapshot()
        .ok_or_else(|| {
            CdfError::contract(
                "runtime effective-schema preparation requires a pinned schema snapshot",
            )
        })?;
    let (_, baseline) =
        SchemaSnapshotStore::new(project_root).read_with_verified_baseline(snapshot)?;
    let probe_resource =
        resource.with_schema_source_and_schema(SchemaSource::Discover, baseline.schema().clone());
    let artifacts = discover_resource_schema_artifacts(
        &probe_resource,
        secret_provider,
        SchemaDiscoveryExecutionOptions::new()
            .with_verified_baseline(baseline.clone())
            .for_runtime_effective_schema(),
    )?;
    let prepared = apply_effective_discovered_schema(resource, &artifacts, &baseline)?;
    Ok(PreparedEffectiveSchemaResource {
        resource: prepared,
        discovery_manifest: artifacts.discovery_manifest,
    })
}

fn discover_resource_schema_artifacts_inner(
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    rest_transport: Option<&mut dyn HttpTransport>,
    file_dependencies: Option<FileRuntimeDependencies>,
    options: SchemaDiscoveryExecutionOptions,
) -> Result<ResourceSchemaDiscoveryArtifacts> {
    ensure_discover_schema_mode(resource)?;
    match resource.plan() {
        CompiledResourcePlan::Files(plan) if !is_http_root(&plan.root) => {
            discover_local_binary_resource_schema(resource, plan, options)
        }
        CompiledResourcePlan::Files(plan) => match plan.format {
            FileFormatDeclaration::ArrowIpc => Err(unsupported_discover_slice(
                resource.descriptor(),
                "remote Arrow IPC discovery is excluded; use a local Arrow IPC file resource",
            )),
            FileFormatDeclaration::Parquet => {
                let discovery = discover_parquet_resource_schema(resource, file_dependencies)?;
                Ok(ResourceSchemaDiscoveryArtifacts {
                    discovery: ResourceSchemaDiscovery {
                        normalized_schema: Arc::clone(&discovery.normalized_schema),
                        snapshot: DiscoveredSchemaSnapshot {
                            artifact: discovery.snapshot.artifact,
                            reference: discovery.snapshot.reference,
                            source_identity: discovery.snapshot.source_identity,
                        },
                    },
                    discovery_manifest: None,
                    effective_schema_runtime: None,
                })
            }
            ref format => Err(unsupported_discover_slice(
                resource.descriptor(),
                format!(
                    "schema discovery for local file format {format:?} is not implemented in this slice; supported local binary formats are Parquet and Arrow IPC"
                ),
            )),
        },
        CompiledResourcePlan::Sql(plan) => Ok(ResourceSchemaDiscoveryArtifacts {
            discovery: discover_postgres_resource_schema(resource, plan, secret_provider)?,
            discovery_manifest: None,
            effective_schema_runtime: None,
        }),
        CompiledResourcePlan::Rest(_) => match rest_transport {
            Some(transport) => Ok(ResourceSchemaDiscoveryArtifacts {
                discovery: discover_rest_resource_schema(resource, secret_provider, transport)?,
                discovery_manifest: None,
                effective_schema_runtime: None,
            }),
            None => Err(unsupported_discover_slice(
                resource.descriptor(),
                "REST resource discovery requires an explicit HTTP transport",
            )),
        },
    }
}

#[derive(Clone, Debug)]
struct LocalBinaryProbe {
    location: String,
    size_bytes: u64,
    modified_at_ms: Option<i64>,
    bounded_identity_value: String,
    physical_schema_hash: SchemaHash,
    probe_bytes: u64,
    schema: arrow_schema::SchemaRef,
    source_identity: BTreeMap<String, String>,
}

#[derive(Clone, Copy, Debug)]
enum LocalBinaryDiscoveryAdapter {
    Parquet,
    ArrowIpc,
}

impl LocalBinaryDiscoveryAdapter {
    fn for_format(resource: &CompiledResource, format: &FileFormatDeclaration) -> Result<Self> {
        match format {
            FileFormatDeclaration::Parquet => Ok(Self::Parquet),
            FileFormatDeclaration::ArrowIpc => Ok(Self::ArrowIpc),
            _ => Err(unsupported_discover_slice(
                resource.descriptor(),
                format!("local exhaustive binary discovery does not support {format:?}"),
            )),
        }
    }

    fn probe(
        self,
        candidate: &cdf_declarative::LocalFileDiscoveryCandidate,
        budget: &DiscoveryExecutorBudget,
    ) -> Result<(arrow_schema::SchemaRef, BTreeMap<String, String>, u64)> {
        match self {
            Self::Parquet => {
                let probe = discover_local_parquet_schema_bounded(
                    &candidate.path,
                    candidate.selection_bytes_read,
                    budget.max_metadata_bytes_per_file(),
                )?;
                Ok((probe.schema, probe.source_identity, probe.probe_bytes_read))
            }
            Self::ArrowIpc => {
                let probe = discover_local_arrow_ipc_schema_bounded(
                    &candidate.path,
                    candidate.selection_bytes_read,
                    budget.max_metadata_bytes_per_file(),
                )?;
                Ok((probe.schema, probe.source_identity, probe.probe_bytes_read))
            }
        }
    }

    fn snapshot_metadata(self) -> BTreeMap<String, String> {
        let (probe, format) = match self {
            Self::Parquet => (
                SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER,
                SCHEMA_DISCOVERY_FORMAT_PARQUET,
            ),
            Self::ArrowIpc => (
                SCHEMA_DISCOVERY_PROBE_ARROW_IPC_FILE_SCHEMA,
                SCHEMA_DISCOVERY_FORMAT_ARROW_IPC,
            ),
        };
        BTreeMap::from([
            ("probe".to_owned(), probe.to_owned()),
            ("format".to_owned(), format.to_owned()),
            ("source_kind".to_owned(), "files".to_owned()),
            (
                "cdf:normalizer".to_owned(),
                NORMALIZER_NAMECASE_V1.to_owned(),
            ),
        ])
    }
}

fn discover_local_binary_resource_schema(
    resource: &CompiledResource,
    plan: &cdf_declarative::FileResourcePlan,
    options: SchemaDiscoveryExecutionOptions,
) -> Result<ResourceSchemaDiscoveryArtifacts> {
    ensure_discover_schema_mode(resource)?;
    let baseline_schema_hash =
        options.verified_baseline_hash_for(&resource.descriptor().resource_id)?;
    let adapter = LocalBinaryDiscoveryAdapter::for_format(resource, &plan.format)?;
    let candidates = local_file_discovery_candidates(&resource.descriptor().resource_id, plan)?;
    if candidates.is_empty() {
        return Err(CdfError::data(format!(
            "local binary discovery for resource `{}` matched no files under `{}` for glob `{}`",
            resource.descriptor().resource_id,
            plan.root,
            plan.glob
        )));
    }

    let selector_candidates = candidates
        .iter()
        .map(|candidate| DiscoverySelectorCandidate {
            canonical_location: candidate.relative_path.clone(),
            identity: selector_candidate_identity(candidate),
        })
        .collect::<Vec<_>>();
    let selection = plan_discovery_selection(
        &resource.descriptor().resource_id,
        resource.schema_discovery_sample_files(),
        &selector_candidates,
    )?;
    let coverage_label = match selection.coverage {
        DiscoveryCoverageMode::Exhaustive => "exhaustive",
        DiscoveryCoverageMode::Sampled => "sampled",
    };

    let scheduled_candidates = candidates
        .iter()
        .filter(|candidate| {
            options.runtime_effective_schema || selection.selects(&candidate.relative_path)
        })
        .collect::<Vec<_>>();
    let mut probes = Vec::with_capacity(scheduled_candidates.len());
    let mut probe_reports = Vec::with_capacity(scheduled_candidates.len());
    let mut failed = false;
    for candidate in scheduled_candidates {
        match probe_local_binary_candidate(resource, adapter, candidate, &options.budget) {
            Ok(probe) => {
                probe_reports.push(format!(
                    "{}: probed {} metadata bytes",
                    probe.location, probe.probe_bytes
                ));
                probes.push(probe);
            }
            Err(error) => {
                failed = true;
                probe_reports.push(format!("{}: failed: {}", candidate.relative_path, error));
            }
        }
    }
    if failed {
        return Err(CdfError::data(format!(
            "{coverage_label} local binary discovery failed for resource `{}` after evaluating every selected candidate without substitution: {}",
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
    if !options.runtime_effective_schema && !file_aggregate.is_compatible() {
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
            if selection.coverage == DiscoveryCoverageMode::Sampled {
                "initial sampled schema pin"
            } else {
                "initial exhaustive schema pin"
            },
            resource.descriptor().resource_id,
        )));
    }

    let contract = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    let (effective_schema, terminal_quarantines) = if options.runtime_effective_schema {
        let baseline = options.verified_baseline().ok_or_else(|| {
            CdfError::contract(
                "runtime effective-schema discovery requires a verified baseline snapshot",
            )
        })?;
        classify_runtime_schema_observations(&probes, baseline, &contract)?
    } else {
        (
            normalize_arrow_schema(&file_aggregate.schema, &IdentifierPolicy::default())?,
            Vec::new(),
        )
    };
    let normalized = effective_schema;
    let normalized = Arc::new(normalized);
    let metadata = adapter.snapshot_metadata();
    let effective = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        normalized.as_ref(),
        metadata.clone(),
    )?;
    let manifest_candidates = candidates
        .iter()
        .map(|candidate| {
            if !selection.selects(&candidate.relative_path) {
                return Ok(unprobed_manifest_candidate(candidate));
            }
            let probe = probes
                .iter()
                .find(|probe| probe.location == candidate.relative_path)
                .ok_or_else(|| {
                    CdfError::internal(format!(
                        "selected discovery candidate `{}` was not probed",
                        candidate.relative_path
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
                (selection.coverage == DiscoveryCoverageMode::Sampled)
                    .then(|| selector_candidate_identity(candidate)),
                terminal_quarantines
                    .iter()
                    .find(|item| item.observation_id() == probe.location),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let manifest = DiscoveryManifestArtifact::new(DiscoveryManifestInput {
        resource_id: resource.descriptor().resource_id.to_string(),
        baseline_schema_hash,
        // This is intentionally the schema-only v1 identity. The final v2
        // snapshot binds this manifest, so using that final hash here would be
        // circular. The manifest hash and linked v2 snapshot hash remain the
        // authoritative identities of the complete discovery evidence.
        effective_schema_hash: Some(effective.schema_hash),
        coverage: selection.coverage.clone(),
        selector: selection.selector.clone(),
        budget: options.budget.clone(),
        normalizer_version: NORMALIZER_NAMECASE_V1.to_owned(),
        policy_version: crate::internal::semantic_hash(&ContractPolicy::for_trust(
            resource.descriptor().trust_level.clone(),
        ))?,
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
                "{coverage_label} local binary discovery metadata byte accounting overflowed for resource `{}` while adding `{}`; reduce the matched file set or probe budget",
                resource.descriptor().resource_id,
                probe.location
            ))
        })
    })?;
    let mut source_identity = BTreeMap::from([
        ("transport".to_owned(), "local".to_owned()),
        ("coverage".to_owned(), coverage_label.to_owned()),
        ("matched_files".to_owned(), candidates.len().to_string()),
        (
            "probed_files".to_owned(),
            selection.selected_count().to_string(),
        ),
        (
            "unprobed_files".to_owned(),
            (candidates.len() - selection.selected_count()).to_string(),
        ),
        ("probe_bytes_read".to_owned(), total_probe_bytes.to_string()),
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
        source_identity.extend(probe.source_identity.clone());
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
    let effective_schema_runtime = if options.runtime_effective_schema {
        let baseline = options.verified_baseline().ok_or_else(|| {
            CdfError::contract(
                "runtime effective-schema discovery requires a verified baseline snapshot",
            )
        })?;
        let effective_snapshot_schema_hash =
            manifest.effective_schema_hash.clone().ok_or_else(|| {
                CdfError::internal(
                    "local binary discovery manifest omitted its effective schema snapshot hash",
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
        let mut evidence = EffectiveSchemaEvidence::new(
            baseline.snapshot().clone(),
            effective_snapshot_schema_hash,
            manifest.reference(),
            observations,
        )?;
        if let Some(selector) = &manifest.selector {
            evidence = evidence.with_discovery_coverage(DiscoveryCoverageEvidence::sampled(
                selector.selector.clone(),
                selector.sample_files,
                selector.matched_count,
                u64::try_from(selection.selected_count()).map_err(|_| {
                    CdfError::contract("sampled discovery selected count exceeds u64")
                })?,
            )?)?;
        }
        Some(
            EffectiveSchemaRuntime::new(evidence, schema_catalog)?
                .with_terminal_quarantines(terminal_quarantines)?
                .with_discovery_executor_budget(DiscoveryExecutorBudgetEvidence::new(
                    options.budget.max_metadata_bytes_per_file(),
                    options.budget.max_total_in_flight_bytes(),
                    options.budget.max_concurrent_probes(),
                )?)?,
        )
    } else {
        None
    };
    Ok(ResourceSchemaDiscoveryArtifacts {
        discovery,
        discovery_manifest: Some(manifest),
        effective_schema_runtime,
    })
}

fn probe_local_binary_candidate(
    resource: &CompiledResource,
    adapter: LocalBinaryDiscoveryAdapter,
    candidate: &cdf_declarative::LocalFileDiscoveryCandidate,
    budget: &DiscoveryExecutorBudget,
) -> Result<LocalBinaryProbe> {
    if candidate.compression != "none" {
        return Err(unsupported_discover_slice(
            resource.descriptor(),
            format!(
                "compressed {adapter:?} discovery is excluded; use an uncompressed binary file",
            ),
        ));
    }
    let (schema, source_identity, probe_bytes) = adapter.probe(candidate, budget)?;
    let modified_at_ms = source_identity
        .get("modified_unix_millis")
        .map(|value| {
            value.parse::<i64>().map_err(|error| {
                CdfError::data(format!(
                    "invalid modification time `{value}` for `{}`: {error}",
                    candidate.relative_path
                ))
            })
        })
        .transpose()?;
    let physical_schema_hash = physical_arrow_schema_hash(schema.as_ref())?;
    let fingerprint = source_identity
        .get("footer_sha256")
        .or_else(|| source_identity.get("schema_hash"))
        .cloned()
        .unwrap_or_else(|| physical_schema_hash.to_string());
    Ok(LocalBinaryProbe {
        location: candidate.relative_path.clone(),
        size_bytes: candidate.size_bytes,
        modified_at_ms,
        bounded_identity_value: fingerprint,
        physical_schema_hash,
        probe_bytes,
        schema,
        source_identity,
    })
}

fn classify_runtime_schema_observations(
    probes: &[LocalBinaryProbe],
    baseline: &VerifiedSchemaBaseline,
    contract: &ContractPolicy,
) -> Result<(
    arrow_schema::Schema,
    Vec<TerminalSchemaObservationQuarantine>,
)> {
    let effective = baseline.schema().as_ref().clone();
    let mut physical_type_policy = contract.types.clone();
    physical_type_policy.coerce_types = false;
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
        let constrained = reconcile_schema(
            probe.schema.as_ref(),
            baseline.schema().as_ref(),
            &physical_type_policy,
        );
        if constrained.is_ok() && !freeze_deviation {
            continue;
        }

        let mut fields = report
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
    let effective = baseline.schema().as_ref().clone();
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
    probe: &LocalBinaryProbe,
    verdict: &AggregateFileSchemaVerdict,
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
        transport: "file".to_owned(),
        canonical_location: probe.location.clone(),
        identity: selector_identity.unwrap_or_else(|| DiscoveryBoundedIdentity {
            size_bytes: Some(probe.size_bytes),
            modified_at_ms: probe.modified_at_ms,
            value: Some(probe.bounded_identity_value.clone()),
            strength: DiscoveryIdentityStrength::BoundedObservation,
        }),
        participation: DiscoveryParticipation::Probed,
        metadata_variance: manifest_metadata_variance(verdict),
        physical_schema_hash: Some(probe.physical_schema_hash.clone()),
        probe_bytes: Some(probe.probe_bytes),
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

fn selector_candidate_identity(
    candidate: &cdf_declarative::LocalFileDiscoveryCandidate,
) -> DiscoveryBoundedIdentity {
    DiscoveryBoundedIdentity {
        size_bytes: Some(candidate.size_bytes),
        modified_at_ms: candidate.modified_at_ms(),
        value: None,
        strength: DiscoveryIdentityStrength::Unavailable,
    }
}

fn unprobed_manifest_candidate(
    candidate: &cdf_declarative::LocalFileDiscoveryCandidate,
) -> DiscoveryCandidateEvidence {
    DiscoveryCandidateEvidence {
        transport: "file".to_owned(),
        canonical_location: candidate.relative_path.clone(),
        identity: selector_candidate_identity(candidate),
        participation: DiscoveryParticipation::Unprobed,
        metadata_variance: Vec::new(),
        physical_schema_hash: None,
        probe_bytes: None,
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

fn discover_parquet_resource_schema(
    resource: &CompiledResource,
    file_dependencies: Option<FileRuntimeDependencies>,
) -> Result<LocalParquetSchemaDiscovery> {
    match resource.plan() {
        CompiledResourcePlan::Files(plan) if is_http_root(&plan.root) => {
            let dependencies = file_dependencies.ok_or_else(|| {
                unsupported_discover_slice(
                    resource.descriptor(),
                    "HTTP(S) Parquet discovery requires explicit file transport dependencies",
                )
            })?;
            discover_http_parquet_resource_schema(resource, dependencies)
        }
        CompiledResourcePlan::Files(_) => Err(unsupported_discover_slice(
            resource.descriptor(),
            "local Parquet discovery must use the resource-level exhaustive discovery API",
        )),
        CompiledResourcePlan::Rest(_) | CompiledResourcePlan::Sql(_) => Err(
            unsupported_discover_slice(resource.descriptor(), "resource is not a file resource"),
        ),
    }
}

#[deprecated(
    note = "use discover_resource_schema_artifacts; this compatibility helper can represent exactly one local Parquet file"
)]
pub fn discover_local_parquet_resource_schema(
    resource: &CompiledResource,
) -> Result<LocalParquetSchemaDiscovery> {
    ensure_discover_schema_mode(resource)?;
    let plan = match resource.plan() {
        CompiledResourcePlan::Files(plan)
            if plan.format == FileFormatDeclaration::Parquet && !is_http_root(&plan.root) =>
        {
            plan
        }
        _ => {
            return Err(unsupported_discover_slice(
                resource.descriptor(),
                "local Parquet discovery requires a local Parquet file resource",
            ));
        }
    };
    let candidates = local_file_discovery_candidates(&resource.descriptor().resource_id, plan)?;
    let first = match candidates.as_slice() {
        [first] => first,
        [] => {
            return Err(CdfError::data(format!(
                "local Parquet discovery for resource `{}` matched no files under `{}` for glob `{}`",
                resource.descriptor().resource_id,
                plan.root,
                plan.glob
            )));
        }
        _ => {
            return Err(CdfError::contract(format!(
                "legacy local Parquet discovery helper cannot represent {} matched candidates for resource `{}` without partial evidence; use `discover_resource_schema_artifacts` for exhaustive resource-level discovery",
                candidates.len(),
                resource.descriptor().resource_id
            )));
        }
    };
    let artifacts = discover_local_binary_resource_schema(resource, plan, Default::default())?;
    let discovery = artifacts.discovery;
    let artifact = discovery.snapshot.artifact;
    let snapshot = DiscoveredParquetSchemaSnapshot {
        reference: discovery.snapshot.reference,
        artifact,
        source_identity: discovery.snapshot.source_identity,
    };
    let partition_id = PartitionId::new(format!(
        "discovery:{}:{}",
        resource.descriptor().resource_id,
        first.relative_path
    ))?;
    let partition = PartitionPlan {
        partition_id,
        scope: ScopeKey::File {
            path: first.relative_path.clone(),
        },
        start_position: None,
        metadata: BTreeMap::from([
            ("path".to_owned(), first.relative_path.clone()),
            ("discovery_only".to_owned(), "true".to_owned()),
        ]),
    };

    Ok(LocalParquetSchemaDiscovery {
        normalized_schema: discovery.normalized_schema,
        snapshot,
        partition,
    })
}

fn discover_http_parquet_resource_schema(
    resource: &CompiledResource,
    dependencies: FileRuntimeDependencies,
) -> Result<LocalParquetSchemaDiscovery> {
    ensure_discover_schema_mode(resource)?;
    let (plan, partition) = single_http_parquet_partition(resource, &dependencies)?;
    let url = partition.metadata.get("path").cloned().ok_or_else(|| {
        CdfError::contract(format!(
            "HTTP(S) Parquet discovery for resource `{}` expected file partition URL metadata",
            resource.descriptor().resource_id
        ))
    })?;
    let resource_request = FileTransportResource {
        location: FileTransportLocation::HttpUrl { url },
        egress_allowlist: plan.allowlist.clone(),
        auth: plan.auth.clone(),
        credentials: plan.credentials.clone(),
    };
    let mut probe = discover_transport_parquet_schema(resource_request, &dependencies)?;
    let normalized = normalize_arrow_schema(probe.schema.as_ref(), &IdentifierPolicy::default())?;
    let normalized = Arc::new(normalized);
    let metadata = BTreeMap::from([
        (
            "probe".to_owned(),
            SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER.to_owned(),
        ),
        (
            "format".to_owned(),
            SCHEMA_DISCOVERY_FORMAT_PARQUET.to_owned(),
        ),
        ("source_kind".to_owned(), "files".to_owned()),
        (
            "cdf:normalizer".to_owned(),
            NORMALIZER_NAMECASE_V1.to_owned(),
        ),
    ]);
    let artifact = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        normalized.as_ref(),
        metadata,
    )?;
    probe
        .source_identity
        .insert("transport".to_owned(), "https".to_owned());
    let snapshot = DiscoveredParquetSchemaSnapshot {
        reference: artifact.reference(),
        artifact,
        source_identity: probe.source_identity,
    };

    Ok(LocalParquetSchemaDiscovery {
        normalized_schema: normalized,
        snapshot,
        partition,
    })
}

fn discover_postgres_resource_schema(
    resource: &CompiledResource,
    plan: &cdf_declarative::SqlResourcePlan,
    secret_provider: &dyn SecretProvider,
) -> Result<ResourceSchemaDiscovery> {
    if let Some(dialect) = &plan.dialect
        && !dialect.eq_ignore_ascii_case("postgres")
    {
        return Err(unsupported_discover_slice(
            resource.descriptor(),
            format!(
                "SQL dialect `{dialect}` discovery is not implemented in this slice; only dialect `postgres` table resources support catalog discovery"
            ),
        ));
    }
    let target = postgres_table_target_for_sql_plan(plan).map_err(|error| {
        unsupported_discover_slice(
            resource.descriptor(),
            format!(
                "Postgres table catalog discovery is unavailable: {}",
                error.message
            ),
        )
    })?;
    let secret = secret_provider.resolve(&plan.connection)?;
    let probe = discover_postgres_table_catalog_schema(
        secret.as_str()?,
        &resource.descriptor().resource_id,
        &target,
    )?;
    let metadata = BTreeMap::from([
        (
            "probe".to_owned(),
            POSTGRES_CATALOG_DISCOVERY_PROBE.to_owned(),
        ),
        ("source_kind".to_owned(), "sql".to_owned()),
        ("dialect".to_owned(), "postgres".to_owned()),
        ("table".to_owned(), target.display_name()),
        (
            "cdf:normalizer".to_owned(),
            NORMALIZER_NAMECASE_V1.to_owned(),
        ),
    ]);
    build_schema_discovery(resource, &probe.schema, metadata, probe.source_identity)
}

fn discover_rest_resource_schema(
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    rest_transport: &mut dyn HttpTransport,
) -> Result<ResourceSchemaDiscovery> {
    let probe = discover_rest_sample_schema(resource, rest_transport, secret_provider)?;
    let metadata = BTreeMap::from([
        ("probe".to_owned(), "rest-sample-page".to_owned()),
        ("source_kind".to_owned(), "rest".to_owned()),
        (
            "cdf:normalizer".to_owned(),
            NORMALIZER_NAMECASE_V1.to_owned(),
        ),
    ]);
    build_schema_discovery(
        resource,
        probe.schema.as_ref(),
        metadata,
        probe.source_identity,
    )
}

fn build_schema_discovery(
    resource: &CompiledResource,
    schema: &arrow_schema::Schema,
    metadata: BTreeMap<String, String>,
    source_identity: BTreeMap<String, String>,
) -> Result<ResourceSchemaDiscovery> {
    let normalized = normalize_arrow_schema(schema, &IdentifierPolicy::default())?;
    let normalized = Arc::new(normalized);
    let artifact = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        normalized.as_ref(),
        metadata,
    )?;
    Ok(ResourceSchemaDiscovery {
        normalized_schema: normalized,
        snapshot: DiscoveredSchemaSnapshot {
            reference: artifact.reference(),
            artifact,
            source_identity,
        },
    })
}

pub fn prepare_local_parquet_discover_resource(
    project_root: impl AsRef<Path>,
    resource: &CompiledResource,
) -> Result<PreparedDiscoveredResource> {
    if !matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return Ok(PreparedDiscoveredResource {
            resource: resource.clone(),
            discovery: None,
        });
    }

    let discovery = discover_resource_schema_artifacts(
        resource,
        &crate::EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )?;
    prepare_discovered_schema(project_root, resource, discovery)
}

pub fn prepare_discover_resource(
    project_root: impl AsRef<Path>,
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
) -> Result<PreparedDiscoveredResource> {
    if !matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return Ok(PreparedDiscoveredResource {
            resource: resource.clone(),
            discovery: None,
        });
    }

    let discovery =
        discover_resource_schema_artifacts(resource, secret_provider, Default::default())?;
    prepare_discovered_schema(project_root, resource, discovery)
}

pub fn prepare_discover_resource_with_file_dependencies(
    project_root: impl AsRef<Path>,
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    file_dependencies: FileRuntimeDependencies,
) -> Result<PreparedDiscoveredResource> {
    if !matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return Ok(PreparedDiscoveredResource {
            resource: resource.clone(),
            discovery: None,
        });
    }

    let discovery = discover_resource_schema_with_file_dependencies_artifacts(
        resource,
        secret_provider,
        file_dependencies,
        Default::default(),
    )?;
    prepare_discovered_schema(project_root, resource, discovery)
}

pub fn prepare_discover_resource_with_rest_transport(
    project_root: impl AsRef<Path>,
    resource: &CompiledResource,
    secret_provider: &dyn SecretProvider,
    rest_transport: &mut dyn HttpTransport,
) -> Result<PreparedDiscoveredResource> {
    if !matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return Ok(PreparedDiscoveredResource {
            resource: resource.clone(),
            discovery: None,
        });
    }

    let discovery = ResourceSchemaDiscoveryArtifacts {
        discovery: discover_resource_schema_with_rest_transport(
            resource,
            secret_provider,
            rest_transport,
        )?,
        discovery_manifest: None,
        effective_schema_runtime: None,
    };
    prepare_discovered_schema(project_root, resource, discovery)
}

fn prepare_discovered_schema(
    project_root: impl AsRef<Path>,
    resource: &CompiledResource,
    artifacts: ResourceSchemaDiscoveryArtifacts,
) -> Result<PreparedDiscoveredResource> {
    write_schema_discovery_artifacts(project_root, &artifacts)?;
    Ok(apply_discovered_schema(resource, artifacts.discovery))
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
) -> PreparedDiscoveredResource {
    let pinned = resource.with_schema_source_and_schema(
        SchemaSource::Discovered {
            snapshot: discovery.snapshot.reference.clone(),
        },
        Arc::clone(&discovery.normalized_schema),
    );

    PreparedDiscoveredResource {
        resource: pinned,
        discovery: Some(discovery),
    }
}

pub fn apply_effective_discovered_schema(
    resource: &CompiledResource,
    artifacts: &ResourceSchemaDiscoveryArtifacts,
    baseline: &VerifiedSchemaBaseline,
) -> Result<CompiledResource> {
    if resource.descriptor().resource_id != *baseline.resource_id() {
        return Err(CdfError::contract(format!(
            "verified effective-schema baseline belongs to `{}` but resource is `{}`",
            baseline.resource_id(),
            resource.descriptor().resource_id
        )));
    }
    let manifest = artifacts.discovery_manifest.as_ref().ok_or_else(|| {
        CdfError::contract(
            "effective multi-file schema execution requires a discovery manifest artifact",
        )
    })?;
    manifest.validate()?;
    if manifest.baseline_schema_hash.as_ref() != Some(baseline.schema_hash()) {
        return Err(CdfError::data(
            "discovery manifest baseline hash does not match the verified pinned snapshot",
        ));
    }
    let runtime = artifacts.effective_schema_runtime.clone().ok_or_else(|| {
        CdfError::contract(
            "effective multi-file schema execution requires verified physical schema runtime evidence",
        )
    })?;
    if runtime.evidence.baseline_snapshot != *baseline.snapshot()
        || runtime.evidence.discovery_manifest != manifest.reference()
    {
        return Err(CdfError::data(
            "effective schema runtime authority does not match the verified baseline and discovery manifest",
        ));
    }
    resource.with_effective_schema(Arc::clone(&artifacts.discovery.normalized_schema), runtime)
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
    if matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
        return Ok(());
    }
    Err(CdfError::contract(format!(
        "cdf schema discover supports resources in discover schema mode; resource `{}` already has a declared or pinned schema",
        resource.descriptor().resource_id
    )))
}

fn single_http_parquet_partition<'a>(
    resource: &'a CompiledResource,
    dependencies: &FileRuntimeDependencies,
) -> Result<(&'a cdf_declarative::FileResourcePlan, PartitionPlan)> {
    let plan = match resource.plan() {
        CompiledResourcePlan::Files(plan) => plan,
        CompiledResourcePlan::Rest(_) | CompiledResourcePlan::Sql(_) => {
            return Err(unsupported_discover_slice(
                resource.descriptor(),
                "HTTP(S) Parquet discovery only supports file resources",
            ));
        }
    };
    if plan.format != FileFormatDeclaration::Parquet {
        return Err(unsupported_discover_slice(
            resource.descriptor(),
            format!(
                "only HTTP(S) single-file Parquet discovery is implemented in this slice; resource uses format = {:?}",
                plan.format
            ),
        ));
    }
    let runtime = resource.to_file_resource(dependencies.clone())?;
    let partitions = runtime.plan_partitions(&discovery_scan_request(resource.descriptor())?)?;
    match partitions.as_slice() {
        [partition] => Ok((plan, partition.clone())),
        [] => Err(CdfError::data(format!(
            "HTTP(S) Parquet discovery for resource `{}` matched no file for `{}` and glob `{}`",
            resource.descriptor().resource_id,
            plan.root,
            plan.glob
        ))),
        _ => Err(CdfError::contract(format!(
            "multi-file HTTP(S) Parquet discovery is unsupported for resource `{}`; glob `{}` under `{}` resolved to {} files",
            resource.descriptor().resource_id,
            plan.glob,
            plan.root,
            partitions.len()
        ))),
    }
}

fn is_http_root(root: &str) -> bool {
    root.starts_with("http://") || root.starts_with("https://")
}

fn discovery_scan_request(descriptor: &ResourceDescriptor) -> Result<ScanRequest> {
    Ok(ScanRequest {
        resource_id: descriptor.resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: descriptor.state_scope.clone(),
    })
}

fn unsupported_discover_slice(
    descriptor: &ResourceDescriptor,
    reason: impl Into<String>,
) -> CdfError {
    CdfError::contract(format!(
        "unsupported schema discovery slice for resource `{}`: {}",
        descriptor.resource_id,
        reason.into()
    ))
}

#[cfg(test)]
mod terminal_evidence_tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Fields, Schema};

    use super::canonical_field_at_path;

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
}
