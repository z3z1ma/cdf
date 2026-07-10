use std::{collections::BTreeMap, path::Path, sync::Arc};

use cdf_contract::{
    AggregateFileSchemaVerdict, AggregateMetadataVariance, AggregateSchemaCandidate,
    ContractPolicy, IdentifierPolicy, NORMALIZER_NAMECASE_V1, RuleOutcome, normalize_arrow_schema,
    plan_aggregate_arrow_schema_join,
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
    CdfError, PartitionId, PartitionPlan, ResourceDescriptor, ResourceStream, Result, ScanRequest,
    SchemaHash, SchemaSource, ScopeKey,
};

use crate::{
    DiscoveredParquetSchemaSnapshot, DiscoveryBoundedIdentity, DiscoveryCandidateEvidence,
    DiscoveryCoverageMode, DiscoveryExecutorBudget, DiscoveryIdentityStrength,
    DiscoveryManifestArtifact, DiscoveryManifestInput, DiscoveryManifestStore,
    DiscoveryMetadataScope, DiscoveryMetadataVariance, DiscoveryParticipation,
    DiscoverySchemaVerdict, DiscoverySchemaVerdictKind, SCHEMA_DISCOVERY_FORMAT_ARROW_IPC,
    SCHEMA_DISCOVERY_FORMAT_PARQUET, SCHEMA_DISCOVERY_PROBE_ARROW_IPC_FILE_SCHEMA,
    SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER, SchemaSnapshotArtifact, SchemaSnapshotStore,
};

#[derive(Clone, Debug)]
pub struct PreparedDiscoveredResource {
    pub resource: CompiledResource,
    pub discovery: Option<ResourceSchemaDiscovery>,
}

#[derive(Clone, Debug)]
pub struct ResourceSchemaDiscovery {
    pub normalized_schema: arrow_schema::SchemaRef,
    pub snapshot: DiscoveredSchemaSnapshot,
}

#[derive(Clone, Debug)]
pub struct ResourceSchemaDiscoveryArtifacts {
    pub discovery: ResourceSchemaDiscovery,
    pub discovery_manifest: Option<DiscoveryManifestArtifact>,
}

/// Authority token proving that a schema snapshot and its linked discovery
/// evidence were hydrated and verified by [`SchemaSnapshotStore`].
///
/// Callers cannot construct this token from an arbitrary hash. Obtain it with
/// [`SchemaSnapshotStore::read_with_verified_baseline`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedSchemaBaseline {
    resource_id: cdf_kernel::ResourceId,
    schema_hash: SchemaHash,
}

impl VerifiedSchemaBaseline {
    pub(crate) fn from_hydrated_snapshot(
        resource_id: cdf_kernel::ResourceId,
        schema_hash: SchemaHash,
    ) -> Self {
        Self {
            resource_id,
            schema_hash,
        }
    }

    pub fn resource_id(&self) -> &cdf_kernel::ResourceId {
        &self.resource_id
    }

    pub fn schema_hash(&self) -> &SchemaHash {
        &self.schema_hash
    }
}

#[non_exhaustive]
#[derive(Clone, Debug, Default)]
pub struct SchemaDiscoveryExecutionOptions {
    budget: DiscoveryExecutorBudget,
    verified_baseline: Option<VerifiedSchemaBaseline>,
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
        }),
        CompiledResourcePlan::Rest(_) => match rest_transport {
            Some(transport) => Ok(ResourceSchemaDiscoveryArtifacts {
                discovery: discover_rest_resource_schema(resource, secret_provider, transport)?,
                discovery_manifest: None,
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
            "exhaustive local binary discovery for resource `{}` matched no files under `{}` for glob `{}`",
            resource.descriptor().resource_id,
            plan.root,
            plan.glob
        )));
    }

    let mut probes = Vec::with_capacity(candidates.len());
    let mut probe_reports = Vec::with_capacity(candidates.len());
    let mut failed = false;
    for candidate in &candidates {
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
            "exhaustive local binary discovery failed for resource `{}` after evaluating every matched candidate: {}",
            resource.descriptor().resource_id,
            probe_reports.join("; ")
        )));
    }

    let aggregate_candidates = probes
        .iter()
        .map(|probe| {
            AggregateSchemaCandidate::new(probe.location.clone(), probe.schema.as_ref().clone())
        })
        .collect::<Vec<_>>();
    let aggregate = plan_aggregate_arrow_schema_join(&aggregate_candidates)?;
    if !aggregate.is_compatible() {
        let file_reports = aggregate
            .files
            .iter()
            .map(aggregate_file_report)
            .collect::<Vec<_>>()
            .join("; ");
        let incompatibilities = aggregate
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
            "initial exhaustive schema pin for resource `{}` found incompatible files; candidate verdicts: {file_reports}; incompatibilities: {incompatibilities}",
            resource.descriptor().resource_id
        )));
    }

    let normalized = normalize_arrow_schema(&aggregate.schema, &IdentifierPolicy::default())?;
    let normalized = Arc::new(normalized);
    let metadata = adapter.snapshot_metadata();
    let effective = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        normalized.as_ref(),
        metadata.clone(),
    )?;
    let manifest_candidates = probes
        .iter()
        .map(|probe| {
            let verdict = aggregate
                .files
                .iter()
                .find(|verdict| verdict.location == probe.location)
                .ok_or_else(|| {
                    CdfError::internal(format!(
                        "aggregate schema report omitted candidate `{}`",
                        probe.location
                    ))
                })?;
            manifest_candidate(probe, verdict)
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
        coverage: DiscoveryCoverageMode::Exhaustive,
        selector: None,
        budget: options.budget,
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
    let total_probe_bytes = probes.iter().try_fold(0_u64, |total, probe| {
        total.checked_add(probe.probe_bytes).ok_or_else(|| {
            CdfError::data(format!(
                "exhaustive local binary discovery metadata byte accounting overflowed for resource `{}` while adding `{}`; reduce the matched file set or probe budget",
                resource.descriptor().resource_id,
                probe.location
            ))
        })
    })?;
    let mut source_identity = BTreeMap::from([
        ("transport".to_owned(), "local".to_owned()),
        ("coverage".to_owned(), "exhaustive".to_owned()),
        ("matched_files".to_owned(), probes.len().to_string()),
        ("probed_files".to_owned(), probes.len().to_string()),
        ("probe_bytes_read".to_owned(), total_probe_bytes.to_string()),
        (
            "discovery_manifest_hash".to_owned(),
            manifest.manifest_hash.to_string(),
        ),
        ("discovery_manifest_path".to_owned(), manifest.path.clone()),
    ]);
    if let [probe] = probes.as_slice() {
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
    Ok(ResourceSchemaDiscoveryArtifacts {
        discovery,
        discovery_manifest: Some(manifest),
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

fn manifest_candidate(
    probe: &LocalBinaryProbe,
    verdict: &AggregateFileSchemaVerdict,
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
    Ok(DiscoveryCandidateEvidence {
        transport: "file".to_owned(),
        canonical_location: probe.location.clone(),
        identity: DiscoveryBoundedIdentity {
            size_bytes: Some(probe.size_bytes),
            modified_at_ms: probe.modified_at_ms,
            value: Some(probe.bounded_identity_value.clone()),
            strength: DiscoveryIdentityStrength::BoundedObservation,
        },
        participation: DiscoveryParticipation::Probed,
        metadata_variance: manifest_metadata_variance(verdict),
        physical_schema_hash: Some(probe.physical_schema_hash.clone()),
        probe_bytes: Some(probe.probe_bytes),
        schema_verdict: Some(DiscoverySchemaVerdict {
            kind: DiscoverySchemaVerdictKind::Admitted,
            rule: "aggregate-schema-join-v1".to_owned(),
            details: BTreeMap::from([
                ("outcome".to_owned(), outcome.to_owned()),
                (
                    "field_verdicts".to_owned(),
                    serde_json::to_string(&verdict.fields)
                        .map_err(|error| CdfError::internal(error.to_string()))?,
                ),
            ]),
        }),
    })
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
