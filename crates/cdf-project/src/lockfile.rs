use crate::internal::*;
use crate::*;
use cdf_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use cdf_kernel::{
    DestinationCommitRequest, DestinationProtocol, DestinationProtocolCapabilities,
    DestinationSheetArtifact, IdempotencyToken, PackageHash, ResourceStream,
    SchemaSnapshotReference, StateSegment, TargetName, WriteDisposition,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectValidationReport {
    pub environment: EffectiveEnvironment,
    pub declarative_resources: usize,
    pub external_resources: usize,
    pub checked_secrets: Vec<SecretCheck>,
}

#[derive(Clone, Debug)]
pub struct CompiledProjectResource {
    pub resource: CompiledResource,
    pub origin: ProjectResourceOrigin,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ProjectResourceOrigin {
    pub source_name: String,
    pub resource_name: String,
    pub source_file: Option<String>,
    pub mapping_pattern: String,
    pub mapping_status: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretCheck {
    pub uri: SecretRef,
    pub status: SecretCheckStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretCheckStatus {
    Resolved,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CdfLock {
    pub version: u16,
    pub project: ProjectLock,
    pub dependency_tuple: DependencyTuple,
    pub normalizer: String,
    #[serde(default)]
    pub resources: BTreeMap<String, LockedResource>,
    #[serde(default)]
    pub destinations: BTreeMap<String, LockedDestination>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectLock {
    pub name: String,
    pub default_environment: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyTuple {
    pub cdf: String,
    pub arrow_rs: String,
    pub datafusion: Option<String>,
    pub object_store: Option<String>,
    pub duckdb_rs: Option<String>,
    pub rust: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockedResource {
    pub descriptor: ResourceDescriptor,
    pub capabilities: ResourceCapabilities,
    pub capability_sheet_hash: String,
    pub schema_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_snapshot: Option<SchemaSnapshotReference>,
    pub contract: Option<ContractSnapshot>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractSnapshot {
    pub contract_ref: Option<String>,
    pub schema_hash: Option<String>,
    pub policy_hash: Option<String>,
    pub validation_program_hash: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractSnapshotCounts {
    pub frozen: usize,
    pub passed: usize,
    pub drifted: usize,
    pub missing: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractFreezeReport {
    pub registry: String,
    pub resource_ids: Vec<String>,
    pub counts: ContractSnapshotCounts,
    pub snapshots: BTreeMap<String, ContractSnapshot>,
    pub drift_details: Vec<ContractSnapshotDrift>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractTestReport {
    pub registry: String,
    pub resource_ids: Vec<String>,
    pub counts: ContractSnapshotCounts,
    pub snapshots: Vec<ContractSnapshotComparison>,
    pub drift_details: Vec<ContractSnapshotDrift>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractSnapshotComparison {
    pub resource_id: String,
    pub verdict: ContractSnapshotVerdict,
    pub frozen: ContractSnapshot,
    pub current: ContractSnapshot,
    pub drift_details: Vec<ContractSnapshotDrift>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractSnapshotVerdict {
    Pass,
    Drift,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractSnapshotDrift {
    pub resource_id: String,
    pub field: String,
    pub frozen: Option<String>,
    pub current: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct LockedDestination {
    pub sheet_hash: String,
    pub sheet: DestinationSheet,
    #[serde(
        default,
        skip_serializing_if = "DestinationProtocolCapabilities::is_default"
    )]
    pub protocol_capabilities: DestinationProtocolCapabilities,
}

impl LockedDestination {
    pub fn new(artifact: DestinationSheetArtifact) -> Result<Self> {
        let sheet_hash = semantic_hash(&artifact)?;
        Ok(Self {
            sheet_hash,
            sheet: artifact.sheet,
            protocol_capabilities: artifact.protocol_capabilities,
        })
    }

    pub fn sheet_artifact(&self) -> Result<DestinationSheetArtifact> {
        DestinationSheetArtifact::new(self.sheet.clone(), self.protocol_capabilities.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LockDiff {
    pub kind: LockDiffKind,
    pub path: String,
    pub before: Option<String>,
    pub after: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LockDiffKind {
    Added,
    Removed,
    Changed,
}

pub fn parse_cdf_toml(input: &str) -> Result<ProjectConfig> {
    let config = toml::from_str::<ProjectConfig>(input)
        .map_err(|error| CdfError::contract(error.to_string()))?;
    validate_project_shape(&config)?;
    Ok(config)
}

pub fn parse_lock(input: &str) -> Result<CdfLock> {
    toml::from_str(input).map_err(|error| CdfError::contract(error.to_string()))
}

pub fn lock_to_toml(lock: &CdfLock) -> Result<String> {
    toml::to_string_pretty(lock).map_err(|error| CdfError::contract(error.to_string()))
}

pub fn compile_project_declarative_resources(
    config: &ProjectConfig,
    resolver: &dyn ResourceSourceResolver,
) -> Result<Vec<CompiledResource>> {
    compile_project_declarative_resource_entries_inner(config, resolver, None).map(resource_entries)
}

pub fn compile_project_declarative_resources_with_root(
    config: &ProjectConfig,
    resolver: &dyn ResourceSourceResolver,
    project_root: impl AsRef<Path>,
) -> Result<Vec<CompiledResource>> {
    compile_project_declarative_resource_entries_inner(
        config,
        resolver,
        Some(project_root.as_ref()),
    )
    .map(resource_entries)
}

pub fn compile_project_declarative_resource_entries_with_root(
    config: &ProjectConfig,
    resolver: &dyn ResourceSourceResolver,
    project_root: impl AsRef<Path>,
) -> Result<Vec<CompiledProjectResource>> {
    compile_project_declarative_resource_entries_inner(
        config,
        resolver,
        Some(project_root.as_ref()),
    )
}

fn compile_project_declarative_resource_entries_inner(
    config: &ProjectConfig,
    resolver: &dyn ResourceSourceResolver,
    project_root: Option<&Path>,
) -> Result<Vec<CompiledProjectResource>> {
    let mut entries = Vec::new();
    for (pattern, mapping) in &config.resources {
        let ResourceSourceKind::DeclarativeFile { path } = mapping.source_kind() else {
            continue;
        };
        let document = parse_resolved_declarative_source(&resolver.resolve(&path)?)?;
        let compiled = match project_root {
            Some(project_root) => compile_document_with_project_root(&document, project_root)?,
            None => compile_document(&document)?,
        };
        validate_mapping_pattern(pattern, &path, &compiled)?;
        entries.extend(
            compiled
                .into_iter()
                .filter(|resource| resource_id_matches_pattern(resource, pattern))
                .map(|resource| CompiledProjectResource {
                    origin: ProjectResourceOrigin {
                        source_name: resource.source_name().to_owned(),
                        resource_name: resource.resource_name().to_owned(),
                        source_file: Some(path.clone()),
                        mapping_pattern: pattern.clone(),
                        mapping_status: "matched".to_owned(),
                    },
                    resource,
                }),
        );
    }
    Ok(entries)
}

fn resource_entries(entries: Vec<CompiledProjectResource>) -> Vec<CompiledResource> {
    entries.into_iter().map(|entry| entry.resource).collect()
}

fn validate_mapping_pattern(
    pattern: &str,
    source_file: &str,
    resources: &[CompiledResource],
) -> Result<()> {
    if resources
        .iter()
        .any(|resource| resource_id_matches_pattern(resource, pattern))
    {
        return Ok(());
    }
    let compiled_ids = resources
        .iter()
        .map(|resource| resource.descriptor().resource_id.as_str().to_owned())
        .collect::<Vec<_>>();
    let examples = resources
        .first()
        .map(|resource| {
            format!(
                "[resources.\"{}\"] or [resources.\"{}.*\"]",
                resource.descriptor().resource_id,
                resource.source_name()
            )
        })
        .unwrap_or_else(|| "[resources.\"<source>.<resource>\"]".to_owned());
    Err(CdfError::contract(format!(
        "resource mapping pattern `{pattern}` for source `{source_file}` matched zero compiled resources; compiled resource ids from that file: {}; declarative resource ids compile as `<source>.<resource>`, so update the cdf.toml mapping to {examples}",
        list_or_none(compiled_ids)
    )))
}

fn resource_id_matches_pattern(resource: &CompiledResource, pattern: &str) -> bool {
    wildcard_pattern_matches(pattern, resource.descriptor().resource_id.as_str())
}

fn wildcard_pattern_matches(pattern: &str, candidate: &str) -> bool {
    let pattern = pattern.as_bytes();
    let candidate = candidate.as_bytes();
    let mut table = vec![vec![false; candidate.len() + 1]; pattern.len() + 1];
    table[0][0] = true;
    for index in 1..=pattern.len() {
        if pattern[index - 1] == b'*' {
            table[index][0] = table[index - 1][0];
        }
    }
    for pattern_index in 1..=pattern.len() {
        for candidate_index in 1..=candidate.len() {
            table[pattern_index][candidate_index] = match pattern[pattern_index - 1] {
                b'*' => {
                    table[pattern_index - 1][candidate_index]
                        || table[pattern_index][candidate_index - 1]
                }
                byte => {
                    byte == candidate[candidate_index - 1]
                        && table[pattern_index - 1][candidate_index - 1]
                }
            };
        }
    }
    table[pattern.len()][candidate.len()]
}

fn list_or_none(items: Vec<String>) -> String {
    if items.is_empty() {
        "none".to_owned()
    } else {
        items.join(", ")
    }
}

pub fn validate_project(
    config: &ProjectConfig,
    env_name: Option<&str>,
    resolver: &dyn ResourceSourceResolver,
    provider: &dyn SecretProvider,
) -> Result<ProjectValidationReport> {
    validate_project_shape(config)?;
    let env_name = env_name.unwrap_or(&config.project.default_environment);
    let environment = config.effective_environment(env_name)?;
    validate_environment_uri_fields(&environment)?;

    let mut secret_refs = collect_secret_refs_from_environment(&environment)?;
    let compiled_entries =
        compile_project_declarative_resource_entries_inner(config, resolver, None)?;
    let declarative_resources = compiled_entries.len();
    let mut external_resources = 0;

    for mapping in config.resources.values() {
        match mapping.source_kind() {
            ResourceSourceKind::DeclarativeFile { .. } => {}
            ResourceSourceKind::Python { .. }
            | ResourceSourceKind::Rust { .. }
            | ResourceSourceKind::External { .. } => external_resources += 1,
        }
    }
    let compiled = compiled_entries
        .iter()
        .map(|entry| entry.resource.clone())
        .collect::<Vec<_>>();
    secret_refs.extend(collect_secret_refs_from_declarative(&compiled)?);

    let mut checked_secrets = Vec::new();
    for secret in dedupe_secret_refs(secret_refs) {
        provider.resolve(&secret.to_secret_uri()?)?;
        checked_secrets.push(SecretCheck {
            uri: secret,
            status: SecretCheckStatus::Resolved,
        });
    }

    Ok(ProjectValidationReport {
        environment,
        declarative_resources,
        external_resources,
        checked_secrets,
    })
}

pub fn generate_lockfile(
    config: &ProjectConfig,
    resources: &[CompiledResource],
    dependency_tuple: DependencyTuple,
    destination_sheets: &[DestinationSheet],
    contract_snapshots: BTreeMap<String, ContractSnapshot>,
) -> Result<CdfLock> {
    let destination_artifacts = destination_sheets
        .iter()
        .cloned()
        .map(|sheet| {
            DestinationSheetArtifact::new(sheet, DestinationProtocolCapabilities::default())
        })
        .collect::<Result<Vec<_>>>()?;
    generate_lockfile_with_destination_artifacts(
        config,
        resources,
        dependency_tuple,
        &destination_artifacts,
        contract_snapshots,
    )
}

pub fn generate_lockfile_with_destination_artifacts(
    config: &ProjectConfig,
    resources: &[CompiledResource],
    dependency_tuple: DependencyTuple,
    destination_artifacts: &[DestinationSheetArtifact],
    contract_snapshots: BTreeMap<String, ContractSnapshot>,
) -> Result<CdfLock> {
    validate_project_shape(config)?;
    let mut locked_resources = BTreeMap::new();
    for resource in resources {
        let descriptor = resource.descriptor().clone();
        let resource_id = descriptor.resource_id.to_string();
        let schema_hash = schema_hash_from_source(&descriptor.schema_source);
        let schema_snapshot = descriptor.schema_source.pinned_snapshot().cloned();
        let contract = Some(match contract_snapshots.get(&resource_id) {
            Some(snapshot) => snapshot.clone(),
            None => contract_snapshot_for_resource(resource)?,
        });
        locked_resources.insert(
            resource_id,
            LockedResource {
                descriptor,
                capabilities: resource.capabilities().clone(),
                capability_sheet_hash: semantic_hash(resource.capabilities())?,
                schema_hash,
                schema_snapshot,
                contract,
            },
        );
    }

    let mut destinations = BTreeMap::new();
    for artifact in destination_artifacts {
        let destination = artifact.sheet.destination.to_string();
        destinations.insert(destination, LockedDestination::new(artifact.clone())?);
    }

    Ok(CdfLock {
        version: LOCKFILE_VERSION,
        project: ProjectLock {
            name: config.project.name.clone(),
            default_environment: config.project.default_environment.clone(),
        },
        dependency_tuple,
        normalizer: config.project.normalizer.clone(),
        resources: locked_resources,
        destinations,
    })
}

pub fn contract_snapshots_for_resources(
    resources: &[CompiledResource],
    selector: Option<&str>,
) -> Result<BTreeMap<String, ContractSnapshot>> {
    let selected = selected_contract_resources(resources, selector)?;
    let mut snapshots = BTreeMap::new();
    for resource in selected {
        snapshots.insert(
            resource.descriptor().resource_id.to_string(),
            contract_snapshot_for_resource(resource)?,
        );
    }
    Ok(snapshots)
}

pub fn contract_snapshot_for_resource(resource: &CompiledResource) -> Result<ContractSnapshot> {
    let descriptor = resource.descriptor();
    let policy = ContractPolicy::for_trust(descriptor.trust_level.clone());
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let validation_program = compile_validation_program(&policy, &observed_schema)?;
    Ok(ContractSnapshot {
        contract_ref: descriptor.contract.as_ref().map(ToString::to_string),
        schema_hash: schema_hash_from_source(&descriptor.schema_source),
        policy_hash: Some(semantic_hash(&policy)?),
        validation_program_hash: Some(semantic_hash(&validation_program)?),
    })
}

pub fn freeze_contract_snapshots(
    config: &ProjectConfig,
    resources: &[CompiledResource],
    existing_lock: Option<&CdfLock>,
    destination_uri: &str,
    selector: Option<&str>,
) -> Result<(CdfLock, ContractFreezeReport)> {
    let snapshots = contract_snapshots_for_resources(resources, selector)?;
    let mut lock = match existing_lock {
        Some(lock) => lock.clone(),
        None => generate_lockfile_with_destination_artifacts(
            config,
            resources,
            current_dependency_tuple(),
            &destination_sheet_artifacts_for_uri(destination_uri)?,
            snapshots.clone(),
        )?,
    };

    if existing_lock.is_some() {
        for resource in selected_contract_resources(resources, selector)? {
            let resource_id = resource.descriptor().resource_id.to_string();
            let snapshot = snapshots
                .get(&resource_id)
                .expect("selected resource snapshot was computed")
                .clone();
            lock.resources.insert(
                resource_id,
                locked_resource_from_current(resource, snapshot)?,
            );
        }
    }

    let resource_ids = snapshots.keys().cloned().collect::<Vec<_>>();
    let report = ContractFreezeReport {
        registry: LOCK_FILE_NAME.to_owned(),
        resource_ids,
        counts: ContractSnapshotCounts {
            frozen: snapshots.len(),
            passed: 0,
            drifted: 0,
            missing: 0,
        },
        snapshots,
        drift_details: Vec::new(),
    };
    Ok((lock, report))
}

pub fn pin_schema_snapshot_in_lockfile(
    existing_lock: &CdfLock,
    resource: &CompiledResource,
) -> Result<CdfLock> {
    let mut lock = existing_lock.clone();
    let snapshot = contract_snapshot_for_resource(resource)?;
    lock.resources.insert(
        resource.descriptor().resource_id.to_string(),
        locked_resource_from_current(resource, snapshot)?,
    );
    Ok(lock)
}

pub fn pin_schema_snapshot_in_project_lockfile(
    config: &ProjectConfig,
    resources: &[CompiledResource],
    existing_lock: Option<&CdfLock>,
    destination_uri: &str,
    pinned_resource: &CompiledResource,
) -> Result<CdfLock> {
    if let Some(lock) = existing_lock {
        return pin_schema_snapshot_in_lockfile(lock, pinned_resource);
    }

    let selected_id = pinned_resource.descriptor().resource_id.as_str();
    let mut found = false;
    let resources = resources
        .iter()
        .map(|resource| {
            if resource.descriptor().resource_id.as_str() == selected_id {
                found = true;
                pinned_resource.clone()
            } else {
                resource.clone()
            }
        })
        .collect::<Vec<_>>();
    if !found {
        return Err(CdfError::contract(format!(
            "cannot pin schema snapshot for resource `{selected_id}` because it is not compiled in the project"
        )));
    }
    generate_lockfile_with_destination_artifacts(
        config,
        &resources,
        current_dependency_tuple(),
        &destination_sheet_artifacts_for_uri(destination_uri)?,
        BTreeMap::new(),
    )
}

pub fn test_contract_snapshots(
    lock: &CdfLock,
    resources: &[CompiledResource],
    selector: Option<&str>,
) -> Result<ContractTestReport> {
    let current_snapshots = contract_snapshots_for_resources(resources, selector)?;
    let mut comparisons = Vec::with_capacity(current_snapshots.len());
    let mut all_drifts = Vec::new();

    for (resource_id, current) in &current_snapshots {
        let frozen = lock
            .resources
            .get(resource_id)
            .and_then(|resource| resource.contract.as_ref())
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "{} has no frozen contract snapshot for `{resource_id}`; run `cdf contract freeze {resource_id}`",
                    LOCK_FILE_NAME
                ))
            })?;
        let drift_details = contract_snapshot_drift(resource_id, frozen, current);
        let verdict = if drift_details.is_empty() {
            ContractSnapshotVerdict::Pass
        } else {
            ContractSnapshotVerdict::Drift
        };
        all_drifts.extend(drift_details.clone());
        comparisons.push(ContractSnapshotComparison {
            resource_id: resource_id.clone(),
            verdict,
            frozen: frozen.clone(),
            current: current.clone(),
            drift_details,
        });
    }

    let drifted = comparisons
        .iter()
        .filter(|comparison| comparison.verdict == ContractSnapshotVerdict::Drift)
        .count();
    let passed = comparisons.len() - drifted;
    Ok(ContractTestReport {
        registry: LOCK_FILE_NAME.to_owned(),
        resource_ids: current_snapshots.keys().cloned().collect(),
        counts: ContractSnapshotCounts {
            frozen: 0,
            passed,
            drifted,
            missing: 0,
        },
        snapshots: comparisons,
        drift_details: all_drifts,
    })
}

pub fn diff_lockfiles(before: &CdfLock, after: &CdfLock) -> Result<Vec<LockDiff>> {
    let before =
        serde_json::to_value(before).map_err(|error| CdfError::internal(error.to_string()))?;
    let after =
        serde_json::to_value(after).map_err(|error| CdfError::internal(error.to_string()))?;
    let mut diffs = Vec::new();
    diff_json_values("$", Some(&before), Some(&after), &mut diffs);
    Ok(diffs)
}

fn current_dependency_tuple() -> DependencyTuple {
    DependencyTuple {
        cdf: env!("CARGO_PKG_VERSION").to_owned(),
        arrow_rs: "59.1.0".to_owned(),
        datafusion: None,
        object_store: None,
        duckdb_rs: None,
        rust: None,
    }
}

fn selected_contract_resources<'a>(
    resources: &'a [CompiledResource],
    selector: Option<&str>,
) -> Result<Vec<&'a CompiledResource>> {
    if resources.is_empty() {
        return Err(CdfError::contract(
            "no compiled project resources are available for contract snapshots",
        ));
    }
    match selector {
        Some(resource_id) => resources
            .iter()
            .find(|resource| resource.descriptor().resource_id.as_str() == resource_id)
            .map(|resource| vec![resource])
            .ok_or_else(|| CdfError::contract(format!("resource `{resource_id}` is not compiled"))),
        None => {
            let mut selected = resources.iter().collect::<Vec<_>>();
            selected.sort_by(|left, right| {
                left.descriptor()
                    .resource_id
                    .as_str()
                    .cmp(right.descriptor().resource_id.as_str())
            });
            Ok(selected)
        }
    }
}

fn locked_resource_from_current(
    resource: &CompiledResource,
    contract: ContractSnapshot,
) -> Result<LockedResource> {
    let descriptor = resource.descriptor().clone();
    Ok(LockedResource {
        schema_hash: schema_hash_from_source(&descriptor.schema_source),
        schema_snapshot: descriptor.schema_source.pinned_snapshot().cloned(),
        descriptor,
        capabilities: resource.capabilities().clone(),
        capability_sheet_hash: semantic_hash(resource.capabilities())?,
        contract: Some(contract),
    })
}

fn destination_sheet_artifacts_for_uri(uri: &str) -> Result<Vec<DestinationSheetArtifact>> {
    if let Some(path) = uri.strip_prefix("duckdb://") {
        if path.trim().is_empty() {
            return Err(CdfError::contract(
                "duckdb:// destination path cannot be empty",
            ));
        }
        return Ok(vec![
            cdf_dest_duckdb::DuckDbDestination::new(path)?.sheet_artifact()?,
        ]);
    }
    if uri.strip_prefix("parquet://").is_some() {
        let request = DestinationCommitRequest {
            package_hash: PackageHash::new("contract-freeze-snapshot")?,
            target: TargetName::new("contract_freeze_snapshot")?,
            disposition: WriteDisposition::Append,
            segments: Vec::<StateSegment>::new(),
            idempotency_token: IdempotencyToken::new("contract-freeze-snapshot")?,
        };
        let (sheet, _) = cdf_dest_parquet::ParquetDestination::dry_plan_commit(&request)?;
        return Ok(vec![DestinationSheetArtifact::new(
            sheet,
            DestinationProtocolCapabilities::default(),
        )?]);
    }
    if uri.starts_with("postgres://") {
        return Ok(vec![
            cdf_dest_postgres::PostgresDestination::new().sheet_artifact()?,
        ]);
    }
    Err(CdfError::contract(
        "destination URI is unsupported for lockfile generation; expected duckdb://, parquet://, or postgres://",
    ))
}

fn contract_snapshot_drift(
    resource_id: &str,
    frozen: &ContractSnapshot,
    current: &ContractSnapshot,
) -> Vec<ContractSnapshotDrift> {
    let mut drift = Vec::new();
    push_snapshot_drift(
        &mut drift,
        resource_id,
        "contract_ref",
        &frozen.contract_ref,
        &current.contract_ref,
    );
    push_snapshot_drift(
        &mut drift,
        resource_id,
        "schema_hash",
        &frozen.schema_hash,
        &current.schema_hash,
    );
    push_snapshot_drift(
        &mut drift,
        resource_id,
        "policy_hash",
        &frozen.policy_hash,
        &current.policy_hash,
    );
    push_snapshot_drift(
        &mut drift,
        resource_id,
        "validation_program_hash",
        &frozen.validation_program_hash,
        &current.validation_program_hash,
    );
    drift
}

fn push_snapshot_drift(
    drift: &mut Vec<ContractSnapshotDrift>,
    resource_id: &str,
    field: &str,
    frozen: &Option<String>,
    current: &Option<String>,
) {
    if frozen != current {
        drift.push(ContractSnapshotDrift {
            resource_id: resource_id.to_owned(),
            field: field.to_owned(),
            frozen: frozen.clone(),
            current: current.clone(),
        });
    }
}
