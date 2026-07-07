use crate::internal::*;
use crate::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectValidationReport {
    pub environment: EffectiveEnvironment,
    pub declarative_resources: usize,
    pub external_resources: usize,
    pub checked_secrets: Vec<SecretCheck>,
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
pub struct LockedDestination {
    pub sheet_hash: String,
    pub sheet: DestinationSheet,
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
    compile_project_declarative_resources_inner(config, resolver, None)
}

pub fn compile_project_declarative_resources_with_root(
    config: &ProjectConfig,
    resolver: &dyn ResourceSourceResolver,
    project_root: impl AsRef<Path>,
) -> Result<Vec<CompiledResource>> {
    compile_project_declarative_resources_inner(config, resolver, Some(project_root.as_ref()))
}

fn compile_project_declarative_resources_inner(
    config: &ProjectConfig,
    resolver: &dyn ResourceSourceResolver,
    project_root: Option<&Path>,
) -> Result<Vec<CompiledResource>> {
    let mut resources = Vec::new();
    for mapping in config.resources.values() {
        let ResourceSourceKind::DeclarativeFile { path } = mapping.source_kind() else {
            continue;
        };
        let document = parse_resolved_declarative_source(&resolver.resolve(&path)?)?;
        match project_root {
            Some(project_root) => {
                resources.extend(compile_document_with_project_root(&document, project_root)?);
            }
            None => resources.extend(compile_document(&document)?),
        }
    }
    Ok(resources)
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
    let mut declarative_resources = 0;
    let mut external_resources = 0;

    for mapping in config.resources.values() {
        match mapping.source_kind() {
            ResourceSourceKind::DeclarativeFile { path } => {
                let document = parse_resolved_declarative_source(&resolver.resolve(&path)?)?;
                let compiled = compile_document(&document)?;
                declarative_resources += compiled.len();
                secret_refs.extend(collect_secret_refs_from_declarative(&compiled)?);
            }
            ResourceSourceKind::Python { .. }
            | ResourceSourceKind::Rust { .. }
            | ResourceSourceKind::External { .. } => external_resources += 1,
        }
    }

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
    validate_project_shape(config)?;
    let mut locked_resources = BTreeMap::new();
    for resource in resources {
        let descriptor = resource.descriptor().clone();
        let resource_id = descriptor.resource_id.to_string();
        let schema_hash = schema_hash_from_source(&descriptor.schema_source);
        let contract = contract_snapshots
            .get(&resource_id)
            .cloned()
            .or_else(|| contract_snapshot_from_descriptor(&descriptor));
        locked_resources.insert(
            resource_id,
            LockedResource {
                descriptor,
                capabilities: resource.capabilities().clone(),
                capability_sheet_hash: semantic_hash(resource.capabilities())?,
                schema_hash,
                contract,
            },
        );
    }

    let mut destinations = BTreeMap::new();
    for sheet in destination_sheets {
        destinations.insert(
            sheet.destination.to_string(),
            LockedDestination {
                sheet_hash: semantic_hash(sheet)?,
                sheet: sheet.clone(),
            },
        );
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

pub fn diff_lockfiles(before: &CdfLock, after: &CdfLock) -> Result<Vec<LockDiff>> {
    let before =
        serde_json::to_value(before).map_err(|error| CdfError::internal(error.to_string()))?;
    let after =
        serde_json::to_value(after).map_err(|error| CdfError::internal(error.to_string()))?;
    let mut diffs = Vec::new();
    diff_json_values("$", Some(&before), Some(&after), &mut diffs);
    Ok(diffs)
}
