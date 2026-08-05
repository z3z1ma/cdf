use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Read,
    path::{Path, PathBuf},
};

use cdf_kernel::{CdfError, ResourceId, Result, TargetName};
use cdf_runtime::{SourceDriverDescriptor, SourceRegistry, artifact_hash};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    ManifestInputContentHash, PROJECT_FILE_NAME, PROJECT_MANIFEST_MAX_BYTES, ProjectConfig,
    ProjectSourceConfig, manifest::PROJECT_MANIFEST_MAX_INPUTS,
};

const CDF_DIRECTORY: &str = "cdf";
const RESOURCE_SUFFIX: &str = ".cdf.sql";
const PROJECT_TOKEN_GRAMMAR: &str = "[a-z][a-z0-9_]{0,127}";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectSourceName(String);

impl ProjectSourceName {
    pub fn new(value: &str, authority_path: &str) -> Result<Self> {
        validate_project_token("source", value, authority_path)?;
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectResourceName(String);

impl ProjectResourceName {
    pub fn new(value: &str, authority_path: &str) -> Result<Self> {
        validate_project_token("resource", value, authority_path)?;
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectResourceNamespace(String);

impl ProjectResourceNamespace {
    pub fn new(value: &str, authority_path: &str) -> Result<Self> {
        validate_project_token("resource namespace", value, authority_path)?;
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectSourceConfigurationHash(String);

impl ProjectSourceConfigurationHash {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectSourceBinding {
    pub name: ProjectSourceName,
    pub source_type: String,
    pub base_options: BTreeMap<String, serde_json::Value>,
    pub overlay_options: BTreeMap<String, serde_json::Value>,
    pub effective_options: BTreeMap<String, serde_json::Value>,
    pub base_hash: ProjectSourceConfigurationHash,
    pub overlay_hash: ProjectSourceConfigurationHash,
    pub effective_hash: ProjectSourceConfigurationHash,
    pub driver: SourceDriverDescriptor,
    pub driver_descriptor_hash: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectResourceInput {
    pub relative_path: String,
    pub content_hash: ManifestInputContentHash,
    pub namespace: ProjectResourceNamespace,
    pub resource_name: ProjectResourceName,
    pub resource_id: ResourceId,
    pub default_target: TargetName,
    pub sql: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectResourcePath {
    pub relative_path: String,
    pub namespace: ProjectResourceNamespace,
    pub resource_name: ProjectResourceName,
    pub resource_id: ResourceId,
    pub default_target: TargetName,
    pub absolute_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProjectResourcePathCatalog {
    pub root_present: bool,
    pub resources: Vec<ProjectResourcePath>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectResourceInventory {
    pub environment: String,
    pub sources: BTreeMap<ProjectSourceName, ProjectSourceBinding>,
    pub resources: Vec<ProjectResourceInput>,
    pub total_authored_bytes: usize,
}

#[derive(Serialize)]
struct SourceConfigurationHashInput<'a> {
    phase: &'static str,
    environment: Option<&'a str>,
    source: &'a str,
    source_type: Option<&'a str>,
    options: &'a BTreeMap<String, serde_json::Value>,
}

/// Builds the bounded, path-derived input authority consumed by query-first project lowering.
///
/// Regular files inside a resource namespace are ignored only when their names contain
/// neither `.cdf` nor `.sql` (case-insensitively). This permits deterministic colocated prose such
/// as `README.md` while treating every SQL-like near match as a blocking authoring error.
pub fn inventory_project_resources(
    project_root: &Path,
    config: &ProjectConfig,
    environment: &str,
    registry: &SourceRegistry,
) -> Result<ProjectResourceInventory> {
    let selected_environment = config.environments.get(environment).ok_or_else(|| {
        CdfError::contract(format!("environment `{environment}` is not declared"))
    })?;
    let configured_sources = validate_source_configuration_shape(config)?;
    let path_catalog = inventory_project_resource_paths(project_root)?;
    if !path_catalog.root_present && !configured_sources.is_empty() {
        return Err(CdfError::contract(format!(
            "{} configures sources but {} is missing; create at least one cdf/<namespace>/<resource>.cdf.sql file that explicitly references each configured source",
            PROJECT_FILE_NAME,
            project_root.join(CDF_DIRECTORY).display()
        )));
    }
    let mut total_authored_bytes = 0usize;
    let mut resources = Vec::with_capacity(path_catalog.resources.len());
    for path in &path_catalog.resources {
        let input = read_project_resource_path_with_limit(
            path,
            PROJECT_MANIFEST_MAX_BYTES.saturating_sub(total_authored_bytes),
        )?;
        total_authored_bytes = total_authored_bytes
            .checked_add(input.sql.len())
            .ok_or_else(|| CdfError::contract("project authored input byte count overflowed"))?;
        resources.push(input);
    }

    let mut sources = BTreeMap::new();
    for (source_name, base) in &configured_sources {
        let overlay = selected_environment
            .sources
            .get(source_name.as_str())
            .cloned()
            .unwrap_or_default();
        let mut effective_options = base.options.clone();
        effective_options.extend(overlay.options.clone());
        add_serialized_size(
            &mut total_authored_bytes,
            &base.options,
            "base project source configuration",
        )?;
        add_serialized_size(
            &mut total_authored_bytes,
            &overlay.options,
            "selected project source overlay",
        )?;
        let driver = registry
            .validate_source_configuration(&base.source_type, &effective_options)
            .map_err(|error| {
                CdfError::new(
                    error.kind,
                    format!(
                        "[sources.{}] effective configuration for environment `{environment}`: {}",
                        source_name.as_str(),
                        error.message
                    ),
                )
            })?;
        let base_hash = configuration_hash(SourceConfigurationHashInput {
            phase: "base",
            environment: None,
            source: source_name.as_str(),
            source_type: Some(&base.source_type),
            options: &base.options,
        })?;
        let overlay_hash = configuration_hash(SourceConfigurationHashInput {
            phase: "overlay",
            environment: Some(environment),
            source: source_name.as_str(),
            source_type: None,
            options: &overlay.options,
        })?;
        let effective_hash = configuration_hash(SourceConfigurationHashInput {
            phase: "effective",
            environment: Some(environment),
            source: source_name.as_str(),
            source_type: Some(&base.source_type),
            options: &effective_options,
        })?;
        let driver_descriptor_hash = artifact_hash(&driver)?;
        sources.insert(
            source_name.clone(),
            ProjectSourceBinding {
                name: source_name.clone(),
                source_type: base.source_type.clone(),
                base_options: base.options.clone(),
                overlay_options: overlay.options,
                effective_options,
                base_hash,
                overlay_hash,
                effective_hash,
                driver,
                driver_descriptor_hash,
            },
        );
    }

    Ok(ProjectResourceInventory {
        environment: environment.to_owned(),
        sources,
        resources,
        total_authored_bytes,
    })
}

/// Inventories only path-derived resource identity. It never opens or parses a resource file.
pub(crate) fn inventory_project_resource_paths(
    project_root: &Path,
) -> Result<ProjectResourcePathCatalog> {
    let canonical_root = canonical_project_root(project_root)?;
    let cdf_path = canonical_root.join(CDF_DIRECTORY);
    let Some(cdf_metadata) = optional_symlink_metadata(&cdf_path)? else {
        return Ok(ProjectResourcePathCatalog {
            root_present: false,
            resources: Vec::new(),
        });
    };
    require_real_directory(&cdf_path, &cdf_metadata, "CDF resource root")?;
    ensure_inside_project_root(&canonical_root, &cdf_path)?;

    let mut namespace_directories = BTreeMap::new();
    for entry in read_directory(&cdf_path, "enumerate CDF resource namespaces")? {
        let entry = entry.map_err(|error| {
            project_input_io_error("enumerate CDF resource namespaces", &cdf_path, error)
        })?;
        let entry_path = entry.path();
        let metadata = fs::symlink_metadata(&entry_path).map_err(|error| {
            project_input_io_error("inspect CDF resource namespace", &entry_path, error)
        })?;
        require_real_directory(&entry_path, &metadata, "CDF resource namespace")?;
        ensure_inside_project_root(&canonical_root, &entry_path)?;
        let name = utf8_file_name(&entry_path, "resource namespace directory")?;
        let namespace = ProjectResourceNamespace::new(&name, &entry_path.display().to_string())?;
        if namespace_directories.len() == PROJECT_MANIFEST_MAX_INPUTS {
            return Err(CdfError::contract(format!(
                "CDF resource namespaces exceed the {PROJECT_MANIFEST_MAX_INPUTS}-input bound"
            )));
        }
        namespace_directories.insert(namespace, entry_path);
    }
    ensure_metadata_stable(&cdf_path, &cdf_metadata, "CDF resource root")?;

    let mut resources = Vec::new();
    for (namespace, namespace_path) in namespace_directories {
        let namespace_before = fs::symlink_metadata(&namespace_path).map_err(|error| {
            project_input_io_error("inspect CDF resource namespace", &namespace_path, error)
        })?;
        let mut namespace_resources = Vec::new();
        for entry in read_directory(&namespace_path, "enumerate CDF namespace resources")? {
            let entry = entry.map_err(|error| {
                project_input_io_error("enumerate CDF namespace resources", &namespace_path, error)
            })?;
            let resource_path = entry.path();
            let metadata = fs::symlink_metadata(&resource_path).map_err(|error| {
                project_input_io_error("inspect CDF resource", &resource_path, error)
            })?;
            if metadata.file_type().is_symlink() || metadata.is_dir() || !metadata.is_file() {
                return Err(CdfError::contract(format!(
                    "{} must contain only regular <resource>.cdf.sql inputs at its top level; {} has an unsupported filesystem shape",
                    namespace_path.display(),
                    resource_path.display()
                )));
            }
            ensure_inside_project_root(&canonical_root, &resource_path)?;
            let file_name = utf8_file_name(&resource_path, "resource file")?;
            let Some(resource_token) = file_name.strip_suffix(RESOURCE_SUFFIX) else {
                if is_resource_near_match(&file_name) {
                    return Err(CdfError::contract(format!(
                        "malformed resource input {}; rename it to an exact <resource>.cdf.sql file whose resource matches {}",
                        resource_path.display(),
                        PROJECT_TOKEN_GRAMMAR
                    )));
                }
                continue;
            };
            let resource_name =
                ProjectResourceName::new(resource_token, &resource_path.display().to_string())?;
            if resources.len() + namespace_resources.len() == PROJECT_MANIFEST_MAX_INPUTS {
                return Err(CdfError::contract(format!(
                    "CDF resources exceed the {PROJECT_MANIFEST_MAX_INPUTS}-input bound"
                )));
            }
            namespace_resources.push((resource_name, resource_path));
        }
        ensure_metadata_stable(&namespace_path, &namespace_before, "CDF resource namespace")?;
        namespace_resources.sort_by(|left, right| left.0.cmp(&right.0));
        if namespace_resources.is_empty() {
            return Err(CdfError::contract(format!(
                "{} contains no valid regular <resource>.cdf.sql file",
                namespace_path.display()
            )));
        }
        for (resource_name, absolute_path) in namespace_resources {
            resources.push(project_resource_path(
                namespace.clone(),
                resource_name,
                absolute_path,
            )?);
        }
        ensure_metadata_stable(&namespace_path, &namespace_before, "CDF resource namespace")?;
    }
    ensure_metadata_stable(&cdf_path, &cdf_metadata, "CDF resource root")?;
    resources.sort_by(|left, right| left.resource_id.cmp(&right.resource_id));
    Ok(ProjectResourcePathCatalog {
        root_present: true,
        resources,
    })
}

/// Resolves one exact canonical id without enumerating sibling namespaces or resources.
pub(crate) fn resolve_exact_project_resource_path(
    project_root: &Path,
    resource_id: &str,
) -> Result<Option<ProjectResourcePath>> {
    let Some((namespace_token, resource_token)) = resource_id.split_once('.') else {
        return Err(CdfError::contract(format!(
            "resource selector {resource_id:?} must be an exact <namespace>.<resource> id or a glob"
        )));
    };
    if resource_token.contains('.') {
        return Err(CdfError::contract(format!(
            "resource selector {resource_id:?} must contain exactly one namespace separator"
        )));
    }
    let namespace = ProjectResourceNamespace::new(namespace_token, "resource selector")?;
    let resource_name = ProjectResourceName::new(resource_token, "resource selector")?;
    let canonical_root = canonical_project_root(project_root)?;
    let cdf_path = canonical_root.join(CDF_DIRECTORY);
    let Some(cdf_metadata) = optional_symlink_metadata(&cdf_path)? else {
        return Ok(None);
    };
    require_real_directory(&cdf_path, &cdf_metadata, "CDF resource root")?;
    let namespace_path = cdf_path.join(namespace.as_str());
    let Some(namespace_metadata) = optional_symlink_metadata(&namespace_path)? else {
        return Ok(None);
    };
    require_real_directory(
        &namespace_path,
        &namespace_metadata,
        "CDF resource namespace",
    )?;
    ensure_inside_project_root(&canonical_root, &namespace_path)?;
    let absolute_path = namespace_path.join(format!("{}{RESOURCE_SUFFIX}", resource_name.as_str()));
    let Some(metadata) = optional_symlink_metadata(&absolute_path)? else {
        return Ok(None);
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CdfError::contract(format!(
            "project resource {} must be a regular non-symlink file",
            absolute_path.display()
        )));
    }
    ensure_inside_project_root(&canonical_root, &absolute_path)?;
    Ok(Some(project_resource_path(
        namespace,
        resource_name,
        absolute_path,
    )?))
}

pub(crate) fn read_project_resource_path(
    path: &ProjectResourcePath,
) -> Result<ProjectResourceInput> {
    read_project_resource_path_with_limit(path, PROJECT_MANIFEST_MAX_BYTES)
}

fn read_project_resource_path_with_limit(
    path: &ProjectResourcePath,
    remaining_bytes: usize,
) -> Result<ProjectResourceInput> {
    let bytes = read_stable_resource_file(&path.absolute_path, remaining_bytes)?;
    let content_hash = ManifestInputContentHash::new(bytes_hash(&bytes))?;
    let sql = String::from_utf8(bytes).map_err(|error| {
        CdfError::data(format!(
            "CDF resource {} is not UTF-8: {error}",
            path.absolute_path.display()
        ))
    })?;
    Ok(ProjectResourceInput {
        relative_path: path.relative_path.clone(),
        content_hash,
        namespace: path.namespace.clone(),
        resource_name: path.resource_name.clone(),
        resource_id: path.resource_id.clone(),
        default_target: path.default_target.clone(),
        sql,
    })
}

fn project_resource_path(
    namespace: ProjectResourceNamespace,
    resource_name: ProjectResourceName,
    absolute_path: PathBuf,
) -> Result<ProjectResourcePath> {
    let resource_id =
        ResourceId::new(format!("{}.{}", namespace.as_str(), resource_name.as_str()))?;
    let relative_path = format!(
        "{}/{}/{}{}",
        CDF_DIRECTORY,
        namespace.as_str(),
        resource_name.as_str(),
        RESOURCE_SUFFIX
    );
    let default_target = TargetName::new(resource_id.as_str())?;
    Ok(ProjectResourcePath {
        relative_path,
        namespace,
        resource_name,
        resource_id,
        default_target,
        absolute_path,
    })
}

fn validate_source_configuration_shape(
    config: &ProjectConfig,
) -> Result<BTreeMap<ProjectSourceName, ProjectSourceConfig>> {
    let mut sources = BTreeMap::new();
    for (name, source) in &config.sources {
        let authority = format!("{PROJECT_FILE_NAME} [sources.{name}]");
        let source_name = ProjectSourceName::new(name, &authority)?;
        if source.source_type.is_empty() {
            return Err(CdfError::contract(format!(
                "{authority} requires one non-empty `type`"
            )));
        }
        sources.insert(source_name, source.clone());
    }
    for (environment, environment_config) in &config.environments {
        for (name, overlay) in &environment_config.sources {
            let authority =
                format!("{PROJECT_FILE_NAME} [environments.{environment}.sources.{name}]");
            let source_name = ProjectSourceName::new(name, &authority)?;
            if overlay.source_type.is_some() {
                return Err(CdfError::contract(format!(
                    "{authority} may not override immutable source `type`; remove `type` from the environment overlay"
                )));
            }
            if !sources.contains_key(&source_name) {
                return Err(CdfError::contract(format!(
                    "{authority} may not add a source; declare [sources.{name}] in the base project and reference it explicitly from cdf/<namespace>/<resource>.cdf.sql"
                )));
            }
        }
    }
    Ok(sources)
}

fn validate_project_token(kind: &str, value: &str, authority_path: &str) -> Result<()> {
    let bytes = value.as_bytes();
    let valid = (1..=128).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_');
    if valid {
        return Ok(());
    }
    Err(CdfError::contract(format!(
        "invalid project {kind} token `{value}` at {authority_path}; rename it to match {PROJECT_TOKEN_GRAMMAR} exactly"
    )))
}

fn canonical_project_root(project_root: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(project_root)
        .map_err(|error| project_input_io_error("inspect project root", project_root, error))?;
    if !metadata.is_dir() {
        return Err(CdfError::contract(format!(
            "project root {} is not a directory",
            project_root.display()
        )));
    }
    fs::canonicalize(project_root)
        .map_err(|error| project_input_io_error("canonicalize project root", project_root, error))
}

fn optional_symlink_metadata(path: &Path) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(project_input_io_error("inspect project input", path, error)),
    }
}

fn require_real_directory(path: &Path, metadata: &fs::Metadata, label: &str) -> Result<()> {
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        return Ok(());
    }
    Err(CdfError::contract(format!(
        "{label} {} must be a real directory and may not be a symlink",
        path.display()
    )))
}

fn ensure_inside_project_root(project_root: &Path, path: &Path) -> Result<()> {
    let canonical = fs::canonicalize(path)
        .map_err(|error| project_input_io_error("canonicalize project input", path, error))?;
    if canonical.starts_with(project_root) {
        return Ok(());
    }
    Err(CdfError::contract(format!(
        "project input {} escapes project root {}",
        path.display(),
        project_root.display()
    )))
}

fn read_directory(path: &Path, action: &str) -> Result<fs::ReadDir> {
    fs::read_dir(path).map_err(|error| project_input_io_error(action, path, error))
}

fn utf8_file_name(path: &Path, label: &str) -> Result<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .ok_or_else(|| {
            CdfError::contract(format!(
                "{label} path {path:?} is not UTF-8 and cannot be project authority"
            ))
        })
}

fn is_resource_near_match(file_name: &str) -> bool {
    let lower = file_name.to_ascii_lowercase();
    lower.contains(".cdf") || lower.contains(".sql")
}

fn read_stable_resource_file(path: &Path, remaining_bytes: usize) -> Result<Vec<u8>> {
    if remaining_bytes == 0 {
        return Err(CdfError::contract(format!(
            "project authored inputs exceed the {PROJECT_MANIFEST_MAX_BYTES}-byte bound before reading {}",
            path.display()
        )));
    }
    let path_before = fs::symlink_metadata(path)
        .map_err(|error| project_input_io_error("inspect project source resource", path, error))?;
    if !path_before.is_file() || path_before.file_type().is_symlink() {
        return Err(CdfError::data(format!(
            "project source resource {} changed to a non-regular file during compilation",
            path.display()
        )));
    }
    if u64::try_from(remaining_bytes).is_ok_and(|remaining| path_before.len() > remaining) {
        return Err(CdfError::contract(format!(
            "project authored inputs exceed the {PROJECT_MANIFEST_MAX_BYTES}-byte bound at {}",
            path.display()
        )));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    configure_no_follow(&mut options);
    let mut file = options
        .open(path)
        .map_err(|error| project_input_io_error("open project source resource", path, error))?;
    let opened_before = file.metadata().map_err(|error| {
        project_input_io_error("inspect opened project source resource", path, error)
    })?;
    if !same_file_identity(&path_before, &opened_before) {
        return Err(CdfError::data(format!(
            "project source resource {} changed before it could be read stably",
            path.display()
        )));
    }
    let read_bound = u64::try_from(remaining_bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(1);
    let mut bytes = Vec::new();
    file.by_ref()
        .take(read_bound)
        .read_to_end(&mut bytes)
        .map_err(|error| project_input_io_error("read project source resource", path, error))?;
    if bytes.len() > remaining_bytes {
        return Err(CdfError::contract(format!(
            "project authored inputs exceed the {PROJECT_MANIFEST_MAX_BYTES}-byte bound at {}",
            path.display()
        )));
    }
    let opened_after = file.metadata().map_err(|error| {
        project_input_io_error("reinspect opened project source resource", path, error)
    })?;
    let path_after = fs::symlink_metadata(path).map_err(|error| {
        project_input_io_error("reinspect project source resource", path, error)
    })?;
    if !same_file_identity(&opened_before, &opened_after)
        || !same_file_identity(&opened_after, &path_after)
        || opened_after.len() != u64::try_from(bytes.len()).unwrap_or(u64::MAX)
    {
        return Err(CdfError::data(format!(
            "project source resource {} changed while it was read; retry compilation",
            path.display()
        )));
    }
    Ok(bytes)
}

fn ensure_metadata_stable(path: &Path, before: &fs::Metadata, label: &str) -> Result<()> {
    let after = fs::symlink_metadata(path)
        .map_err(|error| project_input_io_error("reinspect project input", path, error))?;
    if same_file_identity(before, &after) {
        return Ok(());
    }
    Err(CdfError::data(format!(
        "{label} {} changed during enumeration; retry compilation",
        path.display()
    )))
}

#[cfg(unix)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.file_type() == right.file_type()
        && left.len() == right.len()
        && left.mtime() == right.mtime()
        && left.mtime_nsec() == right.mtime_nsec()
}

#[cfg(not(unix))]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.file_type() == right.file_type()
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
}

#[cfg(unix)]
fn configure_no_follow(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_NOFOLLOW);
}

#[cfg(not(unix))]
fn configure_no_follow(_options: &mut OpenOptions) {}

fn add_serialized_size<T: Serialize>(total: &mut usize, value: &T, label: &str) -> Result<()> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| CdfError::internal(format!("serialize {label}: {error}")))?;
    *total = total
        .checked_add(bytes.len())
        .ok_or_else(|| CdfError::contract("project authored input byte count overflowed"))?;
    if *total > PROJECT_MANIFEST_MAX_BYTES {
        return Err(CdfError::contract(format!(
            "project authored inputs exceed the {PROJECT_MANIFEST_MAX_BYTES}-byte bound while recording {label}"
        )));
    }
    Ok(())
}

fn configuration_hash<T: Serialize>(value: T) -> Result<ProjectSourceConfigurationHash> {
    Ok(ProjectSourceConfigurationHash(artifact_hash(&value)?))
}

fn bytes_hash(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn project_input_io_error(action: &str, path: &Path, error: std::io::Error) -> CdfError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::InvalidData
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::data(format!("{action} {}: {error}", path.display()))
    } else {
        CdfError::environment(format!("{action} {}: {error}", path.display()))
    }
}
