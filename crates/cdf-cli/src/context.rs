use std::{
    fs,
    path::{Path, PathBuf},
    result::Result as StdResult,
    sync::Arc,
};

use cdf_declarative::CompiledResource;
use cdf_kernel::{CdfError, Result as CdfResult};
use cdf_project::{
    CdfLock, DefaultSecretProvider, EffectiveEnvironment, EnvSecretProvider,
    FileResourceSourceResolver, FileSecretProvider, LOCK_FILE_NAME, LockFileAuthority,
    PROJECT_FILE_NAME, ProjectConfig, ProjectResource, ProjectResourceOrigin, ResourceSourceKind,
    SchemaSnapshotStore, parse_cdf_toml, parse_lock, project_file_transaction_generation,
    read_lock_file_authority, recover_project_file_transaction,
};
use cdf_semantic::SemanticCatalog;
use cdf_state_sqlite::SqliteCheckpointStore;
use serde::Serialize;

use crate::{error_catalog, output::CliError, suggestions};

#[derive(Debug)]
pub struct ProjectContext {
    pub root: PathBuf,
    pub config: ProjectConfig,
    pub environment: EffectiveEnvironment,
    pub resources: Vec<CompiledResource>,
    pub resource_origins: Vec<ProjectResourceOrigin>,
    pub lock: Option<CdfLock>,
    pub lock_authority: Option<LockFileAuthority>,
    pub semantic_catalog: SemanticCatalog,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct DestinationRuntime {
    pub kind: String,
    pub destination_id: Option<String>,
    pub label: Option<String>,
    pub schemes: Vec<String>,
    pub sheet: Option<cdf_kernel::DestinationSheetArtifact>,
    pub capabilities: Option<cdf_runtime::DestinationRuntimeCapabilities>,
    pub health: Vec<cdf_runtime::DestinationHealthResult>,
    pub error: Option<String>,
}

#[derive(Clone, Copy)]
enum ProjectPublicationRecovery {
    FailClosed,
    Complete,
}

impl ProjectContext {
    pub fn load_for_command(
        command: &str,
        project_arg: Option<&PathBuf>,
        env_arg: Option<&str>,
    ) -> StdResult<Self, CliError> {
        Self::load_for_command_with_locked_snapshots(command, project_arg, env_arg, true)
    }

    pub fn load_for_command_with_locked_snapshots(
        command: &str,
        project_arg: Option<&PathBuf>,
        env_arg: Option<&str>,
        hydrate_locked_snapshots: bool,
    ) -> StdResult<Self, CliError> {
        Self::load_for_command_with_policy(
            command,
            project_arg,
            env_arg,
            hydrate_locked_snapshots,
            ProjectPublicationRecovery::FailClosed,
        )
    }

    pub fn load_for_command_with_recovery(
        command: &str,
        project_arg: Option<&PathBuf>,
        env_arg: Option<&str>,
    ) -> StdResult<Self, CliError> {
        Self::load_for_command_with_policy(
            command,
            project_arg,
            env_arg,
            true,
            ProjectPublicationRecovery::Complete,
        )
    }

    fn load_for_command_with_policy(
        command: &str,
        project_arg: Option<&PathBuf>,
        env_arg: Option<&str>,
        hydrate_locked_snapshots: bool,
        recovery: ProjectPublicationRecovery,
    ) -> StdResult<Self, CliError> {
        Self::load_with_policy(project_arg, env_arg, recovery)
            .and_then(|mut context| {
                if hydrate_locked_snapshots
                    && matches!(
                        command,
                        "plan" | "explain" | "preview" | "run" | "validate --deep"
                    )
                {
                    context.resources = hydrate_locked_schema_snapshots(
                        &context.root,
                        context.resources,
                        context.lock.as_ref(),
                    )?;
                }
                Ok(context)
            })
            .map_err(|error| {
                if error.message.contains("missing merge_key") {
                    return CliError::mapped(
                        CdfError::contract(format!(
                            "cdf {command} cannot compile the selected resource: {}",
                            error.message
                        )),
                        error_catalog::PROJECT_MERGE_KEY,
                    );
                }
                if error.message.contains("resource mapping pattern") {
                    return CliError::usage_with(
                        format!("cdf {command} cannot load project: {}", error.message),
                        error_catalog::PROJECT_RESOURCE_MAPPING,
                    );
                }
                CliError::from(error)
            })
    }

    pub fn load(project_arg: Option<&PathBuf>, env_arg: Option<&str>) -> CdfResult<Self> {
        Self::load_with_policy(project_arg, env_arg, ProjectPublicationRecovery::FailClosed)
    }

    fn load_with_policy(
        project_arg: Option<&PathBuf>,
        env_arg: Option<&str>,
        recovery: ProjectPublicationRecovery,
    ) -> CdfResult<Self> {
        let (root, project_file) = project_location(project_arg)?;
        for attempt in 0..3 {
            let generation_before = match recovery {
                ProjectPublicationRecovery::FailClosed => {
                    project_file_transaction_generation(&root)?
                }
                ProjectPublicationRecovery::Complete => recover_project_file_transaction(&root)?,
            };
            let loaded = Self::load_observed_project(&root, &project_file, env_arg);
            let generation_after = match recovery {
                ProjectPublicationRecovery::FailClosed => {
                    project_file_transaction_generation(&root)?
                }
                ProjectPublicationRecovery::Complete => recover_project_file_transaction(&root)?,
            };
            if generation_before == generation_after {
                return loaded;
            }
            if attempt == 2 {
                return Err(CdfError::contract(format!(
                    "project authority changed repeatedly while loading {}; retry after concurrent cdf add publication completes",
                    project_file.display()
                )));
            }
        }
        Err(CdfError::internal(
            "project load retry loop exited without a stable authority",
        ))
    }

    fn load_observed_project(
        root: &Path,
        project_file: &Path,
        env_arg: Option<&str>,
    ) -> CdfResult<Self> {
        let project_text = fs::read_to_string(project_file).map_err(|error| {
            project_authority_read_error("read project configuration", project_file, error)
        })?;
        let config = parse_cdf_toml(&project_text)?;
        let env_name = env_arg.unwrap_or(&config.project.default_environment);
        let environment = config.effective_environment(env_name)?;
        let resolver = FileResourceSourceResolver::new(root);
        let source_registry = crate::source_registry::builtin_source_registry()?;
        let semantic_catalog = SemanticCatalog::builtins()?;
        let entries = cdf_project::compile_project_declarative_resource_entries_with_root_and_semantic_catalog(
            source_registry,
            &config,
            &resolver,
            root,
            &semantic_catalog,
        )?;
        let (resources, resource_origins) = entries
            .into_iter()
            .map(|entry| (entry.resource, entry.origin))
            .unzip();
        let (lock, lock_authority) = load_lock(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            config,
            environment,
            resources,
            resource_origins,
            lock,
            lock_authority,
            semantic_catalog,
        })
    }

    pub fn resource(&self, id: &str) -> StdResult<&CompiledResource, CliError> {
        self.resources
            .iter()
            .find(|resource| resource.descriptor().resource_id.as_str() == id)
            .ok_or_else(|| self.resource_not_compiled_error(id))
    }

    pub fn resource_origin(&self, id: &str) -> Option<&ProjectResourceOrigin> {
        self.resources
            .iter()
            .zip(&self.resource_origins)
            .find(|(resource, _)| resource.descriptor().resource_id.as_str() == id)
            .map(|(_, origin)| origin)
    }

    pub fn source_reference_mapping(&self, id: &str) -> Option<&ProjectResource> {
        self.config
            .resources
            .get(id)
            .filter(|mapping| matches!(mapping.source_kind(), ResourceSourceKind::Reference { .. }))
    }

    pub fn has_resource(&self, id: &str) -> bool {
        self.resources
            .iter()
            .any(|resource| resource.descriptor().resource_id.as_str() == id)
            || self.source_reference_mapping(id).is_some()
    }

    pub fn resource_ids(&self) -> Vec<String> {
        let mut ids = self
            .resources
            .iter()
            .map(|resource| resource.descriptor().resource_id.to_string())
            .chain(
                self.config
                    .resources
                    .iter()
                    .filter(|(_, mapping)| {
                        matches!(mapping.source_kind(), ResourceSourceKind::Reference { .. })
                    })
                    .map(|(id, _)| id.clone()),
            )
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        ids
    }

    pub fn secret_provider(&self) -> DefaultSecretProvider {
        DefaultSecretProvider::new(
            EnvSecretProvider::process(),
            FileSecretProvider::new(self.root.clone()),
        )
    }

    pub fn package_root(&self) -> PathBuf {
        absolute_under_root(&self.root, &self.environment.packages)
    }

    pub fn state_store_path(&self) -> CdfResult<PathBuf> {
        sqlite_uri_path(&self.root, &self.environment.state)
    }

    pub fn state_store_path_ownership(&self) -> cdf_state_sqlite::StateStorePathOwnership {
        state_store_path_ownership(&self.environment.state)
    }

    pub fn state_store(&self) -> CdfResult<SqliteCheckpointStore> {
        SqliteCheckpointStore::open_with_path_ownership(
            self.state_store_path()?,
            self.state_store_path_ownership(),
        )
    }

    pub fn execution_with_state_authorities(
        &self,
        execution: &cdf_runtime::ExecutionServices,
    ) -> CdfResult<cdf_runtime::ExecutionServices> {
        let scopes: std::sync::Arc<dyn cdf_kernel::ScopeLeaseStore> = std::sync::Arc::new(
            cdf_state_sqlite::SqliteScopeLeaseStore::open_with_path_ownership(
                self.state_store_path()?,
                self.state_store_path_ownership(),
            )?,
        );
        let execution = execution.with_staging_lease_authority(std::sync::Arc::new(
            cdf_runtime::ScopeStagingLeaseAuthority::new(scopes),
        ))?;
        Ok(
            execution.with_content_reachability_store(std::sync::Arc::new(
                cdf_state_sqlite::SqliteContentReachabilityStore::open_with_path_ownership(
                    self.state_store_path()?,
                    self.state_store_path_ownership(),
                )?,
            )),
        )
    }

    pub fn destination_runtime(
        &self,
        registry: &cdf_runtime::DestinationRegistry,
    ) -> DestinationRuntime {
        crate::destination_registry::inspect_destination_runtime(registry, self)
    }

    pub fn duckdb_destination_path(&self) -> Option<PathBuf> {
        self.environment
            .destination
            .strip_prefix("duckdb://")
            .map(|path| absolute_under_root(&self.root, path))
    }

    fn resource_suggestions(&self, id: &str) -> Vec<String> {
        suggestions::nearest(
            id,
            self.resources
                .iter()
                .map(|resource| resource.descriptor().resource_id.to_string())
                .chain(
                    self.config
                        .resources
                        .iter()
                        .filter(|(_, mapping)| {
                            matches!(mapping.source_kind(), ResourceSourceKind::Reference { .. })
                        })
                        .map(|(id, _)| id.clone()),
                ),
        )
    }

    fn resource_not_compiled_error(&self, id: &str) -> CliError {
        CliError::mapped(
            CdfError::contract(resource_not_compiled_message(
                id,
                &self.resources,
                &self.resource_origins,
                &self.config,
            )),
            error_catalog::RESOURCE_NOT_COMPILED,
        )
        .with_suggestions(self.resource_suggestions(id))
    }
}

fn hydrate_locked_schema_snapshots(
    root: &Path,
    resources: Vec<CompiledResource>,
    lock: Option<&CdfLock>,
) -> CdfResult<Vec<CompiledResource>> {
    resources
        .into_iter()
        .map(|resource| hydrate_locked_schema_snapshot(root, resource, lock))
        .collect()
}

pub(crate) fn hydrate_locked_schema_snapshot(
    root: &Path,
    resource: CompiledResource,
    lock: Option<&CdfLock>,
) -> CdfResult<CompiledResource> {
    let Some(lock) = lock else {
        return Ok(resource);
    };
    if resource
        .descriptor()
        .schema_source
        .without_pinned_snapshot()
        .is_none()
    {
        return Ok(resource);
    }
    let resource_id = resource.descriptor().resource_id.as_str();
    let Some(locked) = lock.resources.get(resource_id) else {
        return Ok(resource);
    };
    let Some(reference) = locked.schema_snapshot.as_ref() else {
        return Ok(resource);
    };
    if locked.schema_hash.as_deref() != Some(reference.schema_hash.as_str())
        || locked.descriptor.schema_source.pinned_snapshot() != Some(reference)
    {
        return Err(CdfError::data(format!(
            "{LOCK_FILE_NAME} has inconsistent schema snapshot pointers for resource `{resource_id}`"
        )));
    }
    let artifact = SchemaSnapshotStore::new(root).read(reference)?;
    if artifact.resource_id != resource_id {
        return Err(CdfError::data(format!(
            "schema snapshot {} belongs to resource `{}` instead of locked resource `{resource_id}`",
            reference.path, artifact.resource_id
        )));
    }
    let pinned_source = resource
        .descriptor()
        .schema_source
        .with_pinned_snapshot(reference.clone())
        .ok_or_else(|| {
            CdfError::internal("schema source lost pinning support during lock hydration")
        })?;
    Ok(
        resource
            .with_schema_source_and_schema(pinned_source, Arc::new(artifact.schema.to_arrow()?)),
    )
}

fn resource_not_compiled_message(
    id: &str,
    resources: &[CompiledResource],
    origins: &[ProjectResourceOrigin],
    config: &ProjectConfig,
) -> String {
    let mut compiled = resources
        .iter()
        .zip(origins)
        .map(|(resource, origin)| {
            format!(
                "`{}` from {} (mapping `{}` {})",
                resource.descriptor().resource_id,
                origin
                    .source_file
                    .as_deref()
                    .unwrap_or("<external or unknown source>"),
                origin.mapping_pattern,
                origin.mapping_status
            )
        })
        .collect::<Vec<_>>();
    compiled.extend(
        config
            .resources
            .iter()
            .filter(|(_, mapping)| {
                matches!(mapping.source_kind(), ResourceSourceKind::Reference { .. })
            })
            .map(|(id, mapping)| {
                format!("`{id}` from {} (source reference matched)", mapping.source)
            }),
    );
    let compiled = if compiled.is_empty() {
        "none".to_owned()
    } else {
        compiled.join(", ")
    };
    format!(
        "resource `{id}` is not compiled; compiled resource ids: {compiled}; likely causes: the resource id does not use `<source>.<resource>`, the `[resources]` mapping did not select the source file, the source file failed to parse, or the glob/resource declaration matched nothing"
    )
}

pub fn require_lock(context: &ProjectContext) -> CdfResult<&CdfLock> {
    context.lock.as_ref().ok_or_else(|| {
        CdfError::contract(format!(
            "{} is not present under {}",
            LOCK_FILE_NAME,
            context.root.display()
        ))
    })
}

pub fn project_location(project_arg: Option<&PathBuf>) -> CdfResult<(PathBuf, PathBuf)> {
    project_location_with_current_dir(project_arg, std::env::current_dir)
}

pub(crate) fn project_location_with_current_dir(
    project_arg: Option<&PathBuf>,
    current_dir: impl FnOnce() -> std::io::Result<PathBuf>,
) -> CdfResult<(PathBuf, PathBuf)> {
    let candidate = match project_arg {
        Some(path) => path.clone(),
        None => current_dir().map_err(|error| {
            CdfError::environment(format!(
                "read current directory: {error}; change to an accessible directory or pass an absolute --project path"
            ))
        })?,
    };
    let path = if candidate.file_name().and_then(|name| name.to_str()) == Some(PROJECT_FILE_NAME) {
        candidate
    } else {
        candidate.join(PROJECT_FILE_NAME)
    };
    let root = path
        .parent()
        .ok_or_else(|| CdfError::internal(format!("{} has no parent", path.display())))?
        .to_path_buf();
    Ok((root, path))
}

fn load_lock(root: &Path) -> CdfResult<(Option<CdfLock>, Option<LockFileAuthority>)> {
    let path = root.join(LOCK_FILE_NAME);
    match std::fs::metadata(&path) {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            match std::fs::symlink_metadata(&path) {
                Err(link_error) if link_error.kind() == std::io::ErrorKind::NotFound => {
                    validate_missing_lock_ancestors(&path)?;
                    return Ok((None, None));
                }
                _ => {}
            }
        }
        Err(_) => {}
    }
    let authority = read_lock_file_authority(&path)?;
    let text = std::str::from_utf8(&authority.bytes).map_err(|error| {
        CdfError::contract(format!("read {} as UTF-8: {error}", path.display()))
    })?;
    Ok((Some(parse_lock(text)?), Some(authority)))
}

fn validate_missing_lock_ancestors(path: &Path) -> CdfResult<()> {
    let mut cursor = path.parent();
    while let Some(parent) = cursor {
        if parent.as_os_str().is_empty() {
            return Ok(());
        }
        match fs::metadata(parent) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(CdfError::data(format!(
                    "cdf.lock ancestor {} is not a real directory",
                    parent.display()
                )));
            }
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                match fs::symlink_metadata(parent) {
                    Ok(metadata) if metadata.file_type().is_symlink() => {
                        return Err(CdfError::data(format!(
                            "cdf.lock ancestor {} is a dangling symlink",
                            parent.display()
                        )));
                    }
                    Ok(_) => {
                        return Err(CdfError::data(format!(
                            "cdf.lock ancestor {} changed filesystem shape during inspection",
                            parent.display()
                        )));
                    }
                    Err(link_error) if link_error.kind() == std::io::ErrorKind::NotFound => {
                        cursor = parent.parent();
                    }
                    Err(link_error) => {
                        return Err(project_authority_read_error(
                            "inspect cdf.lock ancestor",
                            parent,
                            link_error,
                        ));
                    }
                }
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::NotADirectory
                    || cdf_kernel::is_filesystem_loop(&error) =>
            {
                return Err(CdfError::data(format!(
                    "cdf.lock ancestor {} has an invalid filesystem shape: {error}",
                    parent.display()
                )));
            }
            Err(error) => {
                return Err(CdfError::environment(format!(
                    "inspect cdf.lock ancestor {}: {error}; check project-path permissions, device availability, and process file limits before retrying",
                    parent.display()
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn project_authority_read_error(
    action: &str,
    path: &Path,
    error: std::io::Error,
) -> CdfError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::NotFound
            | std::io::ErrorKind::NotADirectory
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::InvalidData
    ) || cdf_kernel::is_filesystem_loop(&error)
    {
        CdfError::contract(format!("{action} {}: {error}", path.display()))
    } else {
        CdfError::environment(format!(
            "{action} {}: {error}; check project-path permissions, device availability, memory, and process file limits before retrying",
            path.display()
        ))
    }
}

fn sqlite_uri_path(root: &Path, uri: &str) -> CdfResult<PathBuf> {
    uri.strip_prefix("sqlite://")
        .map(|path| absolute_under_root(root, path))
        .ok_or_else(|| {
            CdfError::contract(format!(
                "state URI `{uri}` is not supported by cdf-cli; expected sqlite://path"
            ))
        })
}

fn state_store_path_ownership(uri: &str) -> cdf_state_sqlite::StateStorePathOwnership {
    if uri == "sqlite://.cdf/state.db" {
        cdf_state_sqlite::StateStorePathOwnership::CdfManaged
    } else {
        cdf_state_sqlite::StateStorePathOwnership::Configured
    }
}

fn absolute_under_root(root: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        root.join(path)
    }
}

#[cfg(test)]
mod tests {
    use cdf_kernel::ErrorKind;

    use super::*;

    #[test]
    fn state_store_ownership_comes_from_the_selected_uri_not_path_components() {
        assert_eq!(
            state_store_path_ownership("sqlite://.cdf/state.db"),
            cdf_state_sqlite::StateStorePathOwnership::CdfManaged
        );
        assert_eq!(
            state_store_path_ownership("sqlite://custom/.cdf/state.db"),
            cdf_state_sqlite::StateStorePathOwnership::Configured
        );
    }

    #[test]
    fn lock_authority_parent_shape_is_not_silently_treated_as_absent() {
        let root = tempfile::tempdir().unwrap();
        let project_root = root.path().join("project");
        std::fs::write(&project_root, b"not a directory").unwrap();

        let error = load_lock(&project_root).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Data);
    }

    #[cfg(unix)]
    #[test]
    fn dangling_lock_symlink_is_not_silently_treated_as_absent() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        symlink(
            root.path().join("missing.lock"),
            root.path().join(LOCK_FILE_NAME),
        )
        .unwrap();

        let error = load_lock(root.path()).unwrap_err();

        assert_eq!(error.kind, ErrorKind::Data);
    }
}
