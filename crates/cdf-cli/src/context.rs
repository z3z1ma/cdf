use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    result::Result as StdResult,
    sync::Arc,
};

use cdf_declarative::CompiledResource;
use cdf_kernel::{CdfError, Result as CdfResult, TargetName};
use cdf_project::{
    CompilationSnapshot, DefaultSecretProvider, EffectiveEnvironment, EnvSecretProvider,
    FileSecretProvider, PROJECT_FILE_NAME, ProjectConfig, ProjectQueryCompilation,
    compile_query_project_resources, compile_selected_query_project_resources, parse_cdf_toml,
    project_file_transaction_generation, recover_project_file_transaction,
    resolve_project_resource_selection,
};
use cdf_semantic::SemanticCatalog;
use cdf_state_sqlite::SqliteCheckpointStore;
use serde::Serialize;

use crate::{error_catalog, output::CliError, suggestions};

#[derive(Debug)]
pub struct ProjectContext {
    pub root: PathBuf,
    pub(crate) project_bytes: Vec<u8>,
    pub config: ProjectConfig,
    pub environment: EffectiveEnvironment,
    pub resources: Vec<CompiledResource>,
    pub resource_queries: Vec<ProjectQueryCompilation>,
    adhoc_resource_ids: BTreeSet<String>,
    pub semantic_catalog: SemanticCatalog,
}

#[derive(Debug)]
pub struct ProjectCompilationContext {
    pub root: PathBuf,
    pub environment: EffectiveEnvironment,
    pub compilation: CompilationSnapshot,
}

/// Project/environment authority for operational commands that must not inventory or compile
/// authored resources.
#[derive(Debug)]
pub struct ProjectOperationalContext {
    pub root: PathBuf,
    pub config: ProjectConfig,
    pub environment: EffectiveEnvironment,
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
    pub fn load_selected_read_only(
        project_arg: Option<&PathBuf>,
        env_arg: Option<&str>,
        resource_id: &str,
        destinations: &cdf_runtime::DestinationRegistry,
    ) -> StdResult<Self, CliError> {
        Self::load_selected_with_policy(
            project_arg,
            env_arg,
            resource_id,
            ProjectPublicationRecovery::FailClosed,
            destinations,
        )
    }

    pub fn load_selected_for_mutation(
        project_arg: Option<&PathBuf>,
        env_arg: Option<&str>,
        resource_id: &str,
        destinations: &cdf_runtime::DestinationRegistry,
    ) -> StdResult<Self, CliError> {
        Self::load_selected_with_policy(
            project_arg,
            env_arg,
            resource_id,
            ProjectPublicationRecovery::Complete,
            destinations,
        )
    }

    fn load_selected_with_policy(
        project_arg: Option<&PathBuf>,
        env_arg: Option<&str>,
        resource_id: &str,
        recovery: ProjectPublicationRecovery,
        destinations: &cdf_runtime::DestinationRegistry,
    ) -> StdResult<Self, CliError> {
        let (root, project_file) = project_location(project_arg)?;
        for attempt in 0..3 {
            let generation_before = match recovery {
                ProjectPublicationRecovery::FailClosed => {
                    project_file_transaction_generation(&root)?
                }
                ProjectPublicationRecovery::Complete => recover_project_file_transaction(&root)?,
            };
            let loaded = Self::load_selected_observed_project(
                &root,
                &project_file,
                env_arg,
                resource_id,
                destinations,
            );
            let generation_after = match recovery {
                ProjectPublicationRecovery::FailClosed => {
                    project_file_transaction_generation(&root)?
                }
                ProjectPublicationRecovery::Complete => recover_project_file_transaction(&root)?,
            };
            if generation_before == generation_after {
                return loaded.map_err(CliError::from);
            }
            if attempt == 2 {
                return Err(CdfError::contract(
                    "project authority changed repeatedly while loading selected compilation input",
                )
                .into());
            }
        }
        Err(
            CdfError::internal("selected project load retry loop exited without stable authority")
                .into(),
        )
    }

    pub fn load_for_command_with_destination_registry(
        project_arg: Option<&PathBuf>,
        env_arg: Option<&str>,
        destinations: &cdf_runtime::DestinationRegistry,
    ) -> StdResult<Self, CliError> {
        Self::load_with_policy(
            project_arg,
            env_arg,
            ProjectPublicationRecovery::FailClosed,
            destinations,
        )
        .map_err(CliError::from)
    }

    pub fn load_with_destination_registry(
        project_arg: Option<&PathBuf>,
        env_arg: Option<&str>,
        destinations: &cdf_runtime::DestinationRegistry,
    ) -> CdfResult<Self> {
        Self::load_with_policy(
            project_arg,
            env_arg,
            ProjectPublicationRecovery::FailClosed,
            destinations,
        )
    }

    fn load_with_policy(
        project_arg: Option<&PathBuf>,
        env_arg: Option<&str>,
        recovery: ProjectPublicationRecovery,
        destinations: &cdf_runtime::DestinationRegistry,
    ) -> CdfResult<Self> {
        let (root, project_file) = project_location(project_arg)?;
        for attempt in 0..3 {
            let generation_before = match recovery {
                ProjectPublicationRecovery::FailClosed => {
                    project_file_transaction_generation(&root)?
                }
                ProjectPublicationRecovery::Complete => recover_project_file_transaction(&root)?,
            };
            let loaded = Self::load_observed_project(&root, &project_file, env_arg, destinations);
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
        destinations: &cdf_runtime::DestinationRegistry,
    ) -> CdfResult<Self> {
        let project_text = fs::read_to_string(project_file).map_err(|error| {
            project_authority_read_error("read project configuration", project_file, error)
        })?;
        let config = parse_cdf_toml(&project_text)?;
        let project_bytes = project_text.into_bytes();
        let env_name = env_arg.unwrap_or(&config.project.default_environment);
        let environment = config.effective_environment(env_name)?;
        let source_registry = crate::source_registry::builtin_source_registry()?;
        let semantic_catalog = SemanticCatalog::builtins()?;
        let destination = destinations
            .inspect(
                &environment.destination,
                &cdf_runtime::DestinationResolutionContext::for_project_inspection(root)
                    .with_environment_name(&environment.name)
                    .with_destination_policy(&environment.destination_policy),
            )?
            .sheet_artifact
            .sheet;
        let entries = compile_query_project_resources(
            source_registry,
            &config,
            root,
            env_name,
            &destination,
            &semantic_catalog,
            &Default::default(),
        )?;
        let (resources, resource_queries) = entries
            .into_iter()
            .map(|entry| (entry.resource, entry.query))
            .unzip();
        Ok(Self {
            root: root.to_path_buf(),
            project_bytes,
            config,
            environment,
            resources,
            resource_queries,
            adhoc_resource_ids: BTreeSet::new(),
            semantic_catalog,
        })
    }

    fn load_selected_observed_project(
        root: &Path,
        project_file: &Path,
        env_arg: Option<&str>,
        resource_id: &str,
        destinations: &cdf_runtime::DestinationRegistry,
    ) -> CdfResult<Self> {
        let project_text = fs::read_to_string(project_file).map_err(|error| {
            project_authority_read_error("read project configuration", project_file, error)
        })?;
        let config = parse_cdf_toml(&project_text)?;
        let project_bytes = project_text.into_bytes();
        let env_name = env_arg.unwrap_or(&config.project.default_environment);
        let environment = config.effective_environment(env_name)?;
        let selection = resolve_project_resource_selection(root, &[resource_id.to_owned()], &[])
            .map_err(|error| match error {
                cdf_project::ProjectResourceSelectionError::Project(error) => error,
                error => CdfError::contract(error.to_string()),
            })?;
        let source_registry = crate::source_registry::builtin_source_registry()?;
        let semantic_catalog = SemanticCatalog::builtins()?;
        let destination = destinations
            .inspect(
                &environment.destination,
                &cdf_runtime::DestinationResolutionContext::for_project_inspection(root)
                    .with_environment_name(&environment.name)
                    .with_destination_policy(&environment.destination_policy),
            )?
            .sheet_artifact
            .sheet;
        let entries = compile_selected_query_project_resources(
            source_registry,
            &config,
            root,
            env_name,
            &destination,
            &semantic_catalog,
            &selection,
        )?;
        let (resources, resource_queries) = entries
            .into_iter()
            .map(|entry| (entry.resource, entry.query))
            .unzip();
        Ok(Self {
            root: root.to_path_buf(),
            project_bytes,
            config,
            environment,
            resources,
            resource_queries,
            adhoc_resource_ids: BTreeSet::new(),
            semantic_catalog,
        })
    }

    pub fn resource(&self, id: &str) -> StdResult<&CompiledResource, CliError> {
        self.resources
            .iter()
            .find(|resource| resource.descriptor().resource_id.as_str() == id)
            .ok_or_else(|| self.resource_not_compiled_error(id))
    }

    pub fn resource_query(&self, id: &str) -> Option<&ProjectQueryCompilation> {
        self.resources
            .iter()
            .zip(&self.resource_queries)
            .find(|(resource, _)| resource.descriptor().resource_id.as_str() == id)
            .map(|(_, query)| query)
    }

    pub fn resource_target(&self, id: &str) -> StdResult<&TargetName, CliError> {
        self.resource(id)?;
        self.resource_query(id)
            .map(|query| &query.effective.target.value)
            .ok_or_else(|| {
                CdfError::internal("compiled project resource lost its query target authority")
                    .into()
            })
    }

    pub fn register_adhoc_resource(&mut self, id: String) {
        self.adhoc_resource_ids.insert(id);
    }

    pub fn is_adhoc_resource(&self, id: &str) -> bool {
        self.adhoc_resource_ids.contains(id)
    }

    pub fn has_resource(&self, id: &str) -> bool {
        self.resources
            .iter()
            .any(|resource| resource.descriptor().resource_id.as_str() == id)
    }

    pub fn resource_ids(&self) -> Vec<String> {
        let mut ids = self
            .resources
            .iter()
            .map(|resource| resource.descriptor().resource_id.to_string())
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

    pub fn destination_runtime(
        &self,
        registry: &cdf_runtime::DestinationRegistry,
    ) -> DestinationRuntime {
        crate::destination_registry::inspect_destination_runtime(registry, self)
    }

    fn resource_suggestions(&self, id: &str) -> Vec<String> {
        suggestions::nearest(
            id,
            self.resources
                .iter()
                .map(|resource| resource.descriptor().resource_id.to_string()),
        )
    }

    fn resource_not_compiled_error(&self, id: &str) -> CliError {
        CliError::mapped(
            CdfError::contract(resource_not_compiled_message(
                id,
                &self.resources,
                &self.resource_queries,
            )),
            error_catalog::RESOURCE_NOT_COMPILED,
        )
        .with_suggestions(self.resource_suggestions(id))
    }
}

impl ProjectCompilationContext {
    pub fn load(project_arg: Option<&PathBuf>, env_arg: Option<&str>) -> StdResult<Self, CliError> {
        let (root, _) = project_location(project_arg)?;
        let snapshot = cdf_project::load_compilation_snapshot(&root, env_arg)?;
        Ok(Self {
            root,
            environment: snapshot.environment.clone(),
            compilation: snapshot,
        })
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
}

impl ProjectOperationalContext {
    pub fn load(project_arg: Option<&PathBuf>, env_arg: Option<&str>) -> StdResult<Self, CliError> {
        let (root, project_file) = project_location(project_arg)?;
        for attempt in 0..3 {
            let generation_before = project_file_transaction_generation(&root)?;
            let loaded = (|| {
                let project_text = fs::read_to_string(&project_file).map_err(|error| {
                    project_authority_read_error("read project configuration", &project_file, error)
                })?;
                let config = parse_cdf_toml(&project_text)?;
                let environment_name = env_arg.unwrap_or(&config.project.default_environment);
                let environment = config.effective_environment(environment_name)?;
                Ok::<_, CdfError>(Self {
                    root: root.clone(),
                    config,
                    environment,
                })
            })();
            let generation_after = project_file_transaction_generation(&root)?;
            if generation_before == generation_after {
                return loaded.map_err(Into::into);
            }
            if attempt == 2 {
                return Err(CdfError::contract(
                    "project authority changed repeatedly while loading operational input",
                )
                .into());
            }
        }
        Err(CdfError::internal(
            "operational project load retry loop exited without stable authority",
        )
        .into())
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
        let scopes: Arc<dyn cdf_kernel::ScopeLeaseStore> = Arc::new(
            cdf_state_sqlite::SqliteScopeLeaseStore::open_with_path_ownership(
                self.state_store_path()?,
                self.state_store_path_ownership(),
            )?,
        );
        let execution = execution.with_staging_lease_authority(Arc::new(
            cdf_runtime::ScopeStagingLeaseAuthority::new(scopes),
        ))?;
        Ok(execution.with_content_reachability_store(Arc::new(
            cdf_state_sqlite::SqliteContentReachabilityStore::open_with_path_ownership(
                self.state_store_path()?,
                self.state_store_path_ownership(),
            )?,
        )))
    }

    pub fn execution_with_package_schema_authority(
        &self,
        execution: &cdf_runtime::ExecutionServices,
        inputs: &cdf_package_contract::PackageReplayInputs,
    ) -> StdResult<cdf_runtime::ExecutionServices, CliError> {
        let Some(authority) = &inputs.run_schema_authority else {
            return Ok(execution.clone());
        };
        if authority.key.project_id != self.config.project.id
            || authority.key.environment.as_str() != self.environment.name
            || authority.key.resource_id != inputs.state_delta.resource_id
        {
            return Err(CdfError::contract(format!(
                "package schema authority belongs to project `{}` environment `{}` resource `{}`, not selected project `{}` environment `{}` resource `{}`",
                authority.key.project_id,
                authority.key.environment,
                authority.key.resource_id,
                self.config.project.id,
                self.environment.name,
                inputs.state_delta.resource_id,
            ))
            .into());
        }
        crate::schema_authority::bind_package_settlement_services_at(
            self.state_store_path()?,
            self.state_store_path_ownership(),
            authority,
            execution,
        )
    }

    pub fn destination_runtime(
        &self,
        registry: &cdf_runtime::DestinationRegistry,
    ) -> DestinationRuntime {
        crate::destination_registry::inspect_destination_runtime_for_environment(
            registry,
            &self.root,
            &self.environment,
        )
    }

    pub fn duckdb_destination_path(&self) -> Option<PathBuf> {
        self.environment
            .destination
            .strip_prefix("duckdb://")
            .map(|path| absolute_under_root(&self.root, path))
    }
}

fn resource_not_compiled_message(
    id: &str,
    resources: &[CompiledResource],
    queries: &[ProjectQueryCompilation],
) -> String {
    let compiled = resources
        .iter()
        .zip(queries)
        .map(|(resource, query)| {
            format!(
                "`{}` from {} using configured source `{}`",
                resource.descriptor().resource_id,
                query.relative_path,
                query.configured_source.configured_source,
            )
        })
        .collect::<Vec<_>>();
    let compiled = if compiled.is_empty() {
        "none".to_owned()
    } else {
        compiled.join(", ")
    };
    format!(
        "resource `{id}` is not compiled; compiled query-first resources: {compiled}; resource ids derive exactly from cdf/<namespace>/<resource>.cdf.sql"
    )
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

pub(crate) fn sqlite_uri_path(root: &Path, uri: &str) -> CdfResult<PathBuf> {
    uri.strip_prefix("sqlite://")
        .map(|path| absolute_under_root(root, path))
        .ok_or_else(|| {
            CdfError::contract(format!(
                "state URI `{uri}` is not supported by cdf-cli; expected sqlite://path"
            ))
        })
}

pub(crate) fn state_store_path_ownership(uri: &str) -> cdf_state_sqlite::StateStorePathOwnership {
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
}
