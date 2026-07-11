use std::{
    fs,
    path::{Path, PathBuf},
    result::Result as StdResult,
    sync::Arc,
};

use cdf_declarative::CompiledResource;
use cdf_kernel::{CdfError, Result as CdfResult, SchemaSource};
use cdf_project::{
    CdfLock, DefaultSecretProvider, EffectiveEnvironment, EnvSecretProvider,
    FileResourceSourceResolver, FileSecretProvider, LOCK_FILE_NAME, LockFileAuthority,
    PROJECT_FILE_NAME, ProjectConfig, ProjectResource, ProjectResourceOrigin, ResourceSourceKind,
    SchemaSnapshotStore, parse_cdf_toml, parse_lock, read_lock_file_authority,
};
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
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DestinationRuntime {
    DuckDb {
        database_path: String,
        sheet: cdf_kernel::DestinationSheet,
        bulk_paths: Vec<String>,
        single_writer_lock: String,
        parquet_replay: cdf_kernel::CapabilitySupport,
        icu_probe: DoctorProbe,
    },
    Postgres {
        sheet: cdf_dest_postgres::PostgresDestinationSheet,
    },
    Unsupported {
        uri: String,
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DoctorProbe {
    Passed,
    Failed { message: String },
    Skipped { reason: String },
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
        Self::load(project_arg, env_arg)
            .and_then(|mut context| {
                if hydrate_locked_snapshots
                    && matches!(command, "plan" | "explain" | "preview" | "run")
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
        let (root, project_file) = project_location(project_arg)?;
        let project_text = fs::read_to_string(&project_file).map_err(|error| {
            CdfError::contract(format!("read {}: {error}", project_file.display()))
        })?;
        let config = parse_cdf_toml(&project_text)?;
        let env_name = env_arg.unwrap_or(&config.project.default_environment);
        let environment = config.effective_environment(env_name)?;
        let resolver = FileResourceSourceResolver::new(&root);
        let entries = cdf_project::compile_project_declarative_resource_entries_with_root(
            &config, &resolver, &root,
        )?;
        let (resources, resource_origins) = entries
            .into_iter()
            .map(|entry| (entry.resource, entry.origin))
            .unzip();
        let (lock, lock_authority) = load_lock(&root)?;

        Ok(Self {
            root,
            config,
            environment,
            resources,
            resource_origins,
            lock,
            lock_authority,
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

    pub fn python_resource_mapping(&self, id: &str) -> Option<&ProjectResource> {
        self.config
            .resources
            .get(id)
            .filter(|mapping| matches!(mapping.source_kind(), ResourceSourceKind::Python { .. }))
    }

    pub fn has_resource(&self, id: &str) -> bool {
        self.resources
            .iter()
            .any(|resource| resource.descriptor().resource_id.as_str() == id)
            || self.python_resource_mapping(id).is_some()
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

    pub fn state_store(&self) -> CdfResult<SqliteCheckpointStore> {
        SqliteCheckpointStore::open(self.state_store_path()?)
    }

    pub fn destination_runtime(&self) -> DestinationRuntime {
        destination_runtime(&self.root, &self.environment.destination)
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
                            matches!(mapping.source_kind(), ResourceSourceKind::Python { .. })
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
    let Some(lock) = lock else {
        return Ok(resources);
    };
    let store = SchemaSnapshotStore::new(root);
    resources
        .into_iter()
        .map(|resource| {
            if !matches!(resource.descriptor().schema_source, SchemaSource::Discover) {
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
            let artifact = store.read(reference)?;
            if artifact.resource_id != resource_id {
                return Err(CdfError::data(format!(
                    "schema snapshot {} belongs to resource `{}` instead of locked resource `{resource_id}`",
                    reference.path, artifact.resource_id
                )));
            }
            Ok(resource.with_schema_source_and_schema(
                SchemaSource::Discovered {
                    snapshot: reference.clone(),
                },
                Arc::new(artifact.schema.to_arrow()?),
            ))
        })
        .collect()
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
                matches!(mapping.source_kind(), ResourceSourceKind::Python { .. })
            })
            .map(|(id, mapping)| {
                format!("`{id}` from {} (Python mapping matched)", mapping.source)
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
    let candidate = match project_arg {
        Some(path) => path.clone(),
        None => std::env::current_dir().map_err(|error| CdfError::internal(error.to_string()))?,
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
    if !path.exists() {
        return Ok((None, None));
    }
    let authority = read_lock_file_authority(&path)?;
    let text = std::str::from_utf8(&authority.bytes).map_err(|error| {
        CdfError::contract(format!("read {} as UTF-8: {error}", path.display()))
    })?;
    Ok((Some(parse_lock(text)?), Some(authority)))
}

fn destination_runtime(root: &Path, uri: &str) -> DestinationRuntime {
    if let Some(path) = uri.strip_prefix("duckdb://") {
        let database_path = absolute_under_root(root, path);
        let destination = match cdf_dest_duckdb::DuckDbDestination::new(&database_path) {
            Ok(destination) => destination,
            Err(error) => {
                return DestinationRuntime::Unsupported {
                    uri: uri.to_owned(),
                    reason: error.to_string(),
                };
            }
        };
        let capabilities = destination.capabilities();
        let icu_probe = if database_path.exists() {
            match destination.probe_icu() {
                Ok(probe) if probe.available => DoctorProbe::Passed,
                Ok(probe) => DoctorProbe::Failed {
                    message: probe
                        .error
                        .unwrap_or_else(|| "DuckDB ICU probe returned unavailable".to_owned()),
                },
                Err(error) => DoctorProbe::Failed {
                    message: error.to_string(),
                },
            }
        } else {
            DoctorProbe::Skipped {
                reason: "DuckDB database does not exist; probe would create it".to_owned(),
            }
        };
        DestinationRuntime::DuckDb {
            database_path: database_path.display().to_string(),
            sheet: capabilities.sheet,
            bulk_paths: capabilities
                .bulk_paths
                .into_iter()
                .map(|path| format!("{path:?}"))
                .collect(),
            single_writer_lock: capabilities.single_writer_lock,
            parquet_replay: capabilities.parquet_replay,
            icu_probe,
        }
    } else if uri.starts_with("postgres://") {
        DestinationRuntime::Postgres {
            sheet: cdf_dest_postgres::PostgresDestination::new()
                .postgres_sheet()
                .clone(),
        }
    } else {
        DestinationRuntime::Unsupported {
            uri: uri.to_owned(),
            reason: "destination URI scheme is not handled by current destination crates"
                .to_owned(),
        }
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

fn absolute_under_root(root: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        root.join(path)
    }
}

impl From<CliError> for CdfError {
    fn from(error: CliError) -> Self {
        CdfError::new(error.kind, error.message)
    }
}
