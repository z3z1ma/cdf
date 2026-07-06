use std::{
    fs,
    path::{Path, PathBuf},
};

use firn_declarative::CompiledResource;
use firn_kernel::{FirnError, Result};
use firn_project::{
    DefaultSecretProvider, EffectiveEnvironment, EnvSecretProvider, FileResourceSourceResolver,
    FileSecretProvider, FirnLock, LOCK_FILE_NAME, PROJECT_FILE_NAME, ProjectConfig,
    parse_firn_toml, parse_lock,
};
use firn_state_sqlite::SqliteCheckpointStore;
use serde::Serialize;

use crate::output::CliError;

#[derive(Debug)]
pub struct ProjectContext {
    pub root: PathBuf,
    pub config: ProjectConfig,
    pub environment: EffectiveEnvironment,
    pub resources: Vec<CompiledResource>,
    pub lock: Option<FirnLock>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DestinationRuntime {
    DuckDb {
        database_path: String,
        sheet: firn_kernel::DestinationSheet,
        bulk_paths: Vec<String>,
        single_writer_lock: String,
        parquet_replay: firn_kernel::CapabilitySupport,
        icu_probe: DoctorProbe,
    },
    Postgres {
        sheet: firn_dest_postgres::PostgresDestinationSheet,
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
    pub fn load(project_arg: Option<&PathBuf>, env_arg: Option<&str>) -> Result<Self> {
        let (root, project_file) = project_location(project_arg)?;
        let project_text = fs::read_to_string(&project_file).map_err(|error| {
            FirnError::contract(format!("read {}: {error}", project_file.display()))
        })?;
        let config = parse_firn_toml(&project_text)?;
        let env_name = env_arg.unwrap_or(&config.project.default_environment);
        let environment = config.effective_environment(env_name)?;
        let resolver = FileResourceSourceResolver::new(&root);
        let resources = firn_project::compile_project_declarative_resources(&config, &resolver)?;
        let lock = load_lock(&root)?;

        Ok(Self {
            root,
            config,
            environment,
            resources,
            lock,
        })
    }

    pub fn resource(&self, id: &str) -> Result<&CompiledResource> {
        self.resources
            .iter()
            .find(|resource| resource.descriptor().resource_id.as_str() == id)
            .ok_or_else(|| FirnError::contract(format!("resource `{id}` is not compiled")))
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

    pub fn state_store_path(&self) -> Result<PathBuf> {
        sqlite_uri_path(&self.root, &self.environment.state)
    }

    pub fn state_store(&self) -> Result<SqliteCheckpointStore> {
        SqliteCheckpointStore::open(self.state_store_path()?)
    }

    pub fn destination_runtime(&self) -> DestinationRuntime {
        destination_runtime(&self.root, &self.environment.destination)
    }
}

pub fn require_lock(context: &ProjectContext) -> Result<&FirnLock> {
    context.lock.as_ref().ok_or_else(|| {
        FirnError::contract(format!(
            "{} is not present under {}",
            LOCK_FILE_NAME,
            context.root.display()
        ))
    })
}

pub fn project_location(project_arg: Option<&PathBuf>) -> Result<(PathBuf, PathBuf)> {
    let candidate = match project_arg {
        Some(path) => path.clone(),
        None => std::env::current_dir().map_err(|error| FirnError::internal(error.to_string()))?,
    };
    let path = if candidate.file_name().and_then(|name| name.to_str()) == Some(PROJECT_FILE_NAME) {
        candidate
    } else {
        candidate.join(PROJECT_FILE_NAME)
    };
    let root = path
        .parent()
        .ok_or_else(|| FirnError::internal(format!("{} has no parent", path.display())))?
        .to_path_buf();
    Ok((root, path))
}

fn load_lock(root: &Path) -> Result<Option<FirnLock>> {
    let path = root.join(LOCK_FILE_NAME);
    if !path.exists() {
        return Ok(None);
    }
    fs::read_to_string(&path)
        .map_err(|error| FirnError::contract(format!("read {}: {error}", path.display())))
        .and_then(|text| parse_lock(&text).map(Some))
}

fn destination_runtime(root: &Path, uri: &str) -> DestinationRuntime {
    if let Some(path) = uri.strip_prefix("duckdb://") {
        let database_path = absolute_under_root(root, path);
        let destination = match firn_dest_duckdb::DuckDbDestination::new(&database_path) {
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
            sheet: firn_dest_postgres::PostgresDestination::new()
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

fn sqlite_uri_path(root: &Path, uri: &str) -> Result<PathBuf> {
    uri.strip_prefix("sqlite://")
        .map(|path| absolute_under_root(root, path))
        .ok_or_else(|| {
            FirnError::contract(format!(
                "state URI `{uri}` is not supported by firn-cli; expected sqlite://path"
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

impl From<CliError> for FirnError {
    fn from(error: CliError) -> Self {
        FirnError::new(error.kind, error.message)
    }
}
