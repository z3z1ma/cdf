use std::{collections::BTreeSet, fs, path::Path};

use cdf_kernel::{CdfError, Result};
use cdf_runtime::SourceRegistry;
use cdf_semantic::SemanticCatalog;
use serde::{Deserialize, Serialize};
use sha2::Digest;

use crate::{
    CdfLock, LOCK_FILE_NAME, PROJECT_MANIFEST_RELATIVE_PATH, ProjectConfig, ProjectManifest,
    ProjectResourceSelection, ProjectResourceSelectionResolution,
    internal::validate_environment_uri_fields,
    parse_lock, parse_project_manifest,
    project_inputs::read_project_resource_path,
    query_compiler::{validate_static_configured_source, validate_static_query_project_resource},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StaticValidationSeverity {
    Warning,
    Error,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LocalAuthorityStatus {
    Current,
    Stale,
    Missing,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStaticValidationDiagnostic {
    pub severity: StaticValidationSeverity,
    pub code: String,
    pub kind: String,
    pub resource_id: Option<String>,
    pub path: Option<String>,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStaticValidationResource {
    pub resource_id: String,
    pub path: String,
    pub configured_source: Option<String>,
    pub valid: bool,
    pub authority: LocalAuthorityStatus,
    pub diagnostics: Vec<ProjectStaticValidationDiagnostic>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStaticValidationCounts {
    pub environments: usize,
    pub configured_sources: usize,
    pub authored_resources: usize,
    pub selected_resources: usize,
    pub valid_resources: usize,
    pub warnings: usize,
    pub errors: usize,
    pub authority_current: usize,
    pub authority_stale: usize,
    pub authority_missing: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStaticValidationEffects {
    pub writes: String,
    pub checked: Vec<String>,
    pub skipped: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectStaticValidationReport {
    pub project: String,
    pub environment: String,
    pub selection: ProjectResourceSelection,
    pub counts: ProjectStaticValidationCounts,
    pub resources: Vec<ProjectStaticValidationResource>,
    pub diagnostics: Vec<ProjectStaticValidationDiagnostic>,
    pub effects: ProjectStaticValidationEffects,
}

pub fn validate_project_static(
    registry: &SourceRegistry,
    project_root: &Path,
    config: &ProjectConfig,
    environment: &str,
    selection: &ProjectResourceSelectionResolution,
    semantic_catalog: &SemanticCatalog,
) -> Result<ProjectStaticValidationReport> {
    let effective_environment = config.effective_environment(environment)?;
    let mut diagnostics = Vec::new();
    if let Err(error) = registry.validate_project_options(&config.driver_options) {
        diagnostics.push(global_error("CDF-VALIDATE-DRIVER-OPTIONS", error));
    }
    if let Err(error) = validate_environment_uri_fields(&effective_environment) {
        diagnostics.push(global_error("CDF-VALIDATE-ENVIRONMENT", error));
    }

    let validate_complete_source_surface = selection.complete_resource_surface;
    if validate_complete_source_surface {
        for source_name in config.sources.keys() {
            if let Err(error) =
                validate_static_configured_source(registry, config, environment, source_name)
            {
                diagnostics.push(ProjectStaticValidationDiagnostic {
                    severity: StaticValidationSeverity::Error,
                    code: "CDF-VALIDATE-SOURCE".to_owned(),
                    kind: error_kind(&error),
                    resource_id: None,
                    path: Some(format!("cdf.toml [sources.{source_name}]")),
                    message: error.message,
                });
            }
        }
        if let Some(environment_config) = config.environments.get(environment) {
            for source_name in environment_config.sources.keys() {
                if !config.sources.contains_key(source_name) {
                    diagnostics.push(ProjectStaticValidationDiagnostic {
                        severity: StaticValidationSeverity::Error,
                        code: "CDF-VALIDATE-SOURCE-OVERLAY".to_owned(),
                        kind: "contract".to_owned(),
                        resource_id: None,
                        path: Some(format!(
                            "cdf.toml [environments.{environment}.sources.{source_name}]"
                        )),
                        message: format!(
                            "environment source overlay {source_name:?} has no base [sources.{source_name}] declaration"
                        ),
                    });
                }
            }
        }
    }

    let authority = load_local_authority(project_root, &mut diagnostics);
    let mut referenced_sources = BTreeSet::new();
    let mut resources = Vec::with_capacity(selection.resources.len());
    for path in &selection.resources {
        let mut resource_diagnostics = Vec::new();
        let mut configured_source = None;
        let input = match read_project_resource_path(path) {
            Ok(input) => Some(input),
            Err(error) => {
                resource_diagnostics.push(resource_error(
                    "CDF-VALIDATE-RESOURCE-INPUT",
                    path.resource_id.as_str(),
                    &path.relative_path,
                    error,
                ));
                None
            }
        };
        if let Some(input) = input.as_ref() {
            match validate_static_query_project_resource(
                registry,
                config,
                environment,
                input,
                semantic_catalog,
            ) {
                Ok(validated) => {
                    referenced_sources.insert(validated.configured_source.clone());
                    configured_source = Some(validated.configured_source);
                }
                Err(error) => resource_diagnostics.push(resource_error(
                    "CDF-VALIDATE-RESOURCE",
                    input.resource_id.as_str(),
                    &input.relative_path,
                    error,
                )),
            }
        }
        let authority_status = authority.status_for(
            path.resource_id.as_str(),
            &path.relative_path,
            input.as_ref().map(|input| input.content_hash.as_str()),
            config,
            environment,
        );
        if authority_status == LocalAuthorityStatus::Stale && authority.valid {
            resource_diagnostics.push(ProjectStaticValidationDiagnostic {
                severity: StaticValidationSeverity::Warning,
                code: "CDF-VALIDATE-AUTHORITY-STALE".to_owned(),
                kind: "contract".to_owned(),
                resource_id: Some(path.resource_id.to_string()),
                path: Some(path.relative_path.clone()),
                message:
                    "local compiled authority is stale; plan or run will prepare this resource"
                        .to_owned(),
            });
        }
        let valid = resource_diagnostics
            .iter()
            .all(|diagnostic| diagnostic.severity != StaticValidationSeverity::Error);
        resources.push(ProjectStaticValidationResource {
            resource_id: path.resource_id.to_string(),
            path: path.relative_path.clone(),
            configured_source,
            valid,
            authority: authority_status,
            diagnostics: resource_diagnostics,
        });
    }

    if validate_complete_source_surface {
        for source_name in config.sources.keys() {
            if !referenced_sources.contains(source_name) {
                diagnostics.push(ProjectStaticValidationDiagnostic {
                    severity: StaticValidationSeverity::Error,
                    code: "CDF-SOURCE-UNREFERENCED".to_owned(),
                    kind: "contract".to_owned(),
                    resource_id: None,
                    path: Some(format!("cdf.toml [sources.{source_name}]")),
                    message: format!(
                        "configured source {source_name:?} is not referenced by any valid cdf/<namespace>/<resource>.cdf.sql input"
                    ),
                });
            }
        }
    }

    diagnostics.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then(left.code.cmp(&right.code))
            .then(left.message.cmp(&right.message))
    });
    let mut counts = ProjectStaticValidationCounts {
        environments: config.environments.len(),
        configured_sources: config.sources.len(),
        authored_resources: selection.resources.len(),
        selected_resources: selection.resources.len(),
        valid_resources: resources.iter().filter(|resource| resource.valid).count(),
        ..ProjectStaticValidationCounts::default()
    };
    for diagnostic in diagnostics
        .iter()
        .chain(resources.iter().flat_map(|resource| &resource.diagnostics))
    {
        match diagnostic.severity {
            StaticValidationSeverity::Warning => counts.warnings += 1,
            StaticValidationSeverity::Error => counts.errors += 1,
        }
    }
    for resource in &resources {
        match resource.authority {
            LocalAuthorityStatus::Current => counts.authority_current += 1,
            LocalAuthorityStatus::Stale => counts.authority_stale += 1,
            LocalAuthorityStatus::Missing => counts.authority_missing += 1,
        }
    }
    Ok(ProjectStaticValidationReport {
        project: config.project.name.clone(),
        environment: environment.to_owned(),
        selection: selection.selection.clone(),
        counts,
        resources,
        diagnostics,
        effects: ProjectStaticValidationEffects {
            writes: "none".to_owned(),
            checked: vec![
                "project and environment syntax".to_owned(),
                "resource path identity and bounded UTF-8 input".to_owned(),
                "resource SQL and configured-source references".to_owned(),
                "closed source and resource option schemas".to_owned(),
                "secret-reference syntax".to_owned(),
                "locally present lock and manifest integrity".to_owned(),
            ],
            skipped: vec![
                "secret resolution and environment-value lookup".to_owned(),
                "source enumeration, discovery, health, and network access".to_owned(),
                "destination and state service access".to_owned(),
            ],
        },
    })
}

struct LocalAuthority {
    lock: Option<CdfLock>,
    lock_bytes_hash: Option<String>,
    manifest: Option<ProjectManifest>,
    lock_present: bool,
    manifest_present: bool,
    valid: bool,
}

impl LocalAuthority {
    fn status_for(
        &self,
        resource_id: &str,
        path: &str,
        content_hash: Option<&str>,
        config: &ProjectConfig,
        environment: &str,
    ) -> LocalAuthorityStatus {
        let (Some(lock), Some(lock_bytes_hash), Some(manifest), Some(content_hash)) = (
            self.lock.as_ref(),
            self.lock_bytes_hash.as_deref(),
            self.manifest.as_ref(),
            content_hash,
        ) else {
            return if !self.lock_present || !self.manifest_present {
                LocalAuthorityStatus::Missing
            } else {
                LocalAuthorityStatus::Stale
            };
        };
        let current = lock.project.name == config.project.name
            && lock.project.default_environment == config.project.default_environment
            && lock.normalizer == config.project.normalizer
            && lock.resources.contains_key(resource_id)
            && manifest.header.project_name == config.project.name
            && manifest.header.environment == environment
            && manifest.header.normalizer == config.project.normalizer
            && manifest.header.lock_content_hash.as_str() == lock_bytes_hash
            && manifest.resources.iter().any(|resource| {
                resource.resource_id == resource_id
                    && resource.origin.relative_path == path
                    && resource.origin.authored_content_hash == content_hash
            });
        if current {
            LocalAuthorityStatus::Current
        } else {
            LocalAuthorityStatus::Stale
        }
    }
}

fn load_local_authority(
    project_root: &Path,
    diagnostics: &mut Vec<ProjectStaticValidationDiagnostic>,
) -> LocalAuthority {
    let lock_bytes = optional_artifact(&project_root.join(LOCK_FILE_NAME), "project lockfile");
    let manifest_bytes = optional_artifact(
        &project_root.join(PROJECT_MANIFEST_RELATIVE_PATH),
        "project manifest",
    );
    let lock_present = !matches!(&lock_bytes, Ok(None));
    let manifest_present = !matches!(&manifest_bytes, Ok(None));
    let mut valid = true;
    let (lock, lock_bytes_hash) = match lock_bytes {
        Ok(Some(bytes)) => {
            let hash = format!("sha256:{}", hex::encode(sha2::Sha256::digest(&bytes)));
            match std::str::from_utf8(&bytes)
                .map_err(|error| CdfError::data(format!("cdf.lock is not UTF-8: {error}")))
                .and_then(parse_lock)
            {
                Ok(lock) => (Some(lock), Some(hash)),
                Err(error) => {
                    valid = false;
                    diagnostics.push(global_error("CDF-VALIDATE-LOCK", error));
                    (None, Some(hash))
                }
            }
        }
        Ok(None) => (None, None),
        Err(error) => {
            valid = false;
            diagnostics.push(global_error("CDF-VALIDATE-LOCK", error));
            (None, None)
        }
    };
    let manifest = match manifest_bytes {
        Ok(Some(bytes)) => match parse_project_manifest(&bytes) {
            Ok(manifest) => Some(manifest),
            Err(error) => {
                valid = false;
                diagnostics.push(global_error("CDF-VALIDATE-MANIFEST", error));
                None
            }
        },
        Ok(None) => None,
        Err(error) => {
            valid = false;
            diagnostics.push(global_error("CDF-VALIDATE-MANIFEST", error));
            None
        }
    };
    LocalAuthority {
        lock,
        lock_bytes_hash,
        manifest,
        lock_present,
        manifest_present,
        valid,
    }
}

fn optional_artifact(path: &Path, label: &str) -> Result<Option<Vec<u8>>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(CdfError::environment(format!(
                "inspect {label} {}: {error}",
                path.display()
            )));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CdfError::data(format!(
            "{label} {} must be a regular non-symlink file",
            path.display()
        )));
    }
    fs::read(path)
        .map(Some)
        .map_err(|error| CdfError::environment(format!("read {label} {}: {error}", path.display())))
}

fn global_error(code: &str, error: CdfError) -> ProjectStaticValidationDiagnostic {
    ProjectStaticValidationDiagnostic {
        severity: StaticValidationSeverity::Error,
        code: code.to_owned(),
        kind: error_kind(&error),
        resource_id: None,
        path: None,
        message: error.message,
    }
}

fn resource_error(
    code: &str,
    resource_id: &str,
    path: &str,
    error: CdfError,
) -> ProjectStaticValidationDiagnostic {
    ProjectStaticValidationDiagnostic {
        severity: StaticValidationSeverity::Error,
        code: code.to_owned(),
        kind: error_kind(&error),
        resource_id: Some(resource_id.to_owned()),
        path: Some(path.to_owned()),
        message: error.message,
    }
}

fn error_kind(error: &CdfError) -> String {
    format!("{:?}", error.kind).to_ascii_lowercase()
}
