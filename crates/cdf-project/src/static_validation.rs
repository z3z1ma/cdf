use std::{collections::BTreeSet, path::Path};

use cdf_kernel::{CdfError, Result};
use cdf_runtime::SourceRegistry;
use cdf_semantic::SemanticCatalog;
use serde::{Deserialize, Serialize};

use crate::{
    COMPILATION_INDEX_RELATIVE_PATH, LOCK_FILE_NAME, ProjectConfig, ProjectResourceSelection,
    ProjectResourceSelectionResolution,
    internal::validate_environment_uri_fields,
    load_compilation_snapshot,
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

    let authority = load_local_authority(project_root, environment, &mut diagnostics);
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
                "locally present lock, compilation-index, and resource-artifact integrity"
                    .to_owned(),
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
    artifacts: std::collections::BTreeMap<String, crate::CompiledResourceArtifact>,
    lock_present: bool,
    index_present: bool,
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
        if !self.lock_present || !self.index_present {
            return LocalAuthorityStatus::Missing;
        }
        let Some(content_hash) = content_hash else {
            return LocalAuthorityStatus::Stale;
        };
        let current = self.artifacts.get(resource_id).is_some_and(|artifact| {
            artifact.project_name == config.project.name
                && artifact.environment == environment
                && artifact.lock_binding.compiler.normalizer == config.project.normalizer
                && artifact.resource.origin.relative_path == path
                && artifact.resource.origin.authored_content_hash == content_hash
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
    environment: &str,
    diagnostics: &mut Vec<ProjectStaticValidationDiagnostic>,
) -> LocalAuthority {
    let lock_present = project_root.join(LOCK_FILE_NAME).exists();
    let index_present = project_root.join(COMPILATION_INDEX_RELATIVE_PATH).exists();
    match load_compilation_snapshot(project_root, Some(environment)) {
        Ok(snapshot) => {
            let valid = snapshot.authority_diagnostic.is_none();
            if let Some(diagnostic) = snapshot.authority_diagnostic {
                let code = match diagnostic.code.as_str() {
                    "CDF-COMPILE-LOCK" => "CDF-VALIDATE-LOCK".to_owned(),
                    "CDF-COMPILE-INDEX" => "CDF-VALIDATE-INDEX".to_owned(),
                    _ => "CDF-VALIDATE-COMPILATION".to_owned(),
                };
                diagnostics.push(ProjectStaticValidationDiagnostic {
                    severity: StaticValidationSeverity::Error,
                    code,
                    kind: diagnostic.kind,
                    resource_id: None,
                    path: None,
                    message: diagnostic.message,
                });
            }
            LocalAuthority {
                artifacts: snapshot.artifacts,
                lock_present,
                index_present,
                valid,
            }
        }
        Err(error) => {
            diagnostics.push(global_error("CDF-VALIDATE-COMPILATION", error));
            LocalAuthority {
                artifacts: std::collections::BTreeMap::new(),
                lock_present,
                index_present,
                valid: false,
            }
        }
    }
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
