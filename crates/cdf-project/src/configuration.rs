use cdf_declarative::CompiledResource;
use cdf_http::SecretProvider;
use cdf_kernel::{CdfError, Result};
use cdf_runtime::SourceRegistry;
use serde::{Deserialize, Serialize};

use crate::{
    internal::{
        collect_secret_refs_from_declarative, collect_secret_refs_from_environment,
        dedupe_secret_refs, validate_environment_uri_fields, validate_project_shape,
    },
    models::{EffectiveEnvironment, ProjectConfig},
    secrets::SecretRef,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectValidationReport {
    pub environment: EffectiveEnvironment,
    pub resources: usize,
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

pub fn parse_cdf_toml(input: &str) -> Result<ProjectConfig> {
    let config = toml::from_str::<ProjectConfig>(input)
        .map_err(|error| CdfError::contract(error.to_string()))?;
    validate_project_shape(&config)?;
    Ok(config)
}

pub fn validate_project(
    registry: &SourceRegistry,
    config: &ProjectConfig,
    env_name: Option<&str>,
    resources: &[CompiledResource],
    provider: &dyn SecretProvider,
) -> Result<ProjectValidationReport> {
    validate_project_shape(config)?;
    registry.validate_project_options(&config.driver_options)?;
    let env_name = env_name.unwrap_or(&config.project.default_environment);
    let environment = config.effective_environment(env_name)?;
    validate_environment_uri_fields(&environment)?;

    let mut secret_refs = collect_secret_refs_from_environment(&environment)?;
    secret_refs.extend(collect_secret_refs_from_declarative(resources)?);

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
        resources: resources.len(),
        checked_secrets,
    })
}
