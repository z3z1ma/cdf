use std::collections::BTreeSet;

use cdf_contract::NORMALIZER_NAMECASE_V1;
use cdf_declarative::CompiledResource;
use cdf_kernel::{CdfError, Result, SchemaSource};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{
    models::{EffectiveEnvironment, ProjectConfig},
    secrets::SecretRef,
};

pub(crate) fn validate_project_shape(config: &ProjectConfig) -> Result<()> {
    if config.project.name.trim().is_empty() {
        return Err(CdfError::contract("project.name cannot be empty"));
    }
    if config.project.default_environment.trim().is_empty() {
        return Err(CdfError::contract(
            "project.default_environment cannot be empty",
        ));
    }
    if config.project.normalizer != NORMALIZER_NAMECASE_V1 {
        return Err(CdfError::contract(format!(
            "unsupported normalizer {:?}; expected {NORMALIZER_NAMECASE_V1:?}",
            config.project.normalizer
        )));
    }
    if config.environments.is_empty() {
        return Err(CdfError::contract(
            "cdf.toml must declare at least one environment",
        ));
    }
    if !config
        .environments
        .contains_key(&config.project.default_environment)
    {
        return Err(CdfError::contract(format!(
            "default environment `{}` is not declared",
            config.project.default_environment
        )));
    }
    if config.sources.is_empty() {
        return Err(CdfError::contract(
            "cdf.toml must declare at least one configured source",
        ));
    }
    for (environment_name, environment) in &config.environments {
        if environment
            .destination_policy
            .adapters
            .contains_key("postgres")
        {
            return Err(CdfError::contract(format!(
                "environment `{environment_name}` declares unsupported destination_policy.postgres; Postgres consumes finalized package winner authority and has no destination policy"
            )));
        }
    }
    Ok(())
}

pub(crate) fn validate_environment_uri_fields(environment: &EffectiveEnvironment) -> Result<()> {
    for (field, value) in [
        ("state", environment.state.as_str()),
        ("packages", environment.packages.as_str()),
        ("destination", environment.destination.as_str()),
    ] {
        reject_plaintext_uri_credentials(field, value)?;
    }
    validate_state_uri_syntax(&environment.state)?;
    validate_destination_uri_syntax(&environment.destination)?;
    Ok(())
}

fn validate_state_uri_syntax(value: &str) -> Result<()> {
    let valid = value.strip_prefix("sqlite://").is_some_and(|path| {
        !path.trim().is_empty() && !path.contains("://") && !path.chars().any(char::is_control)
    });
    if valid {
        Ok(())
    } else {
        Err(CdfError::contract(
            "state URI is invalid; expected sqlite://path",
        ))
    }
}

fn validate_destination_uri_syntax(value: &str) -> Result<()> {
    if value.starts_with("secret://") {
        SecretRef::new(value.to_owned())?;
        return Ok(());
    }
    let scheme = cdf_runtime::destination_uri_scheme(value).map_err(|_| {
        CdfError::contract(
            "destination URI is invalid; expected <scheme>://location or secret://provider/key",
        )
    })?;
    let prefix = format!("{scheme}://");
    let valid = value.strip_prefix(&prefix).is_some_and(|location| {
        !location.trim().is_empty() && !location.chars().any(char::is_control)
    });
    if valid {
        Ok(())
    } else {
        Err(CdfError::contract(
            "destination URI is invalid; expected <scheme>://location or secret://provider/key",
        ))
    }
}

pub(crate) fn reject_plaintext_uri_credentials(field: &str, value: &str) -> Result<()> {
    let Some((scheme, rest)) = value.split_once("://") else {
        return Ok(());
    };
    if scheme == "secret" {
        SecretRef::new(value.to_owned())?;
        return Ok(());
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    let Some((userinfo, _host)) = authority.rsplit_once('@') else {
        return Ok(());
    };
    let Some((_user, password)) = userinfo.split_once(':') else {
        return Ok(());
    };
    if password.starts_with("secret://") {
        SecretRef::new(password.to_owned())?;
        Ok(())
    } else {
        Err(CdfError::contract(format!(
            "{field} URI contains inline credentials; use a secret://provider/key reference"
        )))
    }
}

pub(crate) fn collect_secret_refs_from_environment(
    environment: &EffectiveEnvironment,
) -> Result<Vec<SecretRef>> {
    let mut refs = Vec::new();
    refs.extend(secret_refs_in_text(&environment.state)?);
    refs.extend(secret_refs_in_text(&environment.packages)?);
    refs.extend(secret_refs_in_text(&environment.destination)?);
    Ok(refs)
}

pub(crate) fn collect_secret_refs_from_declarative(
    resources: &[CompiledResource],
) -> Result<Vec<SecretRef>> {
    let mut refs = Vec::new();
    for resource in resources {
        collect_secret_refs_from_json(&resource.source_plan().redacted_options, &mut refs)?;
    }
    Ok(refs)
}

fn collect_secret_refs_from_json(
    value: &serde_json::Value,
    refs: &mut Vec<SecretRef>,
) -> Result<()> {
    match value {
        serde_json::Value::String(value) => refs.extend(secret_refs_in_text(value)?),
        serde_json::Value::Array(values) => {
            for value in values {
                collect_secret_refs_from_json(value, refs)?;
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                collect_secret_refs_from_json(value, refs)?;
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
    Ok(())
}

pub(crate) fn validate_secret_references_in_json(value: &serde_json::Value) -> Result<()> {
    let mut references = Vec::new();
    collect_secret_refs_from_json(value, &mut references)
}

pub(crate) fn secret_refs_in_text(value: &str) -> Result<Vec<SecretRef>> {
    let mut refs = Vec::new();
    let mut remaining = value;
    while let Some(start) = remaining.find("secret://") {
        let candidate = &remaining[start..];
        let end = candidate
            .find(|character: char| {
                character.is_whitespace() || matches!(character, '"' | '\'' | ',' | ';' | ')' | ']')
            })
            .unwrap_or(candidate.len());
        refs.push(SecretRef::new(candidate[..end].to_owned())?);
        remaining = &candidate[end..];
    }
    Ok(refs)
}

pub(crate) fn dedupe_secret_refs(refs: Vec<SecretRef>) -> Vec<SecretRef> {
    let mut seen = BTreeSet::new();
    refs.into_iter()
        .filter(|secret| seen.insert(secret.as_str().to_owned()))
        .collect()
}

pub(crate) fn schema_hash_from_source(schema_source: &SchemaSource) -> Option<String> {
    match schema_source {
        SchemaSource::Declared { schema_hash, .. } => Some(schema_hash.to_string()),
        SchemaSource::Discover => None,
        SchemaSource::Discovered { snapshot } => Some(snapshot.schema_hash.to_string()),
        SchemaSource::Hints {
            snapshot: Some(snapshot),
            ..
        } => Some(snapshot.schema_hash.to_string()),
        SchemaSource::Hints { snapshot: None, .. } => None,
        SchemaSource::Contract { schema_hash, .. } => schema_hash.as_ref().map(ToString::to_string),
    }
}

pub(crate) fn semantic_hash(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value).map_err(|error| CdfError::internal(error.to_string()))?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}
