use crate::*;

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
    if config.resources.is_empty() {
        return Err(CdfError::contract(
            "cdf.toml must declare at least one resource source mapping",
        ));
    }
    Ok(())
}

pub(crate) fn required_env_field(
    env_name: &str,
    field: &str,
    value: Option<String>,
) -> Result<String> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            CdfError::contract(format!(
            "environment `{env_name}` must resolve `{field}` from itself or the default environment"
        ))
        })
}

pub(crate) fn merge_retention(
    base: Option<RetentionPolicy>,
    override_policy: Option<RetentionPolicy>,
) -> Option<RetentionPolicy> {
    match (base, override_policy) {
        (Some(base), Some(override_policy)) => Some(base.overlay(override_policy)),
        (Some(base), None) => Some(base),
        (None, Some(override_policy)) => Some(override_policy),
        (None, None) => None,
    }
}

pub(crate) fn parse_retention_rule(value: &str) -> Result<RetentionRule> {
    let value = value.trim();
    let Some((amount, unit)) = value.split_once(' ') else {
        return parse_duration_spec(value).map(RetentionRule::Duration);
    };
    let amount = amount.parse::<u32>().map_err(|error| {
        CdfError::contract(format!(
            "retention rule `{value}` has invalid run count: {error}"
        ))
    })?;
    match unit.trim() {
        "run" | "runs" => Ok(RetentionRule::Runs(amount)),
        _ => Err(CdfError::contract(format!(
            "retention rule `{value}` must use `runs` or a duration unit"
        ))),
    }
}

pub(crate) fn parse_duration_spec(value: &str) -> Result<DurationSpec> {
    let value = value.trim();
    if value.is_empty() {
        return Err(CdfError::contract("duration cannot be empty"));
    }
    let digit_len = value
        .chars()
        .take_while(|character| character.is_ascii_digit())
        .map(char::len_utf8)
        .sum::<usize>();
    if digit_len == 0 {
        return Err(CdfError::contract(format!(
            "duration `{value}` must start with a number"
        )));
    }
    let amount = value[..digit_len].parse::<u64>().map_err(|error| {
        CdfError::contract(format!("duration `{value}` has invalid number: {error}"))
    })?;
    let unit = &value[digit_len..];
    let multiplier = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => {
            return Err(CdfError::contract(format!(
                "duration `{value}` has unsupported unit `{unit}`"
            )));
        }
    };
    amount
        .checked_mul(multiplier)
        .map(DurationSpec::from_millis)
        .ok_or_else(|| CdfError::contract(format!("duration `{value}` is too large")))
}

pub(crate) fn split_secret_uri(uri: &SecretUri) -> Result<(&str, &str)> {
    split_secret_parts(uri.as_str())
}

pub(crate) fn split_secret_parts(value: &str) -> Result<(&str, &str)> {
    let rest = value
        .strip_prefix("secret://")
        .ok_or_else(|| CdfError::contract("secret reference must use the secret:// scheme"))?;
    let (provider, key) = rest
        .split_once('/')
        .ok_or_else(|| CdfError::contract("secret reference must use secret://provider/key"))?;
    if provider.trim().is_empty() {
        return Err(CdfError::contract("secret provider cannot be empty"));
    }
    Ok((provider, key))
}

pub(crate) fn parse_resolved_declarative_source(
    source: &ResolvedResourceSource,
) -> Result<DeclarativeDocument> {
    match source {
        ResolvedResourceSource::Toml(input) => parse_declarative_toml(input),
        ResolvedResourceSource::Yaml(input) => parse_declarative_yaml(input),
    }
}

pub(crate) fn validate_environment_uri_fields(environment: &EffectiveEnvironment) -> Result<()> {
    for (field, value) in [
        ("state", environment.state.as_str()),
        ("packages", environment.packages.as_str()),
        ("destination", environment.destination.as_str()),
    ] {
        reject_plaintext_uri_credentials(field, value)?;
    }
    Ok(())
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
        match resource.plan() {
            CompiledResourcePlan::Rest(plan) => {
                if let Some(auth) = &plan.auth {
                    match auth {
                        AuthScheme::Bearer { token_uri } => {
                            refs.push(secret_ref_from_uri(token_uri)?);
                        }
                        AuthScheme::Header { value_uri, .. } => {
                            refs.push(secret_ref_from_uri(value_uri)?);
                        }
                    }
                }
            }
            CompiledResourcePlan::Sql(plan) => {
                refs.push(secret_ref_from_uri(&plan.connection)?);
            }
            CompiledResourcePlan::Files(_) => {}
        }
    }
    Ok(refs)
}

pub(crate) fn secret_ref_from_uri(uri: &SecretUri) -> Result<SecretRef> {
    SecretRef::new(uri.as_str().to_owned())
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
        SchemaSource::Discovered { schema_hash } => schema_hash.as_ref().map(ToString::to_string),
        SchemaSource::Contract { schema_hash, .. } => schema_hash.as_ref().map(ToString::to_string),
    }
}

pub(crate) fn semantic_hash(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value).map_err(|error| CdfError::internal(error.to_string()))?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

pub(crate) fn diff_json_values(
    path: &str,
    before: Option<&serde_json::Value>,
    after: Option<&serde_json::Value>,
    diffs: &mut Vec<LockDiff>,
) {
    match (before, after) {
        (Some(serde_json::Value::Object(before)), Some(serde_json::Value::Object(after))) => {
            let keys = before
                .keys()
                .chain(after.keys())
                .cloned()
                .collect::<BTreeSet<_>>();
            for key in keys {
                diff_json_values(
                    &format!("{path}.{key}"),
                    before.get(&key),
                    after.get(&key),
                    diffs,
                );
            }
        }
        (Some(before), Some(after)) if before == after => {}
        (Some(before), Some(after)) => diffs.push(LockDiff {
            kind: LockDiffKind::Changed,
            path: path.to_owned(),
            before: Some(render_diff_value(before)),
            after: Some(render_diff_value(after)),
        }),
        (Some(before), None) => diffs.push(LockDiff {
            kind: LockDiffKind::Removed,
            path: path.to_owned(),
            before: Some(render_diff_value(before)),
            after: None,
        }),
        (None, Some(after)) => diffs.push(LockDiff {
            kind: LockDiffKind::Added,
            path: path.to_owned(),
            before: None,
            after: Some(render_diff_value(after)),
        }),
        (None, None) => {}
    }
}

pub(crate) fn render_diff_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        _ => value.to_string(),
    }
}
