use crate::internal::*;
use crate::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectConfig {
    pub project: ProjectMetadata,
    #[serde(default)]
    pub environments: BTreeMap<String, EnvironmentConfig>,
    #[serde(default)]
    pub python: PythonConfig,
    #[serde(default)]
    pub defaults: DefaultsConfig,
    #[serde(default)]
    pub resources: BTreeMap<String, ProjectResource>,
}

impl ProjectConfig {
    pub fn effective_environment(&self, name: &str) -> Result<EffectiveEnvironment> {
        let default_name = self.project.default_environment.as_str();
        let default = self.environments.get(default_name).ok_or_else(|| {
            CdfError::contract(format!(
                "default environment `{default_name}` is not declared"
            ))
        })?;
        let requested = self
            .environments
            .get(name)
            .ok_or_else(|| CdfError::contract(format!("environment `{name}` is not declared")))?;

        let merged = if name == default_name {
            default.clone()
        } else {
            default.overlay(requested)
        };

        Ok(EffectiveEnvironment {
            name: name.to_owned(),
            state: required_env_field(name, "state", merged.state)?,
            packages: required_env_field(name, "packages", merged.packages)?,
            destination: required_env_field(name, "destination", merged.destination)?,
            destination_policy: merged.destination_policy,
            retention: merged.retention,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectMetadata {
    pub name: String,
    pub default_environment: String,
    pub normalizer: String,
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentConfig {
    pub state: Option<String>,
    pub packages: Option<String>,
    pub destination: Option<String>,
    #[serde(default)]
    pub destination_policy: DestinationPolicy,
    pub retention: Option<RetentionPolicy>,
}

impl EnvironmentConfig {
    fn overlay(&self, override_config: &Self) -> Self {
        Self {
            state: override_config.state.clone().or_else(|| self.state.clone()),
            packages: override_config
                .packages
                .clone()
                .or_else(|| self.packages.clone()),
            destination: override_config
                .destination
                .clone()
                .or_else(|| self.destination.clone()),
            destination_policy: self
                .destination_policy
                .overlay(&override_config.destination_policy),
            retention: merge_retention(self.retention.clone(), override_config.retention.clone()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveEnvironment {
    pub name: String,
    pub state: String,
    pub packages: String,
    pub destination: String,
    pub destination_policy: DestinationPolicy,
    pub retention: Option<RetentionPolicy>,
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationPolicy {
    #[serde(default)]
    pub postgres: Option<PostgresDestinationPolicy>,
}

impl DestinationPolicy {
    fn overlay(&self, override_policy: &Self) -> Self {
        Self {
            postgres: override_policy
                .postgres
                .clone()
                .or_else(|| self.postgres.clone()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresDestinationPolicy {
    pub merge_dedup: PostgresMergeDedupPolicy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresMergeDedupPolicy {
    Fail,
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PythonConfig {
    pub interpreter: Option<String>,
    pub require_free_threaded: Option<bool>,
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultsConfig {
    pub contract: Option<String>,
    pub trust: Option<TrustPreset>,
    pub write_disposition: Option<WriteDispositionPreset>,
    pub retention: Option<RetentionPolicy>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectResource {
    pub source: String,
    pub trust: Option<TrustPreset>,
    pub freshness: Option<ProjectFreshness>,
}

impl ProjectResource {
    pub fn source_kind(&self) -> ResourceSourceKind {
        if self.source.starts_with("python://") {
            ResourceSourceKind::Python {
                uri: self.source.clone(),
            }
        } else if self.source.starts_with("rust://") {
            ResourceSourceKind::Rust {
                uri: self.source.clone(),
            }
        } else if self.source.contains("://") {
            ResourceSourceKind::External {
                uri: self.source.clone(),
            }
        } else {
            ResourceSourceKind::DeclarativeFile {
                path: self.source.clone(),
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResourceSourceKind {
    DeclarativeFile { path: String },
    Python { uri: String },
    Rust { uri: String },
    External { uri: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectFreshness {
    pub expect_every: Option<DurationSpec>,
    pub alert_after: Option<DurationSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustPreset {
    Experimental,
    Governed,
    Financial,
    Serving,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteDispositionPreset {
    Append,
    Replace,
    Merge,
    CdcApply,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetentionRule {
    Runs(u32),
    Duration(DurationSpec),
}

impl fmt::Display for RetentionRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runs(runs) => write!(f, "{runs} runs"),
            Self::Duration(duration) => duration.fmt(f),
        }
    }
}

impl Serialize for RetentionRule {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for RetentionRule {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_retention_rule(&value).map_err(de::Error::custom)
    }
}

#[derive(Clone, Default, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub default: Option<RetentionRule>,
    pub experimental: Option<RetentionRule>,
    pub governed: Option<RetentionRule>,
    pub financial: Option<RetentionRule>,
    pub serving: Option<RetentionRule>,
}

impl RetentionPolicy {
    pub(crate) fn overlay(self, override_policy: Self) -> Self {
        Self {
            default: override_policy.default.or(self.default),
            experimental: override_policy.experimental.or(self.experimental),
            governed: override_policy.governed.or(self.governed),
            financial: override_policy.financial.or(self.financial),
            serving: override_policy.serving.or(self.serving),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DurationSpec {
    millis: u64,
}

impl DurationSpec {
    pub fn from_millis(millis: u64) -> Self {
        Self { millis }
    }

    pub fn millis(self) -> u64 {
        self.millis
    }
}

impl fmt::Display for DurationSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let millis = self.millis;
        if millis.is_multiple_of(86_400_000) {
            write!(f, "{}d", millis / 86_400_000)
        } else if millis.is_multiple_of(3_600_000) {
            write!(f, "{}h", millis / 3_600_000)
        } else if millis.is_multiple_of(60_000) {
            write!(f, "{}m", millis / 60_000)
        } else if millis.is_multiple_of(1_000) {
            write!(f, "{}s", millis / 1_000)
        } else {
            write!(f, "{millis}ms")
        }
    }
}

impl Serialize for DurationSpec {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DurationSpec {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        parse_duration_spec(&value).map_err(de::Error::custom)
    }
}
