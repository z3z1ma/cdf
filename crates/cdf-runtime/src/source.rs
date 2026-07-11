use std::{collections::BTreeMap, path::Path, sync::Arc};

use arrow_schema::Schema;
use cdf_http::SecretProvider;
use cdf_kernel::{
    CdfError, EffectiveSchemaRuntime, ErrorKind, QueryableResource, ResourceCapabilities,
    ResourceDescriptor, Result, TypePolicyAllowances,
};
use serde::{Deserialize, Serialize};

use crate::{BlockingLaneSpec, ExecutionServices, artifact_hash};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SourceDriverId(String);

impl SourceDriverId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(CdfError::contract(
                "source driver id must contain 1..=128 lowercase ASCII letters, digits, or underscores",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceDriverDescriptor {
    pub driver_id: SourceDriverId,
    pub driver_version: String,
    pub option_schema_hash: String,
    pub kinds: Vec<String>,
    pub schemes: Vec<String>,
}

impl SourceDriverDescriptor {
    pub fn validate(&self) -> Result<()> {
        validate_version(&self.driver_version)?;
        validate_hash("source option schema", &self.option_schema_hash)?;
        if self.kinds.is_empty() {
            return Err(CdfError::contract(
                "source driver must declare at least one source kind",
            ));
        }
        validate_names("source kind", &self.kinds)?;
        validate_names("source scheme", &self.schemes)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceExecutorClass {
    Io,
    Cpu,
    BlockingLane,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceRetryGranularity {
    None,
    Partition,
    Unit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAttestationStrength {
    None,
    Metadata,
    ImmutableContent,
    Snapshot,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceExecutionCapabilities {
    pub minimum_poll_bytes: u64,
    pub maximum_poll_bytes: u64,
    pub minimum_decode_bytes: u64,
    pub maximum_decode_bytes: u64,
    pub maximum_concurrency: u16,
    pub useful_concurrency: u16,
    pub executor_class: SourceExecutorClass,
    pub blocking_lane: Option<BlockingLaneSpec>,
    pub pausable: bool,
    pub spillable: bool,
    pub idempotent_reads: bool,
    pub reopenable: bool,
    pub resumable: bool,
    pub speculative_safe: bool,
    pub retry_granularity: SourceRetryGranularity,
    pub retryable_errors: Vec<ErrorKind>,
    pub attestation: SourceAttestationStrength,
    pub rate_limit_per_second: Option<u64>,
    pub quota_authority: Option<String>,
    pub canonical_order: bool,
    pub bounded: bool,
    pub telemetry_version: String,
}

impl SourceExecutionCapabilities {
    pub fn validate(&self) -> Result<()> {
        if self.minimum_poll_bytes == 0
            || self.maximum_poll_bytes < self.minimum_poll_bytes
            || self.minimum_decode_bytes == 0
            || self.maximum_decode_bytes < self.minimum_decode_bytes
            || self.maximum_concurrency == 0
            || self.useful_concurrency == 0
            || self.useful_concurrency > self.maximum_concurrency
        {
            return Err(CdfError::contract(
                "source execution capabilities require nonzero ordered working sets and concurrency bounds",
            ));
        }
        match (&self.executor_class, &self.blocking_lane) {
            (SourceExecutorClass::BlockingLane, Some(lane)) => lane.validate()?,
            (SourceExecutorClass::BlockingLane, _) => {
                return Err(CdfError::contract(
                    "blocking source execution requires a declared lane",
                ));
            }
            (_, None) => {}
            _ => {
                return Err(CdfError::contract(
                    "nonblocking source execution cannot declare a blocking lane",
                ));
            }
        }
        if self.retry_granularity != SourceRetryGranularity::None
            && (!self.idempotent_reads || !self.reopenable)
        {
            return Err(CdfError::contract(
                "source retry requires idempotent and reopenable reads",
            ));
        }
        if self.speculative_safe
            && (!self.idempotent_reads
                || !self.reopenable
                || self.attestation == SourceAttestationStrength::None)
        {
            return Err(CdfError::contract(
                "speculative source execution requires idempotent reopenable reads with attestation",
            ));
        }
        if self.resumable && !self.reopenable {
            return Err(CdfError::contract(
                "resumable source execution requires reopenable reads",
            ));
        }
        if self.rate_limit_per_second == Some(0) {
            return Err(CdfError::contract(
                "source rate limit must be nonzero when declared",
            ));
        }
        validate_version(&self.telemetry_version)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceCompileRequest {
    pub source_kind: String,
    pub source_options: BTreeMap<String, serde_json::Value>,
    pub resource_options: BTreeMap<String, serde_json::Value>,
    pub descriptor: ResourceDescriptor,
    pub schema: Schema,
    pub type_policy_allowances: TypePolicyAllowances,
    pub effective_schema_runtime: Option<EffectiveSchemaRuntime>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompiledSourcePlan {
    pub driver: SourceDriverDescriptor,
    pub descriptor: ResourceDescriptor,
    pub resource_capabilities: ResourceCapabilities,
    pub execution_capabilities: SourceExecutionCapabilities,
    pub schema: Schema,
    pub type_policy_allowances: TypePolicyAllowances,
    pub effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    pub redacted_options: serde_json::Value,
    pub redacted_options_hash: String,
    pub physical_plan: serde_json::Value,
    pub physical_plan_hash: String,
}

impl CompiledSourcePlan {
    pub fn new(
        driver: SourceDriverDescriptor,
        descriptor: ResourceDescriptor,
        resource_capabilities: ResourceCapabilities,
        execution_capabilities: SourceExecutionCapabilities,
        schema: Schema,
        type_policy_allowances: TypePolicyAllowances,
        effective_schema_runtime: Option<EffectiveSchemaRuntime>,
        redacted_options: serde_json::Value,
        physical_plan: serde_json::Value,
    ) -> Result<Self> {
        driver.validate()?;
        execution_capabilities.validate()?;
        let redacted_options_hash = artifact_hash(&redacted_options)?;
        let physical_plan_hash = artifact_hash(&physical_plan)?;
        Ok(Self {
            driver,
            descriptor,
            resource_capabilities,
            execution_capabilities,
            schema,
            type_policy_allowances,
            effective_schema_runtime,
            redacted_options,
            redacted_options_hash,
            physical_plan,
            physical_plan_hash,
        })
    }

    pub fn validate(&self) -> Result<()> {
        self.driver.validate()?;
        self.execution_capabilities.validate()?;
        if artifact_hash(&self.redacted_options)? != self.redacted_options_hash
            || artifact_hash(&self.physical_plan)? != self.physical_plan_hash
        {
            return Err(CdfError::contract(
                "compiled source plan hash does not match its canonical payload",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SourceResolutionContext<'a> {
    project_root: &'a Path,
    secret_provider: Arc<dyn SecretProvider + Send + Sync>,
    execution: &'a ExecutionServices,
}

impl<'a> SourceResolutionContext<'a> {
    pub fn new(
        project_root: &'a Path,
        secret_provider: Arc<dyn SecretProvider + Send + Sync>,
        execution: &'a ExecutionServices,
    ) -> Self {
        Self {
            project_root,
            secret_provider,
            execution,
        }
    }

    pub fn project_root(&self) -> &'a Path {
        self.project_root
    }

    pub fn secret_provider(&self) -> &Arc<dyn SecretProvider + Send + Sync> {
        &self.secret_provider
    }

    pub fn execution(&self) -> &'a ExecutionServices {
        self.execution
    }
}

pub trait SourceDriver: Send + Sync {
    fn descriptor(&self) -> &SourceDriverDescriptor;
    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan>;
    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>>;
}

fn validate_version(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(CdfError::contract(
            "source driver/telemetry version must be a safe 1..=64 character token",
        ));
    }
    Ok(())
}

fn validate_hash(label: &str, value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(CdfError::contract(format!(
            "{label} hash must use sha256:<64 lowercase hex>"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CdfError::contract(format!(
            "{label} hash must use sha256:<64 lowercase hex>"
        )));
    }
    Ok(())
}

fn validate_names(label: &str, values: &[String]) -> Result<()> {
    let mut sorted = values.to_vec();
    sorted.sort();
    if sorted.windows(2).any(|pair| pair[0] == pair[1])
        || values.iter().any(|value| {
            value.is_empty()
                || value.len() > 64
                || !value.bytes().all(|byte| {
                    byte.is_ascii_lowercase()
                        || byte.is_ascii_digit()
                        || matches!(byte, b'+' | b'-' | b'_' | b'.')
                })
        })
    {
        return Err(CdfError::contract(format!(
            "{label} declarations must be unique safe lowercase tokens"
        )));
    }
    Ok(())
}
