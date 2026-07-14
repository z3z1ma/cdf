use std::{
    any::{Any, type_name},
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};

use arrow_schema::Schema;
use cdf_http::SecretProvider;
use cdf_kernel::{
    CdfError, EffectiveSchemaRuntime, ErrorKind, PayloadRetention, QueryableResource,
    ResourceCapabilities, ResourceDescriptor, ResourceId, Result, TypePolicyAllowances,
};
use serde::{Deserialize, Serialize};

use crate::{BlockingLaneSpec, ExecutionServices, artifact_hash};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PreparedSourcePayloadKey {
    resource_id: ResourceId,
    driver_id: SourceDriverId,
    payload_hash: String,
}

impl PreparedSourcePayloadKey {
    pub fn new(
        resource_id: ResourceId,
        driver_id: SourceDriverId,
        payload_hash: impl Into<String>,
    ) -> Result<Self> {
        let payload_hash = payload_hash.into();
        validate_hash("prepared source payload", &payload_hash)?;
        Ok(Self {
            resource_id,
            driver_id,
            payload_hash,
        })
    }

    pub fn resource_id(&self) -> &ResourceId {
        &self.resource_id
    }

    pub fn driver_id(&self) -> &SourceDriverId {
        &self.driver_id
    }
}

pub struct PreparedSourcePayload {
    payload: Box<dyn Any + Send>,
    payload_type: &'static str,
    retention: PayloadRetention,
}

impl std::fmt::Debug for PreparedSourcePayload {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedSourcePayload")
            .field("payload_type", &self.payload_type)
            .field("retained_bytes", &self.retention.bytes())
            .finish_non_exhaustive()
    }
}

impl PreparedSourcePayload {
    pub fn new<T>(payload: T, retention: PayloadRetention) -> Self
    where
        T: Any + Send,
    {
        Self {
            payload: Box::new(payload),
            payload_type: type_name::<T>(),
            retention,
        }
    }

    pub fn into_typed<T>(self, expected_payload: &'static str) -> Result<(T, PayloadRetention)>
    where
        T: Any + Send,
    {
        let observed_type = self.payload_type;
        let payload = self.payload.downcast::<T>().map_err(|_| {
            CdfError::internal(format!(
                "prepared source payload for {expected_payload} has type `{observed_type}`, expected `{}`",
                type_name::<T>()
            ))
        })?;
        Ok((*payload, self.retention))
    }
}

#[derive(Clone, Debug, Default)]
pub struct SourceContentDigest {
    digest: Arc<Mutex<Option<String>>>,
}

impl SourceContentDigest {
    pub fn record(&self, digest: String) -> Result<()> {
        validate_hash("source content", &digest)?;
        let mut stored = self
            .digest
            .lock()
            .map_err(|_| CdfError::internal("source content-digest state was poisoned"))?;
        if stored.as_ref().is_some_and(|existing| existing != &digest) {
            return Err(CdfError::data(
                "one source invocation observed conflicting content digests",
            ));
        }
        *stored = Some(digest);
        Ok(())
    }

    pub fn completed(&self) -> Result<String> {
        self.digest
            .lock()
            .map_err(|_| CdfError::internal("source content-digest state was poisoned"))?
            .clone()
            .ok_or_else(|| CdfError::internal("fully consumed source omitted its content digest"))
    }
}

#[derive(Clone, Default)]
pub struct PreparedSourcePayloads {
    entries: Arc<Mutex<BTreeMap<PreparedSourcePayloadKey, PreparedSourcePayload>>>,
}

impl std::fmt::Debug for PreparedSourcePayloads {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedSourcePayloads")
            .field("pending", &self.pending_count().ok())
            .finish_non_exhaustive()
    }
}

impl PreparedSourcePayloads {
    pub fn install(
        &self,
        key: PreparedSourcePayloadKey,
        payload: PreparedSourcePayload,
    ) -> Result<()> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| CdfError::internal("prepared source payload store was poisoned"))?;
        if entries.contains_key(&key) {
            return Err(CdfError::internal(format!(
                "prepared source payload for resource `{}` and driver `{}` was installed twice",
                key.resource_id,
                key.driver_id.as_str()
            )));
        }
        entries.insert(key, payload);
        Ok(())
    }

    pub fn take(&self, key: &PreparedSourcePayloadKey) -> Result<Option<PreparedSourcePayload>> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| CdfError::internal("prepared source payload store was poisoned"))?
            .remove(key))
    }

    pub fn pending_count(&self) -> Result<usize> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| CdfError::internal("prepared source payload store was poisoned"))?
            .len())
    }
}

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompiledSourcePlanInput {
    pub descriptor: ResourceDescriptor,
    pub schema: Schema,
    pub type_policy_allowances: TypePolicyAllowances,
    pub effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    pub redacted_options: serde_json::Value,
    pub physical_plan: serde_json::Value,
}

impl CompiledSourcePlan {
    pub fn new(
        driver: SourceDriverDescriptor,
        resource_capabilities: ResourceCapabilities,
        execution_capabilities: SourceExecutionCapabilities,
        input: CompiledSourcePlanInput,
    ) -> Result<Self> {
        driver.validate()?;
        execution_capabilities.validate()?;
        let redacted_options_hash = artifact_hash(&input.redacted_options)?;
        let physical_plan_hash = artifact_hash(&input.physical_plan)?;
        Ok(Self {
            driver,
            descriptor: input.descriptor,
            resource_capabilities,
            execution_capabilities,
            schema: input.schema,
            type_policy_allowances: input.type_policy_allowances,
            effective_schema_runtime: input.effective_schema_runtime,
            redacted_options: input.redacted_options,
            redacted_options_hash,
            physical_plan: input.physical_plan,
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
    prepared_payloads: PreparedSourcePayloads,
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
            prepared_payloads: PreparedSourcePayloads::default(),
        }
    }

    pub fn with_prepared_payloads(mut self, prepared_payloads: PreparedSourcePayloads) -> Self {
        self.prepared_payloads = prepared_payloads;
        self
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

    pub fn prepared_payloads(&self) -> &PreparedSourcePayloads {
        &self.prepared_payloads
    }
}

pub trait SourceDriver: Send + Sync {
    fn descriptor(&self) -> &SourceDriverDescriptor;
    fn option_schema(&self) -> &serde_json::Value;
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
