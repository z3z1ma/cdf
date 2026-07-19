use std::{
    any::{Any, type_name},
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use arrow_schema::Schema;
use cdf_http::{EgressAllowlist, HttpMethod, HttpRequest, SecretProvider};
use cdf_kernel::{
    CdfError, EffectiveSchemaCatalogEntry, EffectiveSchemaRuntime, ErrorKind, EventTimeDomain,
    FreshnessSpec, OperatorWatermarkBehavior, PayloadRetention, PushdownFidelity,
    QueryableResource, ResourceCapabilities, ResourceDescriptor, ResourceId, Result,
    SafeFrontierPolicy, SchemaSource, SourcePosition, SourcePositionKind, TrustLevel,
    TypePolicyAllowances, WatermarkAuthority,
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

/// Credential-free network target presented to the host's source-egress policy.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceEgressTarget {
    scheme: String,
    host: String,
    port: Option<u16>,
}

impl SourceEgressTarget {
    pub fn parse(operational_uri: &str) -> Result<Self> {
        let parsed = url::Url::parse(operational_uri)
            .map_err(|_| CdfError::contract("source egress target must be a valid absolute URI"))?;
        let parsed_host = parsed
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| CdfError::contract("source egress target must name a host"))?;
        let host = parsed_host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(parsed_host)
            .trim_end_matches('.')
            .to_ascii_lowercase();
        let port = parsed.port();
        if port == Some(0) {
            return Err(CdfError::contract(
                "source egress target port must be in 1..=65535",
            ));
        }
        Ok(Self {
            scheme: parsed.scheme().to_owned(),
            host,
            port,
        })
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Returns a credential-free, path-free authority key suitable for shared quotas.
    pub fn canonical_authority(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        let effective_port = self.port.or(match self.scheme.as_str() {
            "http" => Some(80),
            "https" => Some(443),
            _ => None,
        });
        match effective_port {
            Some(port) => format!("{}://{host}:{port}", self.scheme),
            None => format!("{}://{host}", self.scheme),
        }
    }

    fn policy_uri(&self) -> String {
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        match self.port {
            Some(port) => format!("{}://{host}:{port}/", self.scheme),
            None => format!("{}://{host}/", self.scheme),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceEgressRequest {
    pub driver_id: SourceDriverId,
    pub target: SourceEgressTarget,
}

pub trait SourceEgressAuthorizer: Send + Sync {
    fn authorize(&self, request: &SourceEgressRequest) -> Result<()>;
}

impl SourceEgressAuthorizer for EgressAllowlist {
    fn authorize(&self, request: &SourceEgressRequest) -> Result<()> {
        self.check(&HttpRequest::new(
            HttpMethod::Get,
            request.target.policy_uri(),
        ))
    }
}

#[derive(Clone)]
pub struct SourceEgressScope {
    driver_id: SourceDriverId,
    authorizer: Arc<dyn SourceEgressAuthorizer>,
}

impl SourceEgressScope {
    pub fn new(driver_id: SourceDriverId, authorizer: Arc<dyn SourceEgressAuthorizer>) -> Self {
        Self {
            driver_id,
            authorizer,
        }
    }

    pub fn authorize(&self, operational_uri: &str) -> Result<()> {
        self.authorizer.authorize(&SourceEgressRequest {
            driver_id: self.driver_id.clone(),
            target: SourceEgressTarget::parse(operational_uri)?,
        })
    }

    pub fn driver_id(&self) -> &SourceDriverId {
        &self.driver_id
    }
}

impl std::fmt::Debug for SourceEgressScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceEgressScope")
            .field("driver_id", &self.driver_id)
            .finish_non_exhaustive()
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

fn validate_baseline_observation_schema_catalog(
    catalog: &[EffectiveSchemaCatalogEntry],
) -> Result<()> {
    for entry in catalog {
        if cdf_kernel::canonical_arrow_schema_hash(entry.schema.as_ref())?
            != entry.physical_schema_hash
        {
            return Err(CdfError::contract(
                "baseline observation schema catalog hash does not match its Arrow schema",
            ));
        }
    }
    if catalog
        .windows(2)
        .any(|pair| pair[0].physical_schema_hash >= pair[1].physical_schema_hash)
    {
        return Err(CdfError::contract(
            "baseline observation schema catalog must be sorted and unique by hash",
        ));
    }
    Ok(())
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRetryPolicy {
    pub max_total_attempts: u16,
    pub max_elapsed_ms: u64,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for SourceRetryPolicy {
    fn default() -> Self {
        Self {
            max_total_attempts: 3,
            max_elapsed_ms: 30_000,
            base_delay_ms: 100,
            max_delay_ms: 5_000,
        }
    }
}

impl SourceRetryPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.max_total_attempts == 0
            || self.max_elapsed_ms == 0
            || self.base_delay_ms == 0
            || self.max_delay_ms < self.base_delay_ms
        {
            return Err(CdfError::contract(
                "source retry policy requires nonzero attempts/deadline/backoff and a maximum delay at least as large as its base",
            ));
        }
        Ok(())
    }

    /// Narrows a source-owned hard ceiling while retaining its backoff shape.
    pub fn narrow(
        &self,
        max_total_attempts: Option<u16>,
        max_elapsed_ms: Option<u64>,
    ) -> Result<Self> {
        self.validate()?;
        let narrowed = Self {
            max_total_attempts: max_total_attempts
                .unwrap_or(self.max_total_attempts)
                .min(self.max_total_attempts),
            max_elapsed_ms: max_elapsed_ms
                .unwrap_or(self.max_elapsed_ms)
                .min(self.max_elapsed_ms),
            base_delay_ms: self.base_delay_ms,
            max_delay_ms: self.max_delay_ms,
        };
        narrowed.validate()?;
        Ok(narrowed)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceAttestationStrength {
    None,
    Metadata,
    ImmutableContent,
    Snapshot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceBatchMemoryContract {
    /// Every emitted in-memory batch already carries a lease covering its complete retained payload.
    Preaccounted,
    /// The runtime must reserve the compiled maximum before polling and bind it to the emitted batch.
    FrontierReserved,
}

/// A source-owned operation budget over a monotonic interval.
///
/// The unit is deliberately protocol-neutral: an adapter defines the operation (HTTP request,
/// database query, broker fetch) while the scheduler records the exact budget and quota scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRateLimit {
    pub operations: u64,
    pub interval_ms: u64,
}

impl SourceRateLimit {
    pub fn validate(&self) -> Result<()> {
        if self.operations == 0 || self.interval_ms == 0 {
            return Err(CdfError::contract(
                "source rate limit operations and interval must be nonzero",
            ));
        }
        Ok(())
    }
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
    pub retry_policy: Option<SourceRetryPolicy>,
    pub attestation: SourceAttestationStrength,
    pub rate_limit: Option<SourceRateLimit>,
    pub quota_authority: Option<String>,
    pub canonical_order: bool,
    pub bounded: bool,
    pub batch_memory: SourceBatchMemoryContract,
    pub telemetry_version: String,
}

/// Capabilities that exist only for an unbounded source. These claims are part of the compiled
/// source artifact; generic stream-policy compilation consumes them without matching driver ids.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceStreamCapabilities {
    pub quiescence: bool,
    pub watermark_behavior: OperatorWatermarkBehavior,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark: Option<SourceWatermarkCapability>,
    pub safe_frontiers: Vec<SafeFrontierPolicy>,
    /// Exact position dimensions for which the source can compare an authored termination
    /// frontier to emitted positions and stop without crossing it.
    pub source_frontiers: Vec<SourceFrontierCapability>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub idleness_capabilities: Vec<String>,
}

impl SourceStreamCapabilities {
    pub fn validate(&self) -> Result<()> {
        self.watermark_behavior.validate()?;
        match (&self.watermark_behavior, &self.watermark) {
            (OperatorWatermarkBehavior::Drop, None) => {}
            (OperatorWatermarkBehavior::Preserve, Some(capability))
                if capability.authority == WatermarkAuthority::Source =>
            {
                capability.validate()?;
            }
            (
                OperatorWatermarkBehavior::Transform { mapping_id },
                Some(SourceWatermarkCapability {
                    authority:
                        WatermarkAuthority::Derived {
                            mapping_id: authority_mapping,
                        },
                    ..
                }),
            ) if mapping_id == authority_mapping => {
                self.watermark
                    .as_ref()
                    .expect("matched source watermark capability")
                    .validate()?;
            }
            _ => {
                return Err(CdfError::contract(
                    "source watermark behavior must match one exact field/domain/authority capability; use drop with no capability, preserve with source authority, or transform with the same derived mapping id",
                ));
            }
        }
        if self.safe_frontiers.is_empty() {
            return Err(CdfError::contract(
                "unbounded source stream capabilities require at least one safe-frontier policy",
            ));
        }
        if self
            .safe_frontiers
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(CdfError::contract(
                "source safe-frontier policies must use canonical sorted order",
            ));
        }
        if self
            .source_frontiers
            .windows(2)
            .any(|pair| pair[0].kind() >= pair[1].kind())
        {
            return Err(CdfError::contract(
                "source-frontier capabilities must contain one declaration per kind in canonical sorted order",
            ));
        }
        for frontier in &self.source_frontiers {
            frontier.validate()?;
        }
        validate_names(
            "source stream idleness capability",
            &self.idleness_capabilities,
        )?;
        if self
            .idleness_capabilities
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(CdfError::contract(
                "source stream idleness capabilities must use canonical sorted order",
            ));
        }
        Ok(())
    }

    pub fn supports_frontier(&self, policy: SafeFrontierPolicy) -> bool {
        self.safe_frontiers.contains(&policy)
    }

    pub fn supports_idleness(&self, capability_id: &str) -> bool {
        self.idleness_capabilities
            .binary_search_by(|candidate| candidate.as_str().cmp(capability_id))
            .is_ok()
    }

    pub fn supports_source_frontier(&self, position: &SourcePosition) -> bool {
        let Some(capability) = self
            .source_frontiers
            .iter()
            .find(|capability| capability.kind() == position.kind())
        else {
            return false;
        };
        match (capability, position) {
            (SourceFrontierCapability::Cursor { fields }, SourcePosition::Cursor(position)) => {
                fields.binary_search(&position.field).is_ok()
            }
            (SourceFrontierCapability::Log { logs }, SourcePosition::Log(position)) => {
                logs.binary_search(&position.log).is_ok()
            }
            (SourceFrontierCapability::FileManifest, SourcePosition::FileManifest(_))
            | (SourceFrontierCapability::PageToken, SourcePosition::PageToken(_)) => true,
            (SourceFrontierCapability::Composite, SourcePosition::Composite(position)) => position
                .positions
                .values()
                .all(|position| self.supports_source_frontier(position)),
            (
                SourceFrontierCapability::ForeignState { protocols },
                SourcePosition::ForeignState(position),
            ) => protocols.binary_search(&position.protocol).is_ok(),
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceWatermarkCapability {
    pub event_time_field: Box<str>,
    pub domain: EventTimeDomain,
    pub authority: WatermarkAuthority,
}

impl SourceWatermarkCapability {
    pub fn validate(&self) -> Result<()> {
        validate_stream_name("source watermark event-time field", &self.event_time_field)?;
        self.domain.validate()?;
        self.authority.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceFrontierCapability {
    Cursor { fields: Vec<String> },
    Log { logs: Vec<String> },
    FileManifest,
    PageToken,
    Composite,
    ForeignState { protocols: Vec<String> },
}

impl SourceFrontierCapability {
    pub const fn kind(&self) -> SourcePositionKind {
        match self {
            Self::Cursor { .. } => SourcePositionKind::Cursor,
            Self::Log { .. } => SourcePositionKind::Log,
            Self::FileManifest => SourcePositionKind::FileManifest,
            Self::PageToken => SourcePositionKind::PageToken,
            Self::Composite => SourcePositionKind::Composite,
            Self::ForeignState { .. } => SourcePositionKind::ForeignState,
        }
    }

    fn validate(&self) -> Result<()> {
        match self {
            Self::Cursor { fields } => validate_frontier_dimensions("cursor field", fields),
            Self::Log { logs } => validate_frontier_dimensions("log name", logs),
            Self::ForeignState { protocols } => {
                validate_frontier_dimensions("foreign-state protocol", protocols)
            }
            Self::FileManifest | Self::PageToken | Self::Composite => Ok(()),
        }
    }
}

fn validate_frontier_dimensions(label: &str, values: &[String]) -> Result<()> {
    if values.is_empty() {
        return Err(CdfError::contract(format!(
            "source-frontier {label} capability requires at least one value"
        )));
    }
    for value in values {
        validate_stream_name(&format!("source-frontier {label}"), value)?;
    }
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(CdfError::contract(format!(
            "source-frontier {label} capabilities must be unique and canonically sorted"
        )));
    }
    Ok(())
}

fn validate_stream_name(label: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(CdfError::contract(format!(
            "{label} must be nonempty and control-free"
        )));
    }
    Ok(())
}

fn validate_source_stream_capabilities(
    execution: &SourceExecutionCapabilities,
    stream: Option<&SourceStreamCapabilities>,
) -> Result<()> {
    match (execution.bounded, stream) {
        (true, None) => Ok(()),
        (true, Some(_)) => Err(CdfError::contract(
            "bounded source cannot declare unbounded stream capabilities",
        )),
        (false, Some(capabilities)) => {
            capabilities.validate()?;
            if !execution.pausable && !execution.spillable {
                return Err(CdfError::contract(
                    "unbounded non-pausable source must declare spillable execution",
                ));
            }
            Ok(())
        }
        (false, None) => Err(CdfError::contract(
            "unbounded source requires compiled stream capabilities; declare safe-frontier, watermark, idleness, and quiescence support in the source driver",
        )),
    }
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
            (SourceExecutorClass::BlockingLane, Some(lane)) => {
                lane.validate()?;
                if lane.binding == crate::BlockingLaneBinding::RuntimeResolved {
                    return Err(CdfError::contract(
                        "compiled source execution cannot contain an already runtime-resolved blocking lane",
                    ));
                }
            }
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
        if self.retry_granularity != SourceRetryGranularity::None
            && self.batch_memory != SourceBatchMemoryContract::Preaccounted
        {
            return Err(CdfError::contract(
                "retryable source streams must preaccount emitted batches before the scheduler probes their first item",
            ));
        }
        if self.retry_granularity != SourceRetryGranularity::None
            && !matches!(
                self.attestation,
                SourceAttestationStrength::ImmutableContent | SourceAttestationStrength::Snapshot
            )
        {
            return Err(CdfError::contract(
                "source retry requires immutable-content or snapshot attestation",
            ));
        }
        match (self.retry_granularity, &self.retry_policy) {
            (SourceRetryGranularity::None, None) if self.retryable_errors.is_empty() => {}
            (SourceRetryGranularity::None, _) => {
                return Err(CdfError::contract(
                    "source with no retry granularity cannot declare retry errors or policy",
                ));
            }
            (_, Some(policy)) if !self.retryable_errors.is_empty() => policy.validate()?,
            (_, _) => {
                return Err(CdfError::contract(
                    "retryable source requires typed retry errors and a bounded retry policy",
                ));
            }
        }
        if self
            .retryable_errors
            .iter()
            .any(|kind| !matches!(kind, ErrorKind::Transient | ErrorKind::RateLimited))
        {
            return Err(CdfError::contract(
                "source execution retry may declare only transient or rate-limited errors",
            ));
        }
        let transient_count = self
            .retryable_errors
            .iter()
            .filter(|kind| matches!(kind, ErrorKind::Transient))
            .count();
        let rate_limited_count = self
            .retryable_errors
            .iter()
            .filter(|kind| matches!(kind, ErrorKind::RateLimited))
            .count();
        if transient_count > 1 || rate_limited_count > 1 {
            return Err(CdfError::contract(
                "source retry error declarations must be unique",
            ));
        }
        if self
            .retryable_errors
            .windows(2)
            .any(|pair| retry_error_rank(&pair[0]) >= retry_error_rank(&pair[1]))
        {
            return Err(CdfError::contract(
                "source retry error declarations must use canonical transient, rate_limited order",
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
        if let Some(rate_limit) = self.rate_limit {
            rate_limit.validate()?;
        }
        if self.rate_limit.is_some() && self.quota_authority.is_none() {
            return Err(CdfError::contract(
                "source rate limit requires a quota authority",
            ));
        }
        if self.quota_authority.as_ref().is_some_and(|authority| {
            authority.trim().is_empty()
                || authority.len() > 256
                || authority.chars().any(char::is_control)
        }) {
            return Err(CdfError::contract(
                "source quota authority must be a bounded non-empty control-free value",
            ));
        }
        self.maximum_poll_bytes
            .checked_add(self.maximum_decode_bytes)
            .ok_or_else(|| CdfError::contract("source working-set byte bounds overflow u64"))?;
        validate_version(&self.telemetry_version)
    }
}

fn retry_error_rank(kind: &ErrorKind) -> u8 {
    match kind {
        ErrorKind::Transient => 0,
        ErrorKind::RateLimited => 1,
        ErrorKind::Auth
        | ErrorKind::Contract
        | ErrorKind::Data
        | ErrorKind::Destination
        | ErrorKind::Internal => u8::MAX,
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceCompileContext {
    pub source_name: String,
    pub project_root: Option<PathBuf>,
    pub cursor_pushdown: Option<SourceCursorPushdown>,
}

impl SourceCompileContext {
    pub fn validate(&self) -> Result<()> {
        if self.source_name.is_empty() {
            return Err(CdfError::contract(
                "source compilation context requires a source name",
            ));
        }
        if let Some(cursor) = &self.cursor_pushdown {
            cursor.validate()?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceCursorPushdown {
    pub parameter: Option<String>,
    pub fidelity: PushdownFidelity,
}

impl SourceCursorPushdown {
    fn validate(&self) -> Result<()> {
        if self.parameter.as_ref().is_some_and(String::is_empty) {
            return Err(CdfError::contract(
                "source cursor pushdown parameter cannot be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceCompileRequest {
    pub source_kind: String,
    pub context: SourceCompileContext,
    pub source_options: BTreeMap<String, serde_json::Value>,
    pub resource_options: BTreeMap<String, serde_json::Value>,
    pub descriptor: ResourceDescriptor,
    pub schema: Schema,
    pub type_policy_allowances: TypePolicyAllowances,
    pub effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    pub baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
}

/// A project mapping whose URI is compiled by the driver that owns its scheme.
///
/// Unlike a declarative resource, the source may own plan-time metadata extraction needed to
/// produce the descriptor and Arrow schema. Framework-owned identity, trust, and freshness remain
/// explicit inputs and are revalidated by `SourceRegistry` after the driver returns.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceReferenceCompileRequest {
    pub uri: String,
    pub resource_id: ResourceId,
    pub project_root: PathBuf,
    pub trust_level: TrustLevel,
    pub freshness: Option<FreshnessSpec>,
    pub project_options: serde_json::Value,
}

impl SourceReferenceCompileRequest {
    pub fn validate(&self) -> Result<()> {
        if self.uri.is_empty()
            || self.uri.chars().any(char::is_control)
            || !self.uri.contains("://")
        {
            return Err(CdfError::contract(
                "source project reference requires a nonempty control-free URI with an explicit scheme",
            ));
        }
        if !self.project_options.is_object() {
            return Err(CdfError::contract(
                "source project reference options must be a JSON object",
            ));
        }
        Ok(())
    }
}

pub trait SourceReferenceCompiler: Send + Sync {
    fn compile_reference(
        &self,
        request: SourceReferenceCompileRequest,
    ) -> Result<CompiledSourcePlan>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceHealthStatus {
    Passed,
    Failed,
    Skipped,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceHealthResult {
    pub probe_id: String,
    pub status: SourceHealthStatus,
    pub message: String,
    pub details: serde_json::Value,
}

impl SourceHealthResult {
    pub fn failed(
        probe_id: impl Into<String>,
        message: impl Into<String>,
        resource_id: &ResourceId,
        error: &CdfError,
    ) -> Self {
        Self {
            probe_id: probe_id.into(),
            status: SourceHealthStatus::Failed,
            message: message.into(),
            details: serde_json::json!({
                "resource_id": resource_id.as_str(),
                "error_kind": error_kind_code(&error.kind),
            }),
        }
    }

    pub fn validate(&self) -> Result<()> {
        const MAX_PROBE_ID_BYTES: usize = 192;
        const MAX_MESSAGE_BYTES: usize = 4 * 1024;
        const MAX_DETAILS_BYTES: usize = 64 * 1024;

        if self.probe_id.is_empty()
            || self.probe_id.len() > MAX_PROBE_ID_BYTES
            || !self
                .probe_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(CdfError::contract(
                "source health probe id must be bounded and contain only ASCII letters, digits, `.`, `_`, or `-`",
            ));
        }
        if self.message.is_empty()
            || self.message.len() > MAX_MESSAGE_BYTES
            || self.message.chars().any(char::is_control)
            || self.message.contains("://")
        {
            return Err(CdfError::contract(
                "source health message must be bounded, control-free, and contain no URI; put redacted locations in details",
            ));
        }
        let encoded = serde_json::to_vec(&self.details).map_err(|error| {
            CdfError::internal(format!("serialize source health details: {error}"))
        })?;
        if encoded.len() > MAX_DETAILS_BYTES {
            return Err(CdfError::contract(format!(
                "source health details exceed the {MAX_DETAILS_BYTES}-byte boundary"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceHealthLimits {
    pub maximum_duration_ms: u64,
    pub maximum_work_units: u64,
    pub maximum_results: u64,
    pub maximum_details_bytes: u64,
    pub maximum_list_entries: u64,
    pub maximum_payload_bytes: u64,
    pub maximum_subprocess_output_bytes: u64,
}

impl SourceHealthLimits {
    pub fn validate(&self) -> Result<()> {
        if self.maximum_duration_ms == 0
            || self.maximum_work_units == 0
            || self.maximum_results == 0
            || self.maximum_details_bytes == 0
            || self.maximum_list_entries == 0
            || self.maximum_payload_bytes == 0
            || self.maximum_subprocess_output_bytes == 0
        {
            return Err(CdfError::contract(
                "source health limits must all be greater than zero",
            ));
        }
        Ok(())
    }
}

impl Default for SourceHealthLimits {
    fn default() -> Self {
        Self {
            maximum_duration_ms: 30_000,
            maximum_work_units: 10_000,
            maximum_results: 4_096,
            maximum_details_bytes: 8 * 1024 * 1024,
            maximum_list_entries: 10_000,
            maximum_payload_bytes: 8 * 1024 * 1024,
            maximum_subprocess_output_bytes: 64 * 1024,
        }
    }
}

#[derive(Clone)]
pub struct SourceHealthBudget {
    limits: SourceHealthLimits,
    started_ms: u64,
    execution: ExecutionServices,
    cancellation: crate::RunCancellation,
    state: Arc<Mutex<SourceHealthBudgetState>>,
}

#[derive(Default)]
struct SourceHealthBudgetState {
    work_units: u64,
    results: u64,
    details_bytes: u64,
    list_entries: u64,
    payload_bytes: u64,
}

impl std::fmt::Debug for SourceHealthBudget {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceHealthBudget")
            .field("limits", &self.limits)
            .field("started_ms", &self.started_ms)
            .finish_non_exhaustive()
    }
}

impl SourceHealthBudget {
    pub fn new(
        limits: SourceHealthLimits,
        execution: ExecutionServices,
        cancellation: crate::RunCancellation,
    ) -> Result<Self> {
        limits.validate()?;
        let started_ms = duration_millis(execution.monotonic_now());
        Ok(Self {
            limits,
            started_ms,
            execution,
            cancellation,
            state: Arc::new(Mutex::new(SourceHealthBudgetState::default())),
        })
    }

    pub fn limits(&self) -> SourceHealthLimits {
        self.limits
    }

    pub fn cancellation(&self) -> crate::RunCancellation {
        self.cancellation.clone()
    }

    pub fn check(&self) -> Result<()> {
        self.cancellation.check()?;
        let elapsed_ms =
            duration_millis(self.execution.monotonic_now()).saturating_sub(self.started_ms);
        if elapsed_ms >= self.limits.maximum_duration_ms {
            return Err(CdfError::data(format!(
                "source health deadline of {} ms was exhausted",
                self.limits.maximum_duration_ms
            )));
        }
        Ok(())
    }

    pub fn remaining_duration(&self) -> Result<Duration> {
        self.check()?;
        let elapsed_ms =
            duration_millis(self.execution.monotonic_now()).saturating_sub(self.started_ms);
        Ok(Duration::from_millis(
            self.limits.maximum_duration_ms.saturating_sub(elapsed_ms),
        ))
    }

    /// Absolute host-monotonic deadline shared by every operation in this health probe.
    pub fn deadline(&self) -> Duration {
        Duration::from_millis(
            self.started_ms
                .saturating_add(self.limits.maximum_duration_ms),
        )
    }

    pub fn consume_work(&self, units: u64) -> Result<()> {
        self.consume_counter(
            units,
            self.limits.maximum_work_units,
            "work-unit",
            |state| &mut state.work_units,
        )
    }

    pub fn consume_list_entries(&self, entries: u64) -> Result<()> {
        self.consume_counter(
            entries,
            self.limits.maximum_list_entries,
            "list-entry",
            |state| &mut state.list_entries,
        )
    }

    pub fn remaining_list_entries(&self) -> Result<u64> {
        self.check()?;
        let state = self
            .state
            .lock()
            .map_err(|_| CdfError::internal("source health budget lock is poisoned"))?;
        Ok(self
            .limits
            .maximum_list_entries
            .saturating_sub(state.list_entries))
    }

    pub fn consume_payload_bytes(&self, bytes: u64) -> Result<()> {
        self.consume_counter(
            bytes,
            self.limits.maximum_payload_bytes,
            "payload-byte",
            |state| &mut state.payload_bytes,
        )
    }

    pub fn record_result(&self, result: &SourceHealthResult) -> Result<()> {
        let details_bytes = u64::try_from(
            serde_json::to_vec(&result.details)
                .map_err(|error| CdfError::internal(format!("serialize health details: {error}")))?
                .len(),
        )
        .unwrap_or(u64::MAX);
        self.check()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| CdfError::internal("source health budget lock is poisoned"))?;
        let next_results = state
            .results
            .checked_add(1)
            .ok_or_else(|| CdfError::data("source health result budget overflowed"))?;
        let next_details = state
            .details_bytes
            .checked_add(details_bytes)
            .ok_or_else(|| CdfError::data("source health detail-byte budget overflowed"))?;
        if next_results > self.limits.maximum_results {
            return Err(CdfError::data(format!(
                "source health result budget of {} was exhausted",
                self.limits.maximum_results
            )));
        }
        if next_details > self.limits.maximum_details_bytes {
            return Err(CdfError::data(format!(
                "source health detail-byte budget of {} was exhausted",
                self.limits.maximum_details_bytes
            )));
        }
        state.results = next_results;
        state.details_bytes = next_details;
        Ok(())
    }

    pub fn delay(&self, duration: Duration) -> Result<()> {
        self.check()?;
        self.execution
            .run_io(self.execution.delay(duration, self.cancellation.clone()))?;
        self.check()
    }

    fn consume_counter(
        &self,
        amount: u64,
        maximum: u64,
        label: &str,
        select: impl FnOnce(&mut SourceHealthBudgetState) -> &mut u64,
    ) -> Result<()> {
        self.check()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| CdfError::internal("source health budget lock is poisoned"))?;
        let counter = select(&mut state);
        let next = counter
            .checked_add(amount)
            .ok_or_else(|| CdfError::data(format!("source health {label} budget overflowed")))?;
        if next > maximum {
            return Err(CdfError::data(format!(
                "source health {label} budget of {maximum} was exhausted"
            )));
        }
        *counter = next;
        Ok(())
    }
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[derive(Clone, Debug)]
pub struct SourceHealthRequest {
    pub compiled_plans: Vec<CompiledSourcePlan>,
    pub configured_resource_ids: Vec<ResourceId>,
    pub budget: SourceHealthBudget,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceHealthTarget {
    resource_id: ResourceId,
    driver_id: SourceDriverId,
}

impl SourceHealthTarget {
    pub fn new(resource_id: ResourceId, driver_id: SourceDriverId) -> Self {
        Self {
            resource_id,
            driver_id,
        }
    }

    pub fn resource_id(&self) -> &ResourceId {
        &self.resource_id
    }

    pub fn driver_id(&self) -> &SourceDriverId {
        &self.driver_id
    }
}

/// Registry-owned admission boundary for source health output.
///
/// Drivers emit one result at a time. The registry validates and accounts each result before it
/// can become retained command output, so an adapter cannot bypass the aggregate result/detail
/// limits by constructing an unbounded return collection first.
pub trait SourceHealthSink {
    fn emit(&mut self, result: SourceHealthResult) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompiledSourcePlan {
    pub driver: SourceDriverDescriptor,
    pub descriptor: ResourceDescriptor,
    pub resource_capabilities: ResourceCapabilities,
    pub execution_capabilities: SourceExecutionCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_capabilities: Option<SourceStreamCapabilities>,
    pub schema: Schema,
    pub type_policy_allowances: TypePolicyAllowances,
    pub effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    pub baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
    pub redacted_options: serde_json::Value,
    pub redacted_options_hash: String,
    pub physical_plan: serde_json::Value,
    pub physical_plan_hash: String,
}

/// Source-neutral execution ceiling recorded in the engine plan.
///
/// Partition schedules are derived from and revalidated against this compiled plan; explain data
/// is descriptive and cannot widen the source's retry, concurrency, or attestation declarations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledSourceCompilerBinding {
    pub driver_id: String,
    pub driver_version: String,
    pub option_schema_hash: String,
    pub physical_plan_hash: String,
    pub compiled_source_plan_hash: String,
    pub source_semantics_hash: String,
    pub execution_capabilities_hash: String,
}

impl CompiledSourceCompilerBinding {
    pub fn compile(source: &CompiledSourcePlan) -> Result<Self> {
        source.validate()?;
        let binding = Self {
            driver_id: source.driver.driver_id.as_str().to_owned(),
            driver_version: source.driver.driver_version.clone(),
            option_schema_hash: source.driver.option_schema_hash.clone(),
            physical_plan_hash: source.physical_plan_hash.clone(),
            compiled_source_plan_hash: artifact_hash(source)?,
            source_semantics_hash: source.schema_binding_stable_hash()?,
            execution_capabilities_hash: artifact_hash(&source.execution_capabilities)?,
        };
        binding.validate()?;
        Ok(binding)
    }

    pub fn validate(&self) -> Result<()> {
        SourceDriverId::new(self.driver_id.clone())?;
        validate_version(&self.driver_version)?;
        validate_hash("source option schema", &self.option_schema_hash)?;
        validate_hash("compiled source physical plan", &self.physical_plan_hash)?;
        validate_hash(
            "complete compiled source plan",
            &self.compiled_source_plan_hash,
        )?;
        validate_hash("compiled source semantics", &self.source_semantics_hash)?;
        validate_hash(
            "compiled source execution capabilities",
            &self.execution_capabilities_hash,
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledSourceExecutionPlan {
    pub(crate) resource_id: ResourceId,
    pub(crate) driver: SourceDriverDescriptor,
    pub(crate) physical_plan_hash: String,
    pub(crate) execution_capabilities: SourceExecutionCapabilities,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) stream_capabilities: Option<SourceStreamCapabilities>,
    compiled_source_plan_hash: String,
    source_semantics_hash: String,
    execution_binding_hash: String,
}

impl CompiledSourceExecutionPlan {
    pub fn compile(source: &CompiledSourcePlan) -> Result<Self> {
        source.validate()?;
        let compiled_source_plan_hash = artifact_hash(source)?;
        let source_semantics_hash = source.schema_binding_stable_hash()?;
        let mut plan = Self {
            resource_id: source.descriptor.resource_id.clone(),
            driver: source.driver.clone(),
            physical_plan_hash: source.physical_plan_hash.clone(),
            execution_capabilities: source.execution_capabilities.clone(),
            stream_capabilities: source.stream_capabilities.clone(),
            compiled_source_plan_hash,
            source_semantics_hash,
            execution_binding_hash: String::new(),
        };
        plan.execution_binding_hash = plan.canonical_binding_hash()?;
        plan.validate()?;
        Ok(plan)
    }

    pub fn validate(&self) -> Result<()> {
        ResourceId::new(self.resource_id.as_str())?;
        self.driver.validate()?;
        self.execution_capabilities.validate()?;
        validate_source_stream_capabilities(
            &self.execution_capabilities,
            self.stream_capabilities.as_ref(),
        )?;
        validate_hash("compiled source physical plan", &self.physical_plan_hash)?;
        validate_hash(
            "complete compiled source plan",
            &self.compiled_source_plan_hash,
        )?;
        validate_hash("compiled source semantics", &self.source_semantics_hash)?;
        validate_hash(
            "compiled source execution binding",
            &self.execution_binding_hash,
        )?;
        if self.execution_binding_hash != self.canonical_binding_hash()? {
            return Err(CdfError::contract(
                "compiled source execution binding does not match its compiler-owned semantics",
            ));
        }
        Ok(())
    }

    pub fn validate_compiler_binding(&self, binding: &CompiledSourceCompilerBinding) -> Result<()> {
        self.validate()?;
        binding.validate()?;
        if self.driver.driver_id.as_str() != binding.driver_id
            || self.driver.driver_version != binding.driver_version
            || self.driver.option_schema_hash != binding.option_schema_hash
            || self.physical_plan_hash != binding.physical_plan_hash
            || self.compiled_source_plan_hash != binding.compiled_source_plan_hash
            || self.source_semantics_hash != binding.source_semantics_hash
            || artifact_hash(&self.execution_capabilities)? != binding.execution_capabilities_hash
        {
            return Err(CdfError::data(
                "compiled source execution ceiling does not match its compiler source binding",
            ));
        }
        Ok(())
    }

    /// Recomputes the canonical self-binding used to validate serialized execution ceilings.
    pub fn canonical_binding_hash(&self) -> Result<String> {
        let mut identity = serde_json::json!({
            "resource_id": self.resource_id,
            "driver": self.driver,
            "physical_plan_hash": self.physical_plan_hash,
            "compiled_source_plan_hash": self.compiled_source_plan_hash,
            "execution_capabilities": self.execution_capabilities,
            "source_semantics_hash": self.source_semantics_hash,
        });
        if let Some(capabilities) = &self.stream_capabilities {
            identity
                .as_object_mut()
                .expect("source execution identity is an object")
                .insert(
                    "stream_capabilities".to_owned(),
                    serde_json::to_value(capabilities)
                        .map_err(|error| CdfError::internal(error.to_string()))?,
                );
        }
        artifact_hash(&identity)
    }

    pub fn compiled_source_plan_hash(&self) -> &str {
        &self.compiled_source_plan_hash
    }

    pub fn batch_memory_contract(&self) -> SourceBatchMemoryContract {
        self.execution_capabilities.batch_memory
    }

    pub fn execution_capabilities(&self) -> &SourceExecutionCapabilities {
        &self.execution_capabilities
    }

    pub fn stream_capabilities(&self) -> Option<&SourceStreamCapabilities> {
        self.stream_capabilities.as_ref()
    }
}

fn validate_source_stream_schema(
    stream: Option<&SourceStreamCapabilities>,
    schema: &Schema,
) -> Result<()> {
    let Some(watermark) = stream.and_then(|stream| stream.watermark.as_ref()) else {
        return Ok(());
    };
    let field = schema
        .field_with_name(&watermark.event_time_field)
        .map_err(|_| {
            CdfError::contract(format!(
                "source watermark capability field `{}` is absent from the compiled source schema",
                watermark.event_time_field
            ))
        })?;
    if !watermark.domain.matches_arrow_type(field.data_type()) {
        return Err(CdfError::contract(format!(
            "source watermark capability field `{}` declares domain {:?} but its compiled Arrow type is {}; correct the source capability or schema",
            watermark.event_time_field,
            watermark.domain,
            field.data_type()
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompiledSourcePlanInput {
    pub descriptor: ResourceDescriptor,
    pub schema: Schema,
    pub type_policy_allowances: TypePolicyAllowances,
    pub effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    pub baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
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
        Self::new_with_stream_capabilities(
            driver,
            resource_capabilities,
            execution_capabilities,
            None,
            input,
        )
    }

    pub fn new_with_stream_capabilities(
        driver: SourceDriverDescriptor,
        resource_capabilities: ResourceCapabilities,
        execution_capabilities: SourceExecutionCapabilities,
        stream_capabilities: Option<SourceStreamCapabilities>,
        input: CompiledSourcePlanInput,
    ) -> Result<Self> {
        let redacted_options_hash = artifact_hash(&input.redacted_options)?;
        let physical_plan_hash = artifact_hash(&input.physical_plan)?;
        let plan = Self {
            driver,
            descriptor: input.descriptor,
            resource_capabilities,
            execution_capabilities,
            stream_capabilities,
            schema: input.schema,
            type_policy_allowances: input.type_policy_allowances,
            effective_schema_runtime: input.effective_schema_runtime,
            baseline_observation_schema_catalog: input.baseline_observation_schema_catalog,
            redacted_options: input.redacted_options,
            redacted_options_hash,
            physical_plan: input.physical_plan,
            physical_plan_hash,
        };
        plan.validate()?;
        Ok(plan)
    }

    pub fn validate(&self) -> Result<()> {
        self.driver.validate()?;
        self.descriptor.validate()?;
        self.resource_capabilities.validate()?;
        self.execution_capabilities.validate()?;
        validate_source_stream_capabilities(
            &self.execution_capabilities,
            self.stream_capabilities.as_ref(),
        )?;
        validate_source_stream_schema(self.stream_capabilities.as_ref(), &self.schema)?;
        if !self.redacted_options.is_object() || !self.physical_plan.is_object() {
            return Err(CdfError::contract(
                "compiled source options and physical plan must be JSON objects",
            ));
        }
        validate_compiled_source_artifact(&self.redacted_options, "compiled source options", 0)?;
        validate_compiled_source_artifact(&self.physical_plan, "compiled source physical plan", 0)?;
        validate_hash("compiled source options", &self.redacted_options_hash)?;
        validate_hash("compiled source physical plan", &self.physical_plan_hash)?;
        if artifact_hash(&self.redacted_options)? != self.redacted_options_hash
            || artifact_hash(&self.physical_plan)? != self.physical_plan_hash
        {
            return Err(CdfError::contract(
                "compiled source plan hash does not match its canonical payload",
            ));
        }
        validate_baseline_observation_schema_catalog(&self.baseline_observation_schema_catalog)?;
        if let Some(runtime) = &self.effective_schema_runtime {
            runtime.validate_for_resource(&self.descriptor)?;
            if runtime.schema_catalog != self.baseline_observation_schema_catalog {
                return Err(CdfError::contract(
                    "compiled source baseline catalog does not match effective-schema runtime authority",
                ));
            }
        }
        if self.resource_capabilities.idempotent_reads
            && !self.execution_capabilities.idempotent_reads
        {
            return Err(CdfError::contract(
                "resource idempotent-read capability exceeds its source execution capability",
            ));
        }
        match self.resource_capabilities.backpressure {
            cdf_kernel::BackpressureSupport::Pausable if !self.execution_capabilities.pausable => {
                return Err(CdfError::contract(
                    "resource pausable backpressure capability exceeds its source execution capability",
                ));
            }
            cdf_kernel::BackpressureSupport::SpillRequired
                if !self.execution_capabilities.spillable =>
            {
                return Err(CdfError::contract(
                    "resource spill-required backpressure capability exceeds its source execution capability",
                ));
            }
            _ => {}
        }
        Ok(())
    }

    /// Rebinds compiler-owned schema authority without invoking the source
    /// driver again. Driver identity, options, and physical plan remain exact.
    pub fn bind_schema_authority(
        mut self,
        descriptor: &ResourceDescriptor,
        schema: &Schema,
        effective_schema_runtime: Option<EffectiveSchemaRuntime>,
        mut baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
    ) -> Result<Self> {
        let mut expected_descriptor = self.descriptor.clone();
        expected_descriptor.schema_source = descriptor.schema_source.clone();
        if &expected_descriptor != descriptor {
            return Err(CdfError::contract(
                "compiled source plan schema binding changed non-schema resource authority",
            ));
        }
        if let Some(runtime) = &effective_schema_runtime {
            runtime.validate_for_resource(descriptor)?;
        }
        self.descriptor = descriptor.clone();
        self.schema = schema.clone();
        self.effective_schema_runtime = effective_schema_runtime;
        baseline_observation_schema_catalog
            .sort_by(|left, right| left.physical_schema_hash.cmp(&right.physical_schema_hash));
        baseline_observation_schema_catalog
            .dedup_by(|left, right| left.physical_schema_hash == right.physical_schema_hash);
        self.baseline_observation_schema_catalog = baseline_observation_schema_catalog;
        self.validate()?;
        Ok(self)
    }

    pub fn validate_schema_authority(
        &self,
        descriptor: &ResourceDescriptor,
        schema: &Schema,
        effective_schema_runtime: Option<&EffectiveSchemaRuntime>,
        baseline_observation_schema_catalog: &[EffectiveSchemaCatalogEntry],
    ) -> Result<()> {
        self.validate()?;
        if &self.descriptor != descriptor
            || &self.schema != schema
            || self.effective_schema_runtime.as_ref() != effective_schema_runtime
            || self.baseline_observation_schema_catalog != baseline_observation_schema_catalog
        {
            return Err(CdfError::contract(
                "compiled source plan does not match the prepared schema authority",
            ));
        }
        Ok(())
    }

    /// Hashes the complete compiled source semantics while excluding only the
    /// schema fields that the compiler is allowed to bind after discovery.
    pub fn schema_binding_stable_hash(&self) -> Result<String> {
        self.validate()?;
        let mut descriptor = self.descriptor.clone();
        descriptor.schema_source = SchemaSource::Discover;
        let mut identity = serde_json::json!({
            "driver": self.driver,
            "descriptor": descriptor,
            "resource_capabilities": self.resource_capabilities,
            "execution_capabilities": self.execution_capabilities,
            "type_policy_allowances": self.type_policy_allowances,
            "redacted_options": self.redacted_options,
            "redacted_options_hash": self.redacted_options_hash,
            "physical_plan": self.physical_plan,
            "physical_plan_hash": self.physical_plan_hash,
        });
        if let Some(capabilities) = &self.stream_capabilities {
            identity
                .as_object_mut()
                .expect("source identity is an object")
                .insert(
                    "stream_capabilities".to_owned(),
                    serde_json::to_value(capabilities)
                        .map_err(|error| CdfError::internal(error.to_string()))?,
                );
        }
        artifact_hash(&identity)
    }

    /// Hashes only the source interpretation that can change discovery observations.
    ///
    /// Cursor, disposition, keys, and other post-discovery resource semantics belong to the
    /// execution compiler binding, not the schema snapshot. Keeping the two identities separate
    /// allows discovery to propose those semantics without invalidating the snapshot it produced.
    pub fn discovery_binding_hash(&self) -> Result<String> {
        self.validate()?;
        artifact_hash(&serde_json::json!({
            "driver": self.driver,
            "resource_id": self.descriptor.resource_id,
            "type_policy_allowances": self.type_policy_allowances,
            "redacted_options": self.redacted_options,
            "redacted_options_hash": self.redacted_options_hash,
            "physical_plan": self.physical_plan,
            "physical_plan_hash": self.physical_plan_hash,
        }))
    }
}

fn error_kind_code(kind: &ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Transient => "transient",
        ErrorKind::RateLimited => "rate_limited",
        ErrorKind::Auth => "auth",
        ErrorKind::Contract => "contract",
        ErrorKind::Data => "data",
        ErrorKind::Destination => "destination",
        ErrorKind::Internal => "internal",
    }
}

fn validate_compiled_source_artifact(
    value: &serde_json::Value,
    label: &str,
    depth: usize,
) -> Result<()> {
    const MAX_DEPTH: usize = 64;
    if depth > MAX_DEPTH {
        return Err(CdfError::contract(format!(
            "{label} exceeds the {MAX_DEPTH}-level nesting boundary"
        )));
    }
    match value {
        serde_json::Value::Object(object) => {
            for (key, value) in object {
                if compiled_source_key_is_sensitive(key)
                    && !matches!(value, serde_json::Value::Null)
                    && value
                        .as_str()
                        .is_none_or(|text| text != "<redacted>" && !text.starts_with("secret://"))
                {
                    return Err(CdfError::contract(format!(
                        "{label} field `{key}` must contain a secret:// reference or redacted marker"
                    )));
                }
                validate_compiled_source_artifact(value, label, depth + 1)?;
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                validate_compiled_source_artifact(value, label, depth + 1)?;
            }
        }
        serde_json::Value::String(text) if text.starts_with("secret://") => {
            cdf_http::SecretUri::new(text.clone())?;
        }
        serde_json::Value::String(text) if text.contains("://") => {
            let url = url::Url::parse(text).map_err(|_| {
                CdfError::contract(format!("{label} contains a malformed absolute URI"))
            })?;
            if !url.username().is_empty()
                || url.password().is_some()
                || url.query().is_some()
                || url.fragment().is_some()
            {
                return Err(CdfError::contract(format!(
                    "{label} URI must not contain user information, query parameters, or a fragment"
                )));
            }
        }
        _ => {}
    }
    Ok(())
}

fn compiled_source_key_is_sensitive(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace(['-', '.'], "_");
    [
        "authorization",
        "credential",
        "password",
        "secret",
        "token",
        "api_key",
        "cookie",
        "connection",
        "dsn",
        "private_key",
        "access_key",
        "session_key",
    ]
    .iter()
    .any(|sensitive| normalized.contains(sensitive))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceDiscoveryKind {
    SchemaMetadata,
    BoundedContent,
    FullContent,
}

#[derive(Clone, Debug)]
pub struct SourceDiscoveryRequest {
    pub maximum_bytes: u64,
    pub maximum_records: u64,
    pub cancellation: crate::RunCancellation,
}

impl SourceDiscoveryRequest {
    pub fn new(maximum_bytes: u64, maximum_records: u64) -> Result<Self> {
        let request = Self {
            maximum_bytes,
            maximum_records,
            cancellation: crate::RunCancellation::default(),
        };
        request.validate()?;
        Ok(request)
    }

    pub fn with_cancellation(mut self, cancellation: crate::RunCancellation) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub fn validate(&self) -> Result<()> {
        if self.maximum_bytes == 0 || self.maximum_records == 0 {
            return Err(CdfError::contract(
                "source discovery byte and record limits must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SourceEvidenceLocation(String);

impl SourceEvidenceLocation {
    pub fn from_operational(value: &str) -> Result<Self> {
        if value.is_empty() || value.chars().any(char::is_control) {
            return Err(CdfError::contract(
                "source evidence location requires a nonempty control-free value",
            ));
        }
        let without_fragment = value.split('#').next().unwrap_or(value);
        let (base, had_query) = without_fragment
            .split_once('?')
            .map_or((without_fragment, false), |(base, _)| (base, true));
        let redacted_base = redact_uri_userinfo(base);
        let redacted = if had_query {
            format!("{redacted_base}?<redacted>")
        } else {
            redacted_base
        };
        Ok(Self(redacted))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn redact_uri_userinfo(value: &str) -> String {
    let Some((scheme, remainder)) = value.split_once("://") else {
        return value.to_owned();
    };
    let authority_end = remainder.find('/').unwrap_or(remainder.len());
    let (authority, suffix) = remainder.split_at(authority_end);
    let authority = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    format!("{scheme}://{authority}{suffix}")
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceDiscoveryCandidate {
    pub canonical_location: String,
    pub evidence_location: SourceEvidenceLocation,
    pub size_bytes: Option<u64>,
    pub modified_at_ms: Option<i64>,
    pub identity: BTreeMap<String, String>,
}

impl SourceDiscoveryCandidate {
    pub fn new(
        canonical_location: impl Into<String>,
        size_bytes: Option<u64>,
        modified_at_ms: Option<i64>,
        identity: BTreeMap<String, String>,
    ) -> Result<Self> {
        let canonical_location = canonical_location.into();
        let evidence_location = SourceEvidenceLocation::from_operational(&canonical_location)?;
        let candidate = Self {
            canonical_location,
            evidence_location,
            size_bytes,
            modified_at_ms,
            identity,
        };
        candidate.validate()?;
        Ok(candidate)
    }

    pub fn validate(&self) -> Result<()> {
        if self.canonical_location.is_empty()
            || self.canonical_location.chars().any(char::is_control)
        {
            return Err(CdfError::contract(
                "source discovery candidate requires a nonempty control-free canonical location",
            ));
        }
        if SourceEvidenceLocation::from_operational(&self.canonical_location)?
            != self.evidence_location
        {
            return Err(CdfError::contract(
                "source discovery candidate evidence location does not match its canonical redaction",
            ));
        }
        validate_source_evidence_identity(&self.identity)?;
        Ok(())
    }

    /// Framework-owned binding between inventory evidence and a later schema observation.
    /// The raw operational location is never retained: its hash plus the canonical redacted
    /// location, generation, size, and time reject cross-candidate observations without
    /// persisting credentials.
    pub fn discovery_binding(&self) -> Result<String> {
        self.validate()?;
        artifact_hash(&serde_json::json!({
            "version": 1,
            "operational_location_hash": artifact_hash(&self.canonical_location)?,
            "location": self.evidence_location.as_str(),
            "size_bytes": self.size_bytes,
            "modified_at_ms": self.modified_at_ms,
            "identity": self.identity,
        }))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct SourceSchemaObservation {
    pub evidence_location: SourceEvidenceLocation,
    pub schema: Schema,
    pub physical_schema_hash: cdf_kernel::SchemaHash,
    pub source_identity: BTreeMap<String, String>,
    pub bytes_read: u64,
    pub records_read: u64,
    pub(crate) candidate_binding: String,
}

impl SourceSchemaObservation {
    pub fn new(
        candidate: &SourceDiscoveryCandidate,
        schema: Schema,
        source_identity: BTreeMap<String, String>,
        bytes_read: u64,
        records_read: u64,
    ) -> Result<Self> {
        candidate.validate()?;
        let physical_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&schema)?;
        let observation = Self {
            evidence_location: candidate.evidence_location.clone(),
            schema,
            physical_schema_hash,
            source_identity,
            bytes_read,
            records_read,
            candidate_binding: candidate.discovery_binding()?,
        };
        observation.validate()?;
        Ok(observation)
    }

    pub fn validate(&self) -> Result<()> {
        if self.evidence_location.as_str().is_empty()
            || validate_source_evidence_identity(&self.source_identity).is_err()
            || validate_hash(
                "source discovery candidate binding",
                &self.candidate_binding,
            )
            .is_err()
            || cdf_kernel::canonical_arrow_schema_hash(&self.schema)? != self.physical_schema_hash
        {
            return Err(CdfError::contract(
                "source schema observation has invalid canonical identity or does not match its physical schema hash",
            ));
        }
        Ok(())
    }

    pub fn candidate_binding(&self) -> &str {
        &self.candidate_binding
    }
}

pub fn validate_source_evidence_identity(identity: &BTreeMap<String, String>) -> Result<()> {
    for (key, value) in identity {
        if key.is_empty()
            || key.chars().any(char::is_control)
            || value.is_empty()
            || value.chars().any(char::is_control)
            || compiled_source_key_is_sensitive(key)
            || value.starts_with("secret://")
        {
            return Err(CdfError::contract(
                "source evidence identity contains an invalid or sensitive key or value",
            ));
        }
        if value.contains("://")
            && (value.split_whitespace().count() != 1
                || SourceEvidenceLocation::from_operational(value)?.as_str() != value)
        {
            return Err(CdfError::contract(
                "source evidence identity contains an unredacted or mixed operational URI",
            ));
        }
    }
    Ok(())
}

pub trait SourceDiscoverySession: Send + Sync {
    fn kind(&self) -> SourceDiscoveryKind;
    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>>;
    fn observe(
        &self,
        candidate: &SourceDiscoveryCandidate,
        request: &SourceDiscoveryRequest,
    ) -> Result<SourceSchemaObservation>;
}

#[derive(Clone)]
pub struct SourceResolutionContext<'a> {
    project_root: &'a Path,
    secret_provider: Arc<dyn SecretProvider + Send + Sync>,
    execution: &'a ExecutionServices,
    egress_authorizer: Arc<dyn SourceEgressAuthorizer>,
    prepared_payloads: PreparedSourcePayloads,
    driver_options: BTreeMap<String, serde_json::Value>,
    cancellation: crate::RunCancellation,
}

impl<'a> SourceResolutionContext<'a> {
    pub fn new(
        project_root: &'a Path,
        secret_provider: Arc<dyn SecretProvider + Send + Sync>,
        execution: &'a ExecutionServices,
        egress_authorizer: Arc<dyn SourceEgressAuthorizer>,
    ) -> Self {
        Self {
            project_root,
            secret_provider,
            execution,
            egress_authorizer,
            prepared_payloads: PreparedSourcePayloads::default(),
            driver_options: BTreeMap::new(),
            cancellation: crate::RunCancellation::default(),
        }
    }

    pub fn with_prepared_payloads(mut self, prepared_payloads: PreparedSourcePayloads) -> Self {
        self.prepared_payloads = prepared_payloads;
        self
    }

    pub fn with_driver_options(
        mut self,
        driver_options: BTreeMap<String, serde_json::Value>,
    ) -> Self {
        self.driver_options = driver_options;
        self
    }

    pub fn with_cancellation(mut self, cancellation: crate::RunCancellation) -> Self {
        self.cancellation = cancellation;
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

    pub fn cancellation(&self) -> crate::RunCancellation {
        self.cancellation.clone()
    }

    pub fn egress_scope(&self, driver_id: &SourceDriverId) -> SourceEgressScope {
        SourceEgressScope::new(driver_id.clone(), Arc::clone(&self.egress_authorizer))
    }

    pub fn prepared_payloads(&self) -> &PreparedSourcePayloads {
        &self.prepared_payloads
    }

    pub fn driver_options(&self, driver_id: &SourceDriverId) -> Option<&serde_json::Value> {
        self.driver_options.get(driver_id.as_str())
    }
}

pub trait SourceDriver: Send + Sync {
    fn descriptor(&self) -> &SourceDriverDescriptor;
    fn option_schema(&self) -> &serde_json::Value;
    fn validate_project_options(&self, options: &serde_json::Value) -> Result<()> {
        match options.as_object() {
            Some(options) if options.is_empty() => Ok(()),
            Some(_) => Err(CdfError::contract(format!(
                "source driver `{}` does not accept project-level options",
                self.descriptor().driver_id.as_str()
            ))),
            None => Err(CdfError::contract(
                "source driver project options must be a JSON object",
            )),
        }
    }
    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan>;
    /// Validates the driver-owned portion of a compiled plan for isolated execution.
    ///
    /// Drivers must opt in because only the owner can distinguish portable source identifiers
    /// (for example an HTTP path) from coordinator-local paths or opaque host state. The default
    /// fails closed while allowing newly added drivers to compile before they claim support for
    /// the portable-worker protocol.
    fn validate_portable_plan(&self, plan: &CompiledSourcePlan) -> Result<()> {
        plan.validate()?;
        Err(CdfError::contract(format!(
            "source driver `{}` has not declared portable-plan validation",
            self.descriptor().driver_id.as_str()
        )))
    }
    fn reference_compiler(&self) -> Option<&dyn SourceReferenceCompiler> {
        None
    }
    /// Runs this driver's bounded, redacted health checks for the exact plans it owns.
    ///
    /// The registry groups and validates plans before calling this method. Network, secret, and
    /// runtime authority remain host-injected through `context`; a driver cannot manufacture a
    /// second execution environment for doctor.
    fn health(
        &self,
        request: SourceHealthRequest,
        context: &SourceResolutionContext<'_>,
        output: &mut dyn SourceHealthSink,
    ) -> Result<()>;
    fn add_planner(&self) -> Option<&dyn crate::SourceAddPlanner> {
        None
    }
    fn discovery_session(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Box<dyn SourceDiscoverySession>>;
    /// Resolves a portable blocking-lane ceiling against the concrete execution runtime.
    ///
    /// The default is the recorded declaration. Drivers whose safe concurrency depends on a
    /// runtime fact (for example an attached foreign interpreter) may only tighten that
    /// declaration; the registry validates the refinement before installing the lane.
    fn resolve_blocking_lane(
        &self,
        plan: &CompiledSourcePlan,
        _context: &SourceResolutionContext<'_>,
    ) -> Result<Option<BlockingLaneSpec>> {
        Ok(plan.execution_capabilities.blocking_lane.clone())
    }
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
