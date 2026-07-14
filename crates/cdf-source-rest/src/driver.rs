use std::{collections::BTreeMap, sync::Arc};

use cdf_http::{
    AuthScheme, EgressAllowlist, HttpTransport, PaginationConfig, QuotaHeaderPolicy,
    RateLimitPolicy, ResetHeaderSemantics, SecretUri,
};
use cdf_kernel::{
    BackpressureSupport, CapabilitySupport, CdfError, EstimateSupport, FilterCapabilities,
    IncrementalShape, PartitioningCapabilities, PushdownFidelity, QueryableResource, ReplaySupport,
    ResourceCapabilities, Result, ScopeKind,
};
use cdf_runtime::{
    BlockingLaneSpec, CompiledSourcePlan, InterruptionSafety, LaneAffinity,
    SourceAttestationStrength, SourceCompileRequest, SourceDriver, SourceDriverDescriptor,
    SourceDriverId, SourceExecutionCapabilities, SourceExecutorClass, SourceResolutionContext,
    SourceRetryGranularity, artifact_hash,
};
use serde::{Deserialize, Serialize};

use crate::{RestResource, RestResourcePlan, RestRuntimeDependencies};

type TransportFactory = dyn Fn() -> Result<Box<dyn HttpTransport>> + Send + Sync + 'static;

#[derive(Clone)]
pub struct RestSourceDriver {
    descriptor: SourceDriverDescriptor,
    transport_factory: Arc<TransportFactory>,
}

impl std::fmt::Debug for RestSourceDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RestSourceDriver")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

impl RestSourceDriver {
    pub fn new<F>(transport_factory: F) -> Result<Self>
    where
        F: Fn() -> Result<Box<dyn HttpTransport>> + Send + Sync + 'static,
    {
        let option_schema = serde_json::json!({
            "source": ["source_name", "base_url", "auth", "rate_limit", "egress_allowlist"],
            "resource": ["path", "params", "paginate", "records", "records_transform", "cursor_param", "cursor_filter_fidelity"]
        });
        Ok(Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new("rest")?,
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema)?,
                kinds: vec!["rest".to_owned()],
                schemes: vec!["rest+http".to_owned(), "rest+https".to_owned()],
            },
            transport_factory: Arc::new(transport_factory),
        })
    }
}

impl SourceDriver for RestSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        let source: RestSourceOptions = decode_options("REST source", request.source_options)?;
        let resource: RestResourceOptions =
            decode_options("REST resource", request.resource_options)?;
        let physical = RestPhysicalPlan { source, resource };
        physical.to_runtime_plan()?;
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            rest_capabilities(&request.descriptor),
            execution_capabilities(),
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                effective_schema_runtime: request.effective_schema_runtime,
                redacted_options: serde_json::to_value(&physical).map_err(serialize_error)?,
                physical_plan: serde_json::to_value(&physical).map_err(serialize_error)?,
            },
        )
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        let physical: RestPhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid REST source plan: {error}")))?;
        let runtime_plan = physical.to_runtime_plan()?;
        let transport = (self.transport_factory)()?;
        let dependencies = RestRuntimeDependencies::from_boxed_transport(transport)
            .with_shared_secret_provider(Arc::clone(context.secret_provider()))
            .with_execution_services(context.execution().clone())
            .with_prepared_payloads(context.prepared_payloads().clone());
        Ok(Arc::new(RestResource::new(
            plan.descriptor.clone(),
            Arc::new(plan.schema.clone()),
            plan.resource_capabilities.clone(),
            runtime_plan,
            plan.type_policy_allowances,
            dependencies,
        )?))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestSourceOptions {
    source_name: String,
    base_url: String,
    #[serde(default)]
    auth: Option<AuthOptions>,
    #[serde(default)]
    rate_limit: Option<RateLimitOptions>,
    #[serde(default)]
    egress_allowlist: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestResourceOptions {
    path: String,
    #[serde(default)]
    params: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    paginate: Option<PaginationOptions>,
    records: String,
    #[serde(default)]
    records_transform: Option<String>,
    #[serde(default)]
    cursor_param: Option<String>,
    #[serde(default = "default_cursor_fidelity")]
    cursor_filter_fidelity: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum AuthOptions {
    Bearer { token: String },
    Header { name: String, value: String },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RateLimitOptions {
    #[serde(default)]
    requests_per_minute: Option<u32>,
    #[serde(default)]
    respect_headers: Vec<String>,
    #[serde(default)]
    quota_headers: Vec<QuotaOptions>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuotaOptions {
    remaining_header: String,
    reset_header: String,
    reset: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum PaginationOptions {
    LinkHeader,
    CursorParam {
        query_param: String,
        response_field: String,
        initial: Option<String>,
    },
    PageNumber {
        query_param: String,
        start_page: Option<u64>,
    },
    Offset {
        offset_param: String,
        limit_param: String,
        start_offset: Option<u64>,
        limit: u64,
    },
    NextToken {
        query_param: String,
        response_field: String,
        initial: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestPhysicalPlan {
    source: RestSourceOptions,
    resource: RestResourceOptions,
}

impl RestPhysicalPlan {
    fn to_runtime_plan(&self) -> Result<RestResourcePlan> {
        let auth = self
            .source
            .auth
            .as_ref()
            .map(AuthOptions::to_runtime)
            .transpose()?;
        let rate = self.source.rate_limit.clone().unwrap_or_default();
        let pagination = self
            .resource
            .paginate
            .as_ref()
            .map(PaginationOptions::to_runtime);
        let cursor_filter_fidelity = match self.resource.cursor_filter_fidelity.as_str() {
            "exact" => PushdownFidelity::Exact,
            "inexact" => PushdownFidelity::Inexact,
            "unsupported" => PushdownFidelity::Unsupported,
            value => {
                return Err(CdfError::contract(format!(
                    "REST cursor filter fidelity `{value}` is unsupported"
                )));
            }
        };
        Ok(RestResourcePlan {
            source: self.source.source_name.clone(),
            base_url: self.source.base_url.clone(),
            path: self.resource.path.clone(),
            params: self
                .resource
                .params
                .iter()
                .map(|(key, value)| scalar_param(key, value).map(|value| (key.clone(), value)))
                .collect::<Result<_>>()?,
            record_selector: self.resource.records.clone(),
            pagination,
            auth,
            rate_limit: RateLimitPolicy {
                requests_per_minute: rate.requests_per_minute,
                quota_headers: rate
                    .quota_headers
                    .iter()
                    .map(QuotaOptions::to_runtime)
                    .collect::<Result<_>>()?,
            },
            respect_headers: rate.respect_headers,
            allowlist: if self.source.egress_allowlist.is_empty() {
                EgressAllowlist::allow_any()
            } else {
                EgressAllowlist::from_hosts(self.source.egress_allowlist.clone())
            },
            cursor_param: self.resource.cursor_param.clone(),
            cursor_filter_fidelity,
            records_transform: self.resource.records_transform.clone(),
        })
    }
}

impl AuthOptions {
    fn to_runtime(&self) -> Result<AuthScheme> {
        match self {
            Self::Bearer { token } => Ok(AuthScheme::Bearer {
                token_uri: SecretUri::new(token.clone())?,
            }),
            Self::Header { name, value } => Ok(AuthScheme::Header {
                name: name.clone(),
                value_uri: SecretUri::new(value.clone())?,
            }),
        }
    }
}

impl QuotaOptions {
    fn to_runtime(&self) -> Result<QuotaHeaderPolicy> {
        let reset = match self.reset.as_str() {
            "delay_seconds" => ResetHeaderSemantics::DelaySeconds,
            "epoch_seconds" => ResetHeaderSemantics::EpochSeconds,
            value => {
                return Err(CdfError::contract(format!(
                    "REST quota reset semantics `{value}` are unsupported"
                )));
            }
        };
        Ok(QuotaHeaderPolicy::remaining_until_reset(
            self.remaining_header.clone(),
            self.reset_header.clone(),
            reset,
        ))
    }
}

impl PaginationOptions {
    fn to_runtime(&self) -> PaginationConfig {
        match self {
            Self::LinkHeader => PaginationConfig::LinkHeader,
            Self::CursorParam {
                query_param,
                response_field,
                initial,
            } => PaginationConfig::Cursor {
                query_param: query_param.clone(),
                response_field: response_field.clone(),
                initial: initial.clone(),
            },
            Self::PageNumber {
                query_param,
                start_page,
            } => PaginationConfig::Page {
                query_param: query_param.clone(),
                start_page: start_page.unwrap_or(1),
            },
            Self::Offset {
                offset_param,
                limit_param,
                start_offset,
                limit,
            } => PaginationConfig::Offset {
                offset_param: offset_param.clone(),
                limit_param: limit_param.clone(),
                start_offset: start_offset.unwrap_or(0),
                limit: *limit,
            },
            Self::NextToken {
                query_param,
                response_field,
                initial,
            } => PaginationConfig::NextToken {
                query_param: query_param.clone(),
                response_field: response_field.clone(),
                initial: initial.clone(),
            },
        }
    }
}

fn decode_options<T: for<'de> Deserialize<'de>>(
    label: &str,
    options: BTreeMap<String, serde_json::Value>,
) -> Result<T> {
    serde_json::from_value(serde_json::Value::Object(options.into_iter().collect()))
        .map_err(|error| CdfError::contract(format!("{label} options are invalid: {error}")))
}

fn serialize_error(error: serde_json::Error) -> CdfError {
    CdfError::internal(format!("serialize REST source plan: {error}"))
}

fn scalar_param(name: &str, value: &serde_json::Value) -> Result<String> {
    match value {
        serde_json::Value::String(value) => Ok(value.clone()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        _ => Err(CdfError::contract(format!(
            "REST parameter `{name}` must be a string, number, or boolean"
        ))),
    }
}

fn default_cursor_fidelity() -> String {
    "inexact".to_owned()
}

fn rest_capabilities(descriptor: &cdf_kernel::ResourceDescriptor) -> ResourceCapabilities {
    ResourceCapabilities {
        projection: CapabilitySupport::Unsupported,
        filters: FilterCapabilities {
            default_fidelity: PushdownFidelity::Unsupported,
            supported_operators: if descriptor.cursor.is_some() {
                vec![">".to_owned(), ">=".to_owned(), "=".to_owned()]
            } else {
                Vec::new()
            },
        },
        limits: CapabilitySupport::Unsupported,
        ordering: CapabilitySupport::Unsupported,
        partitioning: match descriptor.state_scope.kind() {
            ScopeKind::Resource => PartitioningCapabilities::default(),
            kind => PartitioningCapabilities {
                parallel_partitions: true,
                supported_scopes: vec![kind],
            },
        },
        incremental: if descriptor.cursor.is_some() {
            IncrementalShape::Cursor
        } else {
            IncrementalShape::Full
        },
        replay: if descriptor.cursor.is_some() {
            ReplaySupport::FromPosition
        } else {
            ReplaySupport::None
        },
        idempotent_reads: true,
        backpressure: BackpressureSupport::CannotPause,
        estimates: EstimateSupport::None,
    }
}

fn execution_capabilities() -> SourceExecutionCapabilities {
    SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: 32 * 1024 * 1024,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: 32 * 1024 * 1024,
        maximum_concurrency: 8,
        useful_concurrency: 8,
        executor_class: SourceExecutorClass::BlockingLane,
        blocking_lane: Some(BlockingLaneSpec {
            lane_id: "rest-source.sync".to_owned(),
            maximum_concurrency: 8,
            cpu_slot_cost: 1,
            native_internal_parallelism: 1,
            affinity: LaneAffinity::Shared,
            interruption: InterruptionSafety::CooperativeOnly,
        }),
        pausable: false,
        spillable: false,
        idempotent_reads: true,
        reopenable: true,
        resumable: true,
        speculative_safe: false,
        retry_granularity: SourceRetryGranularity::Unit,
        retryable_errors: vec![
            cdf_kernel::ErrorKind::Transient,
            cdf_kernel::ErrorKind::RateLimited,
        ],
        attestation: SourceAttestationStrength::Metadata,
        rate_limit_per_second: None,
        quota_authority: Some("origin".to_owned()),
        canonical_order: true,
        bounded: true,
        telemetry_version: "v1".to_owned(),
    }
}
