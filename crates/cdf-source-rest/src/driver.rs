use std::{collections::BTreeMap, sync::Arc};

use cdf_http::{
    AuthScheme, EgressAllowlist, HttpTransport, PaginationConfig, QuotaHeaderPolicy,
    RateLimitPolicy, ResetHeaderSemantics, SecretUri,
};
use cdf_kernel::{CdfError, PushdownFidelity, QueryableResource, Result, ScanRequest};
use cdf_runtime::{
    BlockingLaneSpec, CompiledSourcePlan, InterruptionSafety, LaneAffinity, SourceAddCursor,
    SourceAddCursorOrdering, SourceAddPlanner, SourceAddProposal, SourceAddRequest,
    SourceAttestationStrength, SourceCompileRequest, SourceCursorPushdown,
    SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest, SourceDiscoverySession,
    SourceDriver, SourceDriverDescriptor, SourceDriverId, SourceEvidenceLocation,
    SourceExecutionCapabilities, SourceExecutorClass, SourceHealthRequest, SourceHealthResult,
    SourceHealthStatus, SourceRateLimit, SourceResolutionContext, SourceRetryGranularity,
    SourceSchemaObservation, artifact_hash,
};
use serde::{Deserialize, Serialize};

use crate::{
    RestDiscoveryDependencies, RestResource, RestResourcePlan, RestRuntimeDependencies,
    discover_rest_sample_schema, rest_partition, rest_resource_capabilities,
};

type TransportFactory = dyn Fn() -> Result<Box<dyn HttpTransport>> + Send + Sync + 'static;

#[derive(Clone)]
pub struct RestSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
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
        let option_schema = option_schema();
        Ok(Self {
            descriptor: SourceDriverDescriptor {
                driver_id: SourceDriverId::new("rest")?,
                driver_version: "1.0.0".to_owned(),
                option_schema_hash: artifact_hash(&option_schema)?,
                kinds: vec!["rest".to_owned()],
                schemes: vec!["rest+http".to_owned(), "rest+https".to_owned()],
            },
            option_schema,
            transport_factory: Arc::new(transport_factory),
        })
    }
}

impl SourceDriver for RestSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn add_planner(&self) -> Option<&dyn SourceAddPlanner> {
        Some(self)
    }

    fn health(
        &self,
        request: SourceHealthRequest,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Vec<SourceHealthResult>> {
        if request.compiled_plans.is_empty() {
            return Ok(vec![SourceHealthResult {
                probe_id: "request".to_owned(),
                status: SourceHealthStatus::Skipped,
                message: "no REST resources are compiled".to_owned(),
                details: serde_json::json!({"resources": 0}),
            }]);
        }
        let probe_request = SourceDiscoveryRequest::new(1024 * 1024, 1)?;
        let health_context = context
            .clone()
            .with_prepared_payloads(cdf_runtime::PreparedSourcePayloads::default());
        Ok(request
            .compiled_plans
            .iter()
            .map(|plan| {
                let resource_id = plan.descriptor.resource_id.as_str();
                let probe = self
                    .discovery_session(plan, &health_context)
                    .and_then(|session| {
                        let candidates = session.candidates()?;
                        let candidate = candidates.first().ok_or_else(|| {
                            CdfError::data("REST health probe produced no discovery candidate")
                        })?;
                        session.observe(candidate, &probe_request)
                    });
                match probe {
                    Ok(observation) => SourceHealthResult {
                        probe_id: resource_id.to_owned(),
                        status: SourceHealthStatus::Passed,
                        message: "REST endpoint probe passed".to_owned(),
                        details: serde_json::json!({
                            "resource_id": resource_id,
                            "bytes_read": observation.bytes_read,
                            "records_read": observation.records_read,
                        }),
                    },
                    Err(error) => SourceHealthResult {
                        probe_id: resource_id.to_owned(),
                        status: SourceHealthStatus::Failed,
                        message: "REST endpoint probe failed".to_owned(),
                        details: serde_json::json!({
                            "resource_id": resource_id,
                            "error": error.to_string(),
                        }),
                    },
                }
            })
            .collect())
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        let source_name = request.context.source_name.clone();
        let cursor_pushdown = request.context.cursor_pushdown.clone();
        let source: RestSourceOptions = decode_options("REST source", request.source_options)?;
        let resource: RestResourceOptions =
            decode_options("REST resource", request.resource_options)?;
        let physical = RestPhysicalPlan {
            source_name,
            cursor_pushdown,
            source,
            resource,
        };
        let runtime_plan = physical.to_runtime_plan()?;
        let capabilities = rest_resource_capabilities(&request.descriptor, &runtime_plan);
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            capabilities,
            execution_capabilities(&runtime_plan)?,
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog: request.baseline_observation_schema_catalog,
                redacted_options: serde_json::to_value(&physical).map_err(serialize_error)?,
                physical_plan: serde_json::to_value(&physical).map_err(serialize_error)?,
            },
        )
    }

    fn discovery_session(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Box<dyn SourceDiscoverySession>> {
        plan.validate()?;
        let physical: RestPhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid REST source plan: {error}")))?;
        let runtime_plan = physical.to_runtime_plan()?;
        validate_compiled_capabilities(plan, &runtime_plan)?;
        Ok(Box::new(RestDriverDiscoverySession {
            descriptor: plan.descriptor.clone(),
            plan: runtime_plan,
            transport: Arc::from((self.transport_factory)()?),
            secret_provider: Arc::clone(context.secret_provider()),
            memory: context.execution().memory(),
            prepared_payloads: context.prepared_payloads().clone(),
            execution: context.execution().clone(),
            egress: context.egress_scope(&plan.driver.driver_id),
        }))
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        plan.validate()?;
        let physical: RestPhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid REST source plan: {error}")))?;
        let runtime_plan = physical.to_runtime_plan()?;
        validate_compiled_capabilities(plan, &runtime_plan)?;
        let transport = (self.transport_factory)()?;
        let dependencies = RestRuntimeDependencies::from_boxed_transport(
            transport,
            context.execution().clone(),
            context.egress_scope(&plan.driver.driver_id),
        )
        .with_shared_secret_provider(Arc::clone(context.secret_provider()))
        .with_prepared_payloads(context.prepared_payloads().clone());
        Ok(Arc::new(
            RestResource::new(
                plan.descriptor.clone(),
                Arc::new(plan.schema.clone()),
                plan.resource_capabilities.clone(),
                runtime_plan,
                plan.type_policy_allowances,
                dependencies,
            )?
            .with_effective_schema_runtime(plan.effective_schema_runtime.clone())
            .with_compiled_source_plan_hash(cdf_runtime::artifact_hash(plan)?),
        ))
    }
}

impl SourceAddPlanner for RestSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        request.validate()?;
        const KEYS: [&str; 3] = ["records", "cursor", "cursor_param"];
        if !request
            .options
            .keys()
            .any(|key| KEYS.contains(&key.as_str()))
        {
            return Ok(None);
        }
        let unknown = request
            .options
            .keys()
            .filter(|key| !KEYS.contains(&key.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Err(CdfError::contract(format!(
                "REST cdf add received unknown options: {}",
                unknown.join(", ")
            )));
        }
        let missing = KEYS
            .iter()
            .filter(|key| !request.options.contains_key(**key))
            .copied()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(CdfError::contract(
                "REST cdf add requires options `records`, `cursor`, and `cursor_param` together",
            ));
        }
        let parsed = url::Url::parse(&request.location).map_err(|error| {
            CdfError::contract(format!("cdf add could not parse REST URL: {error}"))
        })?;
        match parsed.scheme() {
            "https" => {}
            "http" if is_loopback(&parsed) => {}
            scheme => {
                return Err(CdfError::contract(format!(
                    "cdf add REST endpoints require HTTPS or loopback HTTP; `{scheme}` is not supported"
                )));
            }
        }
        if !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(CdfError::contract(
                "cdf add REST URL must not contain userinfo, query secrets, or fragments; declare stable parameters and authentication in source configuration",
            ));
        }
        let host = parsed
            .host_str()
            .ok_or_else(|| CdfError::contract("cdf add REST URL must contain a host"))?
            .to_owned();
        let path = if parsed.path().is_empty() {
            "/".to_owned()
        } else {
            parsed.path().to_owned()
        };
        let mut origin = parsed;
        origin.set_path("");
        let base_url = origin.as_str().trim_end_matches('/').to_owned();
        let records = request.options["records"].clone();
        let cursor = request.options["cursor"].clone();
        let cursor_param = request.options["cursor_param"].clone();
        Ok(Some(SourceAddProposal {
            source_kind: "rest".to_owned(),
            source_options: BTreeMap::from([
                (
                    "base_url".to_owned(),
                    serde_json::Value::String(base_url.clone()),
                ),
                ("egress_allowlist".to_owned(), serde_json::json!([host])),
            ]),
            resource_options: BTreeMap::from([
                ("path".to_owned(), serde_json::Value::String(path.clone())),
                ("records".to_owned(), serde_json::Value::String(records)),
            ]),
            cursor: Some(SourceAddCursor {
                field: cursor,
                parameter: Some(cursor_param),
                ordering: SourceAddCursorOrdering::BestEffort,
                lag_tolerance_ms: 0,
            }),
            display_location: SourceEvidenceLocation::from_operational(&base_url)?,
            display_selection: path,
            private_files: Vec::new(),
        }))
    }
}

fn is_loopback(url: &url::Url) -> bool {
    matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"))
}

fn validate_compiled_capabilities(
    plan: &CompiledSourcePlan,
    runtime_plan: &RestResourcePlan,
) -> Result<()> {
    let expected = rest_resource_capabilities(&plan.descriptor, runtime_plan);
    if plan.resource_capabilities != expected {
        return Err(CdfError::contract(
            "compiled REST resource capabilities do not match the executable cursor plan; recompile the source plan",
        ));
    }
    Ok(())
}

struct RestDriverDiscoverySession {
    descriptor: cdf_kernel::ResourceDescriptor,
    plan: RestResourcePlan,
    transport: Arc<dyn HttpTransport>,
    secret_provider: Arc<dyn cdf_http::SecretProvider + Send + Sync>,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    prepared_payloads: cdf_runtime::PreparedSourcePayloads,
    execution: cdf_runtime::ExecutionServices,
    egress: cdf_runtime::SourceEgressScope,
}

impl SourceDiscoverySession for RestDriverDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        SourceDiscoveryKind::BoundedContent
    }

    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(vec![SourceDiscoveryCandidate::new(
            self.descriptor.resource_id.as_str(),
            None,
            None,
            BTreeMap::from([
                ("source_kind".to_owned(), "rest".to_owned()),
                ("path".to_owned(), self.plan.path.clone()),
            ]),
        )?])
    }

    fn observe(
        &self,
        candidate: &SourceDiscoveryCandidate,
        request: &SourceDiscoveryRequest,
    ) -> Result<SourceSchemaObservation> {
        let descriptor = self.descriptor.clone();
        let plan = self.plan.clone();
        let transport = Arc::clone(&self.transport);
        let secret_provider = Arc::clone(&self.secret_provider);
        let memory = Arc::clone(&self.memory);
        let prepared_payloads = self.prepared_payloads.clone();
        let egress = self.egress.clone();
        let candidate = candidate.clone();
        let request = request.clone();
        self.execution.run_blocking("rest-source.sync", move || {
            if candidate.canonical_location != descriptor.resource_id.as_str() {
                return Err(CdfError::contract(format!(
                    "REST discovery candidate `{}` does not match compiled resource `{}`",
                    candidate.canonical_location, descriptor.resource_id
                )));
            }
            let partition = rest_partition(
                &descriptor,
                &plan,
                &ScanRequest {
                    resource_id: descriptor.resource_id.clone(),
                    projection: None,
                    filters: Vec::new(),
                    limit: None,
                    order_by: Vec::new(),
                    scope: descriptor.state_scope.clone(),
                },
            )?;
            let dependencies = RestDiscoveryDependencies::new(
                transport.as_ref(),
                secret_provider.as_ref(),
                memory,
                egress,
            )
            .with_prepared_payloads(prepared_payloads);
            let discovery = discover_rest_sample_schema(
                &descriptor,
                &plan,
                &partition,
                &dependencies,
                &request,
            )?;
            SourceSchemaObservation::new(
                &candidate,
                discovery.schema.as_ref().clone(),
                discovery.source_identity,
                discovery.bytes_read,
                discovery.records_read,
            )
        })
    }
}

fn option_schema() -> serde_json::Value {
    let auth = serde_json::json!({
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "token"],
                "properties": {
                    "kind": {"const": "bearer"},
                    "token": {"type": "string", "pattern": "^secret://"}
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "name", "value"],
                "properties": {
                    "kind": {"const": "header"},
                    "name": {"type": "string", "minLength": 1},
                    "value": {"type": "string", "pattern": "^secret://"}
                }
            }
        ]
    });
    let quota = serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["remaining_header", "reset_header", "reset"],
        "properties": {
            "remaining_header": {"type": "string", "minLength": 1},
            "reset_header": {"type": "string", "minLength": 1},
            "reset": {"enum": ["delay_seconds", "epoch_seconds"]}
        }
    });
    let pagination = serde_json::json!({
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind"],
                "properties": {"kind": {"const": "link_header"}}
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "query_param", "response_field"],
                "properties": {
                    "kind": {"enum": ["cursor_param", "next_token"]},
                    "query_param": {"type": "string", "minLength": 1},
                    "response_field": {"type": "string", "minLength": 1},
                    "initial": {"type": ["string", "null"]}
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "query_param"],
                "properties": {
                    "kind": {"const": "page_number"},
                    "query_param": {"type": "string", "minLength": 1},
                    "start_page": {"type": ["integer", "null"], "minimum": 1}
                }
            },
            {
                "type": "object",
                "additionalProperties": false,
                "required": ["kind", "offset_param", "limit_param", "limit"],
                "properties": {
                    "kind": {"const": "offset"},
                    "offset_param": {"type": "string", "minLength": 1},
                    "limit_param": {"type": "string", "minLength": 1},
                    "start_offset": {"type": ["integer", "null"], "minimum": 0},
                    "limit": {"type": "integer", "minimum": 1}
                }
            }
        ]
    });
    serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "source": {
            "type": "object",
            "additionalProperties": false,
            "required": ["base_url"],
            "properties": {
                "base_url": {"type": "string", "format": "uri"},
                "auth": auth,
                "rate_limit": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "requests_per_minute": {"type": ["integer", "null"], "minimum": 1},
                        "respect_headers": {"type": "array", "items": {"type": "string"}, "uniqueItems": true},
                        "quota_headers": {"type": "array", "items": quota}
                    }
                },
                "egress_allowlist": {"type": "array", "items": {"type": "string"}, "uniqueItems": true}
            }
        },
        "resource": {
            "type": "object",
            "additionalProperties": false,
            "required": ["path", "records"],
            "properties": {
                "path": {"type": "string"},
                "params": {"type": "object", "additionalProperties": {"type": ["string", "number", "boolean"]}},
                "paginate": pagination,
                "records": {"type": "string", "minLength": 1},
                "records_transform": {"type": "string", "minLength": 1}
            }
        }
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestSourceOptions {
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
    source_name: String,
    cursor_pushdown: Option<SourceCursorPushdown>,
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
        let (cursor_param, cursor_filter_fidelity) = self
            .cursor_pushdown
            .as_ref()
            .map(|cursor| (cursor.parameter.clone(), cursor.fidelity.clone()))
            .unwrap_or((None, PushdownFidelity::Inexact));
        Ok(RestResourcePlan {
            source: self.source_name.clone(),
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
            cursor_param,
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

fn execution_capabilities(plan: &RestResourcePlan) -> Result<SourceExecutionCapabilities> {
    let quota_authority =
        cdf_runtime::SourceEgressTarget::parse(&plan.base_url)?.canonical_authority();
    Ok(SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: crate::REST_MAXIMUM_BATCH_BYTES,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: crate::REST_MAXIMUM_DECODE_BYTES,
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
        pausable: true,
        spillable: false,
        idempotent_reads: true,
        reopenable: true,
        resumable: true,
        speculative_safe: false,
        retry_granularity: SourceRetryGranularity::None,
        retryable_errors: Vec::new(),
        retry_policy: None,
        attestation: SourceAttestationStrength::None,
        rate_limit: plan
            .rate_limit
            .requests_per_minute
            .map(|operations| SourceRateLimit {
                operations: u64::from(operations),
                interval_ms: 60_000,
            }),
        quota_authority: Some(quota_authority),
        canonical_order: true,
        bounded: true,
        batch_memory: cdf_runtime::SourceBatchMemoryContract::Preaccounted,
        telemetry_version: "v1".to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Schema;
    use cdf_http::{HttpRequest, HttpResponse, SecretProvider, SecretUri, SecretValue};
    use cdf_kernel::{
        CursorOrderingClaim, CursorSpec, ResourceDescriptor, ResourceId, SchemaHash, SchemaSource,
        ScopeKey, TrustLevel, WriteDisposition,
    };
    use cdf_runtime::{
        BlockingLaneSpec, BlockingValueTask, ExecutionHost, ExecutionHostCapabilities,
        ExecutionServices, ExecutionTaskScope, FixedSpillBudget, IoValue, IoValueTask,
        SourceCompileContext, SourceCursorPushdown, SourceRegistry, SpillBudgetCoordinator,
    };

    struct StaticDiscoveryTransport;

    impl HttpTransport for StaticDiscoveryTransport {
        fn send(
            &self,
            _request: HttpRequest,
            budget: cdf_http::HttpResponseBudget,
        ) -> Result<HttpResponse> {
            Ok(HttpResponse::new(200)
                .with_body(budget.account_body(br#"{"items":[{"id":1},{"id":2}]}"#.to_vec())?))
        }
    }

    struct NoopSecretProvider;

    impl SecretProvider for NoopSecretProvider {
        fn resolve(&self, _uri: &SecretUri) -> Result<SecretValue> {
            Err(CdfError::auth(
                "REST discovery test does not resolve secrets",
            ))
        }
    }

    struct ImmediateBlockingHost {
        memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
    }

    impl ExecutionHost for ImmediateBlockingHost {
        fn capabilities(&self) -> ExecutionHostCapabilities {
            ExecutionHostCapabilities {
                logical_cpu_slots: 1,
                io_workers: 1,
                blocking_lanes: Vec::new(),
            }
        }

        fn memory(&self) -> Arc<dyn cdf_memory::MemoryCoordinator> {
            Arc::clone(&self.memory)
        }

        fn spill(&self) -> Arc<dyn SpillBudgetCoordinator> {
            Arc::clone(&self.spill)
        }

        fn open_scope(&self, _run_id: &str) -> Result<Box<dyn ExecutionTaskScope>> {
            Err(CdfError::internal(
                "REST discovery test does not open execution scopes",
            ))
        }

        fn run_io_blocking(&self, _task: IoValueTask) -> Result<IoValue> {
            Err(CdfError::internal(
                "REST discovery test does not execute I/O futures",
            ))
        }

        fn delay(
            &self,
            _duration: std::time::Duration,
            cancellation: cdf_runtime::RunCancellation,
        ) -> cdf_kernel::BoxFuture<'static, Result<()>> {
            Box::pin(async move { cancellation.check() })
        }

        fn monotonic_now(&self) -> std::time::Duration {
            std::time::Duration::ZERO
        }

        fn entropy_u64(&self) -> u64 {
            0
        }

        fn ensure_blocking_lanes(&self, _lanes: &[BlockingLaneSpec]) -> Result<()> {
            Ok(())
        }

        fn run_blocking_value(&self, _lane: &str, task: BlockingValueTask) -> Result<IoValue> {
            task()
        }
    }

    #[test]
    fn common_context_owns_source_name_and_cursor_pushdown() {
        let driver =
            RestSourceDriver::new(|| Err(CdfError::internal("compile-only transport"))).unwrap();
        assert!(
            driver.option_schema()["source"]["properties"]
                .get("source_name")
                .is_none()
        );
        assert!(
            driver.option_schema()["resource"]["properties"]
                .get("cursor_param")
                .is_none()
        );

        let descriptor = ResourceDescriptor {
            resource_id: ResourceId::new("api.items").unwrap(),
            schema_source: SchemaSource::Declared {
                schema_hash: SchemaHash::new("schema-rest-driver").unwrap(),
                source: "test".to_owned(),
            },
            primary_key: Vec::new(),
            merge_key: Vec::new(),
            cursor: Some(CursorSpec {
                field: "updated_at".to_owned(),
                ordering: CursorOrderingClaim::Exact,
                lag_tolerance_ms: 0,
            }),
            write_disposition: WriteDisposition::Append,
            deduplication: None,
            contract: None,
            state_scope: ScopeKey::Resource,
            freshness: None,
            trust_level: TrustLevel::Governed,
        };
        let plan = driver
            .compile(SourceCompileRequest {
                source_kind: "rest".to_owned(),
                context: SourceCompileContext {
                    source_name: "api".to_owned(),
                    project_root: None,
                    cursor_pushdown: Some(SourceCursorPushdown {
                        parameter: Some("since".to_owned()),
                        fidelity: PushdownFidelity::Exact,
                    }),
                },
                source_options: BTreeMap::from([
                    (
                        "base_url".to_owned(),
                        serde_json::json!("https://api.example.com"),
                    ),
                    (
                        "rate_limit".to_owned(),
                        serde_json::json!({"requests_per_minute": 30}),
                    ),
                    ("egress_allowlist".to_owned(), serde_json::json!([])),
                ]),
                resource_options: BTreeMap::from([
                    ("path".to_owned(), serde_json::json!("/items")),
                    ("params".to_owned(), serde_json::json!({})),
                    ("records".to_owned(), serde_json::json!("$.items")),
                ]),
                descriptor,
                schema: Schema::empty(),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap();
        assert_eq!(
            plan.resource_capabilities.filters.default_fidelity,
            PushdownFidelity::Exact
        );
        assert_eq!(
            plan.resource_capabilities.filters.supported_operators,
            vec![">", ">=", "="]
        );
        let physical: RestPhysicalPlan =
            serde_json::from_value(plan.physical_plan.clone()).unwrap();
        let runtime = physical.to_runtime_plan().unwrap();
        assert_eq!(runtime.source, "api");
        assert_eq!(runtime.cursor_param.as_deref(), Some("since"));
        assert_eq!(runtime.cursor_filter_fidelity, PushdownFidelity::Exact);
        assert_eq!(
            plan.execution_capabilities.rate_limit,
            Some(SourceRateLimit {
                operations: 30,
                interval_ms: 60_000,
            })
        );
        assert_eq!(
            plan.execution_capabilities.quota_authority.as_deref(),
            Some("https://api.example.com:443")
        );

        let mut drifted = plan;
        drifted.resource_capabilities.filters.default_fidelity = PushdownFidelity::Unsupported;
        let error = validate_compiled_capabilities(&drifted, &runtime).unwrap_err();
        assert!(error.message.contains("executable cursor plan"));
    }

    #[test]
    fn registry_discovery_session_observes_and_retains_one_rest_sample() {
        let driver = RestSourceDriver::new(|| {
            Ok(Box::new(StaticDiscoveryTransport) as Box<dyn HttpTransport>)
        })
        .unwrap();
        let mut registry = SourceRegistry::new();
        registry.register(driver).unwrap();
        let descriptor = ResourceDescriptor {
            resource_id: ResourceId::new("api.items").unwrap(),
            schema_source: SchemaSource::Declared {
                schema_hash: SchemaHash::new("schema-rest-discovery").unwrap(),
                source: "test".to_owned(),
            },
            primary_key: Vec::new(),
            merge_key: Vec::new(),
            cursor: None,
            write_disposition: WriteDisposition::Append,
            deduplication: None,
            contract: None,
            state_scope: ScopeKey::Resource,
            freshness: None,
            trust_level: TrustLevel::Governed,
        };
        let plan = registry
            .compile(SourceCompileRequest {
                source_kind: "rest".to_owned(),
                context: SourceCompileContext {
                    source_name: "api".to_owned(),
                    project_root: None,
                    cursor_pushdown: None,
                },
                source_options: BTreeMap::from([
                    (
                        "base_url".to_owned(),
                        serde_json::json!("https://api.example.com"),
                    ),
                    ("egress_allowlist".to_owned(), serde_json::json!([])),
                ]),
                resource_options: BTreeMap::from([
                    ("path".to_owned(), serde_json::json!("/items")),
                    ("params".to_owned(), serde_json::json!({})),
                    ("records".to_owned(), serde_json::json!("$.items")),
                ]),
                descriptor,
                schema: Schema::empty(),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
            .unwrap();
        let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
            cdf_memory::DeterministicMemoryCoordinator::new(64 * 1024 * 1024, BTreeMap::new())
                .unwrap(),
        );
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(FixedSpillBudget::new(64 * 1024 * 1024).unwrap());
        let execution =
            ExecutionServices::new(Arc::new(ImmediateBlockingHost { memory, spill })).unwrap();
        let context = SourceResolutionContext::new(
            std::path::Path::new("."),
            Arc::new(NoopSecretProvider),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let health = registry
            .health_checks(&context, std::slice::from_ref(&plan))
            .unwrap();
        assert_eq!(health.len(), 1);
        assert_eq!(health[0].status, SourceHealthStatus::Passed);
        assert_eq!(health[0].details["records_read"], 1);
        let session = registry.discovery_session(&plan, &context).unwrap();

        assert_eq!(session.kind(), SourceDiscoveryKind::BoundedContent);
        let candidates = session.candidates().unwrap();
        let observation = session
            .observe(
                &candidates[0],
                &SourceDiscoveryRequest::new(1024 * 1024, 10).unwrap(),
            )
            .unwrap();
        assert_eq!(observation.records_read, 2);
        assert_eq!(observation.schema.fields()[0].name(), "id");
        assert_eq!(context.prepared_payloads().pending_count().unwrap(), 1);
    }
}
