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
    SourceAttestationStrength, SourceCompileRequest, SourceCursorPushdown, SourceDriver,
    SourceDriverDescriptor, SourceDriverId, SourceExecutionCapabilities, SourceExecutorClass,
    SourceResolutionContext, SourceRetryGranularity, artifact_hash,
};
use serde::{Deserialize, Serialize};

use crate::{RestResource, RestResourcePlan, RestRuntimeDependencies};

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
            "required": ["path", "params", "records"],
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Schema;
    use cdf_kernel::{
        CursorOrderingClaim, CursorSpec, ResourceDescriptor, ResourceId, SchemaHash, SchemaSource,
        ScopeKey, TrustLevel, WriteDisposition,
    };
    use cdf_runtime::{SourceCompileContext, SourceCursorPushdown};

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
            })
            .unwrap();
        let physical: RestPhysicalPlan = serde_json::from_value(plan.physical_plan).unwrap();
        let runtime = physical.to_runtime_plan().unwrap();
        assert_eq!(runtime.source, "api");
        assert_eq!(runtime.cursor_param.as_deref(), Some("since"));
        assert_eq!(runtime.cursor_filter_fidelity, PushdownFidelity::Exact);
    }
}
