use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

use arrow_array::{
    ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, UInt64Array,
};
use arrow_schema::{DataType, SchemaRef, TimeUnit};
use cdf_http::{
    AuthRefreshHook, AuthSession, HttpMethod, HttpRequest, HttpResponse, HttpTransport, Paginator,
    RateLimiter, RetryBudget, RetryDecision, RetryPolicy, RetryUnit, SecretProvider,
    send_with_policy,
};
use cdf_kernel::{
    Batch, BatchId, BatchStream, BoxFuture, CdfError, CursorPosition, CursorValue, PartitionPlan,
    QueryableResource, ResourceDescriptor, ResourceStream, Result, ScanPlan, ScanRequest,
    SchemaHash, SchemaSource, SourcePosition,
};
use futures_util::stream;
use serde_json::{Map, Value};

use crate::{CompiledResource, CompiledResourcePlan, RestResourcePlan};

pub(crate) const CURSOR_QUERY_PARAM_METADATA: &str = "cursor_query_param";
pub(crate) const CURSOR_QUERY_VALUE_METADATA: &str = "cursor_query_value";

#[derive(Clone)]
pub struct RestRuntimeDependencies {
    transport: Arc<Mutex<Box<dyn HttpTransport + Send>>>,
    secret_provider: Option<Arc<dyn SecretProvider + Send + Sync>>,
    auth_refresh: Option<Arc<Mutex<Box<dyn AuthRefreshHook + Send>>>>,
    retry_policy: RetryPolicy,
}

impl RestRuntimeDependencies {
    pub fn new(transport: impl HttpTransport + Send + 'static) -> Self {
        Self {
            transport: Arc::new(Mutex::new(Box::new(transport))),
            secret_provider: None,
            auth_refresh: None,
            retry_policy: RetryPolicy::default(),
        }
    }

    pub fn with_secret_provider(
        mut self,
        provider: impl SecretProvider + Send + Sync + 'static,
    ) -> Self {
        self.secret_provider = Some(Arc::new(provider));
        self
    }

    pub fn with_auth_refresh_hook(mut self, hook: impl AuthRefreshHook + Send + 'static) -> Self {
        self.auth_refresh = Some(Arc::new(Mutex::new(Box::new(hook))));
        self
    }

    pub fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }
}

impl fmt::Debug for RestRuntimeDependencies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestRuntimeDependencies")
            .field("transport", &"<explicit>")
            .field("secret_provider", &self.secret_provider.is_some())
            .field("auth_refresh", &self.auth_refresh.is_some())
            .field("retry_policy", &self.retry_policy)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct RestResource {
    compiled: CompiledResource,
    dependencies: RestRuntimeDependencies,
}

impl RestResource {
    pub fn new(compiled: CompiledResource, dependencies: RestRuntimeDependencies) -> Result<Self> {
        if !matches!(compiled.plan(), CompiledResourcePlan::Rest(_)) {
            return Err(CdfError::contract(
                "only compiled REST resources can be opened with REST runtime dependencies",
            ));
        }
        Ok(Self {
            compiled,
            dependencies,
        })
    }

    pub fn compiled(&self) -> &CompiledResource {
        &self.compiled
    }
}

impl CompiledResource {
    pub fn into_rest_resource(self, dependencies: RestRuntimeDependencies) -> Result<RestResource> {
        RestResource::new(self, dependencies)
    }

    pub fn to_rest_resource(&self, dependencies: RestRuntimeDependencies) -> Result<RestResource> {
        RestResource::new(self.clone(), dependencies)
    }
}

impl ResourceStream for RestResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        self.compiled.descriptor()
    }

    fn schema(&self) -> SchemaRef {
        self.compiled.schema()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        self.compiled.plan_partitions(request)
    }

    fn open(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<BatchStream>> {
        let descriptor = self.compiled.descriptor().clone();
        let schema = self.compiled.schema();
        let plan = match self.compiled.plan() {
            CompiledResourcePlan::Rest(plan) => (**plan).clone(),
            CompiledResourcePlan::Sql(_) | CompiledResourcePlan::Files(_) => {
                return Box::pin(async {
                    Err(CdfError::contract(
                        "only compiled REST resources can be opened by RestResource",
                    ))
                });
            }
        };
        let dependencies = self.dependencies.clone();

        Box::pin(async move {
            let batches = execute_rest(&descriptor, schema, &plan, &partition, dependencies)?;
            Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))) as BatchStream)
        })
    }
}

impl QueryableResource for RestResource {
    fn capabilities(&self) -> &cdf_kernel::ResourceCapabilities {
        self.compiled.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.compiled.negotiate(request)
    }
}

pub(crate) fn cursor_pushdown_value(
    descriptor: &ResourceDescriptor,
    plan: &RestResourcePlan,
    expression: &str,
) -> Option<String> {
    let cursor = descriptor.cursor.as_ref()?;
    let cursor_param = plan.cursor_param.as_deref();
    for operator in [">=", ">", "="] {
        let Some((left, right)) = expression.split_once(operator) else {
            continue;
        };
        let left = left.trim();
        let targets_cursor = expression_side_mentions_name(left, &cursor.field)
            || cursor_param.is_some_and(|param| expression_side_mentions_name(left, param));
        if !targets_cursor {
            continue;
        }
        let value = normalize_predicate_literal(right.trim())?;
        if !value.is_empty() {
            return Some(value);
        }
    }
    None
}

fn normalize_predicate_literal(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Some(stripped) = value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
    {
        return Some(stripped.to_owned());
    }
    let token = value.split_whitespace().next()?;
    is_unquoted_scalar_literal(token).then(|| token.to_owned())
}

fn is_unquoted_scalar_literal(value: &str) -> bool {
    value.chars().any(|character| character.is_ascii_digit())
        && value
            .chars()
            .all(|character| character.is_ascii_digit() || matches!(character, '-' | '+' | '.'))
}

fn expression_side_mentions_name(expression: &str, name: &str) -> bool {
    expression
        .split(|character: char| {
            !(character.is_ascii_alphanumeric() || matches!(character, '_' | '.'))
        })
        .any(|token| token == name || token.ends_with(&format!(".{name}")))
}

fn execute_rest(
    descriptor: &ResourceDescriptor,
    schema: SchemaRef,
    plan: &RestResourcePlan,
    partition: &PartitionPlan,
    dependencies: RestRuntimeDependencies,
) -> Result<Vec<Batch>> {
    validate_partition(descriptor, plan, partition)?;
    let schema_hash = declared_schema_hash(descriptor)?;
    if schema.fields().is_empty() {
        return Err(CdfError::data(
            "declarative REST execution requires a declared schema with at least one field",
        ));
    }

    let mut limiter = RateLimiter::new(plan.rate_limit.clone(), 0);
    let mut auth_session = plan.auth.clone().map(AuthSession::new);
    let mut retry_budget = RetryBudget::new(dependencies.retry_policy.clone());
    let mut paginator = plan.pagination.clone().map(Paginator::new);
    let base_request_url = build_request_url(plan, partition)?;
    let mut next_url = Some(match &paginator {
        Some(paginator) => paginator.first_request(&base_request_url).url,
        None => base_request_url,
    });
    let mut batches = Vec::new();
    let mut page_index = 0_usize;

    while let Some(url) = next_url {
        let mut response = send_page(
            &dependencies,
            plan,
            &url,
            &mut auth_session,
            &mut retry_budget,
            &mut limiter,
        )?;
        let body = response
            .body()
            .ok_or_else(|| CdfError::data("REST HTTP response did not include a JSON body"))?;
        let decoded = decode_response_page(body, &plan.record_selector)?;
        response.page.item_count = decoded.records.len();
        response.page.fields = decoded.pagination_fields;

        if !decoded.records.is_empty() {
            let (record_batch, position) = records_to_batch(&schema, descriptor, &decoded.records)?;
            let mut batch = Batch::from_record_batch(
                BatchId::new(format!(
                    "{}-{}-{:06}",
                    sanitize_id_part(descriptor.resource_id.as_str()),
                    sanitize_id_part(partition.partition_id.as_str()),
                    page_index + 1
                ))?,
                descriptor.resource_id.clone(),
                partition.partition_id.clone(),
                schema_hash.clone(),
                record_batch,
            )?;
            batch.header.source_position = position;
            batches.push(batch);
        }

        page_index = page_index.saturating_add(1);
        next_url = paginator
            .as_mut()
            .and_then(|paginator| paginator.next_request(&url, &response))
            .map(|request| request.url);
    }

    Ok(batches)
}

fn validate_partition(
    descriptor: &ResourceDescriptor,
    plan: &RestResourcePlan,
    partition: &PartitionPlan,
) -> Result<()> {
    if partition.partition_id.as_str() != "rest" {
        return Err(CdfError::contract(format!(
            "declarative REST resource `{}` expected partition `rest`, got `{}`",
            descriptor.resource_id, partition.partition_id
        )));
    }
    if partition.metadata.get("kind").map(String::as_str) != Some("rest") {
        return Err(CdfError::contract(format!(
            "declarative REST resource `{}` expected a REST partition plan",
            descriptor.resource_id
        )));
    }
    if partition.metadata.get("resource_id").map(String::as_str)
        != Some(descriptor.resource_id.as_str())
    {
        return Err(CdfError::contract(format!(
            "declarative REST partition resource id does not match `{}`",
            descriptor.resource_id
        )));
    }
    if partition.scope != descriptor.state_scope {
        return Err(CdfError::contract(format!(
            "declarative REST partition scope does not match resource `{}`",
            descriptor.resource_id
        )));
    }
    if partition.metadata.get("path").map(String::as_str) != Some(plan.path.as_str()) {
        return Err(CdfError::contract(format!(
            "declarative REST partition path does not match `{}`",
            plan.path
        )));
    }
    Ok(())
}

fn declared_schema_hash(descriptor: &ResourceDescriptor) -> Result<SchemaHash> {
    match &descriptor.schema_source {
        SchemaSource::Declared { schema_hash, .. } => Ok(schema_hash.clone()),
        SchemaSource::Discovered { .. } | SchemaSource::Contract { .. } => Err(CdfError::data(
            "declarative REST execution requires a declared schema hash",
        )),
    }
}

fn send_page(
    dependencies: &RestRuntimeDependencies,
    plan: &RestResourcePlan,
    url: &str,
    auth_session: &mut Option<AuthSession>,
    retry_budget: &mut RetryBudget,
    limiter: &mut RateLimiter,
) -> Result<HttpResponse> {
    loop {
        let decision = limiter.before_request(0);
        if !decision.allowed {
            return Err(CdfError::rate_limited(
                decision
                    .reason
                    .unwrap_or_else(|| "REST rate limiter blocked request".to_owned()),
                Some(decision.wait_ms),
            ));
        }

        let mut request = HttpRequest::new(HttpMethod::Get, url.to_owned());
        validate_http_url(&request.url)?;
        plan.allowlist.check(&request)?;
        if let Some(session) = auth_session {
            let provider = dependencies.secret_provider.as_deref().ok_or_else(|| {
                CdfError::auth(
                    "REST resource auth requires an explicit SecretProvider runtime dependency",
                )
            })?;
            session.apply(provider, &mut request)?;
        }

        let response = {
            let mut transport = dependencies.transport.lock().map_err(|_| {
                CdfError::internal("REST HTTP transport mutex was poisoned during send")
            })?;
            match send_with_policy(&mut **transport, &plan.allowlist, request) {
                Ok(response) => response,
                Err(error) => match retry_budget.next_retry(
                    &error,
                    &RetryUnit::Request {
                        method: HttpMethod::Get,
                        idempotency_key: false,
                    },
                ) {
                    RetryDecision::Retry { .. } => continue,
                    RetryDecision::GiveUp { error } => return Err(error),
                },
            }
        };
        limiter.observe_response(&response, 0);

        if matches!(response.status, 401 | 403)
            && auth_session.is_some()
            && let Some(hook) = dependencies.auth_refresh.as_ref()
        {
            let provider = dependencies.secret_provider.as_deref().ok_or_else(|| {
                CdfError::auth(
                    "REST auth refresh requires an explicit SecretProvider runtime dependency",
                )
            })?;
            let mut hook = hook.lock().map_err(|_| {
                CdfError::internal("REST auth refresh hook mutex was poisoned during refresh")
            })?;
            auth_session
                .as_mut()
                .expect("checked auth session availability")
                .refresh_once(provider, &mut **hook)?;
            continue;
        }

        if let Some(error) = RetryBudget::classify_response(&response) {
            match retry_budget.next_retry(
                &error,
                &RetryUnit::Request {
                    method: HttpMethod::Get,
                    idempotency_key: false,
                },
            ) {
                RetryDecision::Retry { .. } => continue,
                RetryDecision::GiveUp { error } => return Err(error),
            }
        }

        return Ok(response);
    }
}

#[derive(Debug)]
struct DecodedPage {
    records: Vec<Map<String, Value>>,
    pagination_fields: BTreeMap<String, String>,
}

fn decode_response_page(bytes: &[u8], selector: &str) -> Result<DecodedPage> {
    let root: Value = serde_json::from_slice(bytes).map_err(|error| {
        CdfError::data(format!("REST response body is not valid JSON: {error}"))
    })?;
    let records = select_records(&root, selector)?;
    let mut objects = Vec::with_capacity(records.len());
    for record in records {
        let Some(object) = record.as_object() else {
            return Err(CdfError::data(
                "REST record selector yielded a non-object array entry",
            ));
        };
        objects.push(object.clone());
    }

    Ok(DecodedPage {
        records: objects,
        pagination_fields: top_level_pagination_fields(&root),
    })
}

fn select_records<'a>(root: &'a Value, selector: &str) -> Result<&'a Vec<Value>> {
    if selector == "$" {
        return root
            .as_array()
            .ok_or_else(|| CdfError::data("REST record selector `$` requires a top-level array"));
    }
    let Some(field) = selector.strip_prefix("$.") else {
        return Err(CdfError::data(
            "REST record selector must be `$` or `$.<field>` in this execution slice",
        ));
    };
    if field.is_empty() || field.contains('.') {
        return Err(CdfError::data(
            "REST record selector supports only one object field after `$.`",
        ));
    }
    root.get(field)
        .ok_or_else(|| {
            CdfError::data(format!(
                "REST record selector target `{field}` is missing from response"
            ))
        })?
        .as_array()
        .ok_or_else(|| {
            CdfError::data(format!(
                "REST record selector target `{field}` is not an array"
            ))
        })
}

fn top_level_pagination_fields(root: &Value) -> BTreeMap<String, String> {
    let Some(object) = root.as_object() else {
        return BTreeMap::new();
    };
    object
        .iter()
        .filter_map(|(key, value)| scalar_marker(value).map(|value| (key.clone(), value)))
        .collect()
}

fn records_to_batch(
    schema: &SchemaRef,
    descriptor: &ResourceDescriptor,
    records: &[Map<String, Value>],
) -> Result<(RecordBatch, Option<SourcePosition>)> {
    let mut arrays = Vec::with_capacity(schema.fields().len());
    let mut cursor = None;

    for field in schema.fields() {
        arrays.push(array_for_field(field.as_ref(), records)?);
        if descriptor
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.field == field.name().as_str())
        {
            cursor = Some(max_cursor_for_field(field.as_ref(), records)?);
        }
    }

    let record_batch = RecordBatch::try_new(schema.clone(), arrays).map_err(CdfError::from)?;
    let source_position = match (&descriptor.cursor, cursor) {
        (Some(cursor_spec), Some(value)) => Some(SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: cursor_spec.field.clone(),
            value: value.into_cursor_value(),
        })),
        (Some(cursor_spec), None) => {
            return Err(CdfError::data(format!(
                "REST cursor field `{}` is missing from declared schema",
                cursor_spec.field
            )));
        }
        (None, _) => None,
    };
    Ok((record_batch, source_position))
}

fn array_for_field(
    field: &arrow_schema::Field,
    records: &[Map<String, Value>],
) -> Result<ArrayRef> {
    let name = field.name();
    match field.data_type() {
        DataType::Utf8 => Ok(Arc::new(StringArray::from(
            records
                .iter()
                .map(|record| match optional_value(record, field)? {
                    Some(value) => string_value(name, value).map(Some),
                    None => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            records
                .iter()
                .map(|record| match optional_value(record, field)? {
                    Some(value) => i64_value(name, value).map(Some),
                    None => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::UInt64 => Ok(Arc::new(UInt64Array::from(
            records
                .iter()
                .map(|record| match optional_value(record, field)? {
                    Some(value) => u64_value(name, value).map(Some),
                    None => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(
            records
                .iter()
                .map(|record| match optional_value(record, field)? {
                    Some(value) => f64_value(name, value).map(Some),
                    None => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(
            records
                .iter()
                .map(|record| match optional_value(record, field)? {
                    Some(value) => bool_value(name, value).map(Some),
                    None => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Date32 => Ok(Arc::new(Date32Array::from(
            records
                .iter()
                .map(|record| match optional_value(record, field)? {
                    Some(value) => date32_value(name, value).map(Some),
                    None => Ok(None),
                })
                .collect::<Result<Vec<_>>>()?,
        ))),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => {
            let array = TimestampMillisecondArray::from(
                records
                    .iter()
                    .map(|record| match optional_value(record, field)? {
                        Some(value) => timestamp_millis_value(name, value).map(Some),
                        None => Ok(None),
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_timezone_opt(timezone.clone());
            Ok(Arc::new(array))
        }
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
            let array = TimestampMicrosecondArray::from(
                records
                    .iter()
                    .map(|record| match optional_value(record, field)? {
                        Some(value) => timestamp_micros_value(name, value).map(Some),
                        None => Ok(None),
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
            .with_timezone_opt(timezone.clone());
            Ok(Arc::new(array))
        }
        other => Err(CdfError::data(format!(
            "REST schema field `{name}` has unsupported Arrow type {other}"
        ))),
    }
}

fn optional_value<'a>(
    record: &'a Map<String, Value>,
    field: &arrow_schema::Field,
) -> Result<Option<&'a Value>> {
    match record.get(field.name()) {
        Some(Value::Null) | None if field.is_nullable() => Ok(None),
        Some(Value::Null) | None => Err(CdfError::data(format!(
            "REST record is missing non-nullable field `{}`",
            field.name()
        ))),
        Some(value) => Ok(Some(value)),
    }
}

fn max_cursor_for_field(
    field: &arrow_schema::Field,
    records: &[Map<String, Value>],
) -> Result<ObservedCursor> {
    let mut max_value = None;
    for record in records {
        let value = record.get(field.name()).ok_or_else(|| {
            CdfError::data(format!(
                "REST cursor field `{}` is missing from an accepted record",
                field.name()
            ))
        })?;
        if value.is_null() {
            return Err(CdfError::data(format!(
                "REST cursor field `{}` is null in an accepted record",
                field.name()
            )));
        }
        let value = cursor_value_for_field(field, value)?;
        if max_value
            .as_ref()
            .is_none_or(|current| value.greater_than(current))
        {
            max_value = Some(value);
        }
    }
    max_value.ok_or_else(|| {
        CdfError::data(format!(
            "REST cursor field `{}` has no observed values",
            field.name()
        ))
    })
}

fn cursor_value_for_field(field: &arrow_schema::Field, value: &Value) -> Result<ObservedCursor> {
    let name = field.name();
    Ok(match field.data_type() {
        DataType::Utf8 => ObservedCursor::String(string_value(name, value)?),
        DataType::Int64 => ObservedCursor::I64(i64_value(name, value)?),
        DataType::UInt64 => ObservedCursor::U64(u64_value(name, value)?),
        DataType::Float64 => ObservedCursor::F64(f64_value(name, value)?),
        DataType::Date32 => ObservedCursor::I64(i64::from(date32_value(name, value)?)),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => ObservedCursor::TimestampMicros {
            micros: timestamp_millis_value(name, value)?.saturating_mul(1_000),
            timezone: timezone.as_ref().map(ToString::to_string),
        },
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => ObservedCursor::TimestampMicros {
            micros: timestamp_micros_value(name, value)?,
            timezone: timezone.as_ref().map(ToString::to_string),
        },
        other => {
            return Err(CdfError::data(format!(
                "REST cursor field `{name}` has unsupported Arrow type {other}"
            )));
        }
    })
}

#[derive(Clone, Debug, PartialEq)]
enum ObservedCursor {
    String(String),
    I64(i64),
    U64(u64),
    F64(f64),
    TimestampMicros {
        micros: i64,
        timezone: Option<String>,
    },
}

impl ObservedCursor {
    fn greater_than(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(left), Self::String(right)) => left > right,
            (Self::I64(left), Self::I64(right)) => left > right,
            (Self::U64(left), Self::U64(right)) => left > right,
            (Self::F64(left), Self::F64(right)) => left > right,
            (
                Self::TimestampMicros { micros: left, .. },
                Self::TimestampMicros { micros: right, .. },
            ) => left > right,
            _ => false,
        }
    }

    fn into_cursor_value(self) -> CursorValue {
        match self {
            Self::String(value) => CursorValue::String(value),
            Self::I64(value) => CursorValue::I64(value),
            Self::U64(value) => CursorValue::U64(value),
            Self::F64(value) => CursorValue::DecimalString(value.to_string()),
            Self::TimestampMicros { micros, timezone } => {
                CursorValue::TimestampMicros { micros, timezone }
            }
        }
    }
}

fn string_value(field: &str, value: &Value) -> Result<String> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).map_err(|error| {
            CdfError::data(format!("serialize REST JSON field `{field}`: {error}"))
        }),
        Value::Null => Err(CdfError::data(format!(
            "REST field `{field}` is null where a value is required"
        ))),
    }
}

fn i64_value(field: &str, value: &Value) -> Result<i64> {
    match value {
        Value::Number(value) => value.as_i64().ok_or_else(|| {
            CdfError::data(format!("REST field `{field}` cannot be coerced to int64"))
        }),
        Value::String(value) => value.parse::<i64>().map_err(|error| {
            CdfError::data(format!(
                "REST field `{field}` cannot be parsed as int64: {error}"
            ))
        }),
        _ => Err(CdfError::data(format!(
            "REST field `{field}` cannot be coerced to int64"
        ))),
    }
}

fn u64_value(field: &str, value: &Value) -> Result<u64> {
    match value {
        Value::Number(value) => value.as_u64().ok_or_else(|| {
            CdfError::data(format!("REST field `{field}` cannot be coerced to uint64"))
        }),
        Value::String(value) => value.parse::<u64>().map_err(|error| {
            CdfError::data(format!(
                "REST field `{field}` cannot be parsed as uint64: {error}"
            ))
        }),
        _ => Err(CdfError::data(format!(
            "REST field `{field}` cannot be coerced to uint64"
        ))),
    }
}

fn f64_value(field: &str, value: &Value) -> Result<f64> {
    let parsed = match value {
        Value::Number(value) => value.as_f64().ok_or_else(|| {
            CdfError::data(format!("REST field `{field}` cannot be coerced to float64"))
        })?,
        Value::String(value) => value.parse::<f64>().map_err(|error| {
            CdfError::data(format!(
                "REST field `{field}` cannot be parsed as float64: {error}"
            ))
        })?,
        _ => {
            return Err(CdfError::data(format!(
                "REST field `{field}` cannot be coerced to float64"
            )));
        }
    };
    if !parsed.is_finite() {
        return Err(CdfError::data(format!(
            "REST field `{field}` contains a non-finite float64"
        )));
    }
    Ok(parsed)
}

fn bool_value(field: &str, value: &Value) -> Result<bool> {
    match value {
        Value::Bool(value) => Ok(*value),
        Value::String(value) if value.eq_ignore_ascii_case("true") => Ok(true),
        Value::String(value) if value.eq_ignore_ascii_case("false") => Ok(false),
        _ => Err(CdfError::data(format!(
            "REST field `{field}` cannot be coerced to boolean"
        ))),
    }
}

fn date32_value(field: &str, value: &Value) -> Result<i32> {
    match value {
        Value::Number(_) => i64_value(field, value).and_then(|value| {
            i32::try_from(value).map_err(|error| {
                CdfError::data(format!("REST field `{field}` cannot fit date32: {error}"))
            })
        }),
        Value::String(value) => parse_date32(value).ok_or_else(|| {
            CdfError::data(format!(
                "REST field `{field}` cannot be parsed as YYYY-MM-DD date32"
            ))
        }),
        _ => Err(CdfError::data(format!(
            "REST field `{field}` cannot be coerced to date32"
        ))),
    }
}

fn timestamp_millis_value(field: &str, value: &Value) -> Result<i64> {
    match value {
        Value::Number(_) => i64_value(field, value),
        Value::String(value) => parse_rfc3339_micros(value)
            .map(|micros| micros / 1_000)
            .ok_or_else(|| {
                CdfError::data(format!(
                    "REST field `{field}` cannot be parsed as RFC3339 timestamp"
                ))
            }),
        _ => Err(CdfError::data(format!(
            "REST field `{field}` cannot be coerced to timestamp millis"
        ))),
    }
}

fn timestamp_micros_value(field: &str, value: &Value) -> Result<i64> {
    match value {
        Value::Number(_) => i64_value(field, value),
        Value::String(value) => parse_rfc3339_micros(value).ok_or_else(|| {
            CdfError::data(format!(
                "REST field `{field}` cannot be parsed as RFC3339 timestamp"
            ))
        }),
        _ => Err(CdfError::data(format!(
            "REST field `{field}` cannot be coerced to timestamp micros"
        ))),
    }
}

fn scalar_marker(value: &Value) -> Option<String> {
    match value {
        Value::String(value) if !value.trim().is_empty() => Some(value.clone()),
        Value::String(_) => None,
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
}

fn build_request_url(plan: &RestResourcePlan, partition: &PartitionPlan) -> Result<String> {
    let mut url = join_base_url_and_path(&plan.base_url, &plan.path)?;
    for (name, value) in &plan.params {
        url = append_query_param(&url, name, value);
    }
    if let (Some(param), Some(value)) = (
        partition.metadata.get(CURSOR_QUERY_PARAM_METADATA),
        partition.metadata.get(CURSOR_QUERY_VALUE_METADATA),
    ) {
        url = append_query_param(&url, param, value);
    }
    validate_http_url(&url)?;
    Ok(url)
}

fn join_base_url_and_path(base_url: &str, path: &str) -> Result<String> {
    validate_http_url(base_url)?;
    if path.contains("://") {
        return Err(CdfError::contract(
            "REST resource path must be relative to the source base_url",
        ));
    }

    let (base_without_query, base_query) = split_query(base_url);
    let (path_without_query, path_query) = split_query(path);
    let joined = if path_without_query.is_empty() {
        base_without_query.to_owned()
    } else if path_without_query.starts_with('/') {
        format!("{}{}", origin(base_without_query)?, path_without_query)
    } else {
        format!(
            "{}/{}",
            base_without_query.trim_end_matches('/'),
            path_without_query.trim_start_matches('/')
        )
    };

    let query = [base_query, path_query]
        .into_iter()
        .flatten()
        .filter(|query| !query.is_empty())
        .collect::<Vec<_>>();
    if query.is_empty() {
        Ok(joined)
    } else {
        Ok(format!("{joined}?{}", query.join("&")))
    }
}

fn split_query(value: &str) -> (&str, Option<&str>) {
    value
        .split_once('?')
        .map_or((value, None), |(head, query)| (head, Some(query)))
}

fn origin(url: &str) -> Result<&str> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| CdfError::contract("REST base_url must include a scheme"))?;
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .ok_or_else(|| CdfError::contract("REST base_url must include a host"))?;
    let len = scheme.len() + "://".len() + authority.len();
    Ok(&url[..len])
}

fn append_query_param(url: &str, name: &str, value: &str) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    format!(
        "{url}{separator}{}={}",
        percent_encode(name),
        percent_encode(value)
    )
}

fn validate_http_url(url: &str) -> Result<()> {
    if url.contains(char::is_whitespace) {
        return Err(CdfError::contract(
            "REST request URL must not contain whitespace",
        ));
    }
    if url.contains('#') {
        return Err(CdfError::contract(
            "REST request URL must not include a fragment",
        ));
    }
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| CdfError::contract("REST request URL must include a scheme"))?;
    if !matches!(scheme, "http" | "https") {
        return Err(CdfError::contract(
            "REST request URL must use the http or https scheme",
        ));
    }
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .ok_or_else(|| CdfError::contract("REST request URL must include a host"))?;
    if authority.trim().is_empty() || authority.contains(char::is_whitespace) {
        return Err(CdfError::contract("REST request URL must include a host"));
    }
    Ok(())
}

fn percent_encode(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.as_bytes() {
        let character = *byte as char;
        if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | '~') {
            encoded.push(character);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
}

fn parse_date32(value: &str) -> Option<i32> {
    let (year, month, day) = parse_date(value)?;
    i32::try_from(days_from_civil(year, month, day)).ok()
}

fn parse_rfc3339_micros(value: &str) -> Option<i64> {
    let (date, time_and_zone) = value.split_once('T')?;
    let (year, month, day) = parse_date(date)?;
    let (time, offset_seconds) = split_time_zone(time_and_zone)?;
    let mut parts = time.split(':');
    let hour = parts.next()?.parse::<i64>().ok()?;
    let minute = parts.next()?.parse::<i64>().ok()?;
    let second_fraction = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let (second_text, fraction_text) = second_fraction
        .split_once('.')
        .map_or((second_fraction, ""), |(second, fraction)| {
            (second, fraction)
        });
    let second = second_text.parse::<i64>().ok()?;
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=60).contains(&second) {
        return None;
    }
    let fraction = parse_fraction_micros(fraction_text)?;
    let days = days_from_civil(year, month, day);
    let seconds = days
        .checked_mul(86_400)?
        .checked_add(hour.checked_mul(3_600)?)?
        .checked_add(minute.checked_mul(60)?)?
        .checked_add(second)?
        .checked_sub(i64::from(offset_seconds))?;
    seconds.checked_mul(1_000_000)?.checked_add(fraction)
}

fn parse_date(value: &str) -> Option<(i64, u32, u32)> {
    let mut parts = value.split('-');
    let year = parts.next()?.parse::<i64>().ok()?;
    let month = parts.next()?.parse::<u32>().ok()?;
    let day = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some()
        || !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
    {
        return None;
    }
    Some((year, month, day))
}

fn split_time_zone(value: &str) -> Option<(&str, i32)> {
    if let Some(time) = value.strip_suffix('Z') {
        return Some((time, 0));
    }
    let split_at = value.rfind(['+', '-'])?;
    let (time, offset) = value.split_at(split_at);
    let sign = if offset.starts_with('-') { -1 } else { 1 };
    let offset = &offset[1..];
    let (hours, minutes) = offset.split_once(':')?;
    let hours = hours.parse::<i32>().ok()?;
    let minutes = minutes.parse::<i32>().ok()?;
    if !(0..=23).contains(&hours) || !(0..=59).contains(&minutes) {
        return None;
    }
    Some((time, sign * (hours * 3_600 + minutes * 60)))
}

fn parse_fraction_micros(value: &str) -> Option<i64> {
    if value.is_empty() {
        return Some(0);
    }
    if !value.chars().all(|character| character.is_ascii_digit()) {
        return None;
    }
    let mut padded = value.chars().take(6).collect::<String>();
    while padded.len() < 6 {
        padded.push('0');
    }
    padded.parse::<i64>().ok()
}

fn days_in_month(year: i64, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

fn is_leap_year(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn sanitize_id_part(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_parser_handles_utc_and_offsets() {
        assert_eq!(parse_rfc3339_micros("1970-01-01T00:00:00Z").unwrap(), 0);
        assert_eq!(
            parse_rfc3339_micros("1970-01-01T01:00:00+01:00").unwrap(),
            0
        );
        assert_eq!(
            parse_rfc3339_micros("1970-01-01T00:00:00.123456Z").unwrap(),
            123_456
        );
        assert_eq!(
            parse_rfc3339_micros("1970-01-01T01:30:00+01:30").unwrap(),
            0
        );
        assert_eq!(
            parse_rfc3339_micros("1969-12-31T22:30:00-01:30").unwrap(),
            0
        );
        assert!(parse_rfc3339_micros("1970-01-01T24:00:00Z").is_none());
        assert!(parse_rfc3339_micros("1970-01-01T00:60:00Z").is_none());
        assert!(parse_rfc3339_micros("1970-01-01T00:00:61Z").is_none());
        assert!(parse_rfc3339_micros("1970-01-01T00:00:00+24:00").is_none());
        assert!(parse_rfc3339_micros("1970-01-01T00:00:00+00:60").is_none());
    }

    #[test]
    fn date32_parser_uses_unix_epoch_days() {
        assert_eq!(parse_date32("1969-12-31").unwrap(), -1);
        assert_eq!(parse_date32("1970-01-01").unwrap(), 0);
        assert_eq!(parse_date32("1970-01-02").unwrap(), 1);
        assert_eq!(parse_date32("2000-02-29").unwrap(), 11016);
        assert_eq!(parse_date32("2000-03-01").unwrap(), 11017);
        assert!(parse_date32("2026-13-01").is_none());
        assert!(parse_date32("2026-04-31").is_none());
        assert!(parse_date32("2026-02-29").is_none());
        assert!(parse_date32("1900-02-29").is_none());
        assert!(parse_date32("2026-01-01-extra").is_none());
    }

    #[test]
    fn helper_contracts_are_strict_at_boundaries() {
        assert!(
            !ObservedCursor::String("a".to_owned())
                .greater_than(&ObservedCursor::String("a".to_owned()))
        );
        assert!(!ObservedCursor::I64(1).greater_than(&ObservedCursor::I64(1)));
        assert!(!ObservedCursor::U64(1).greater_than(&ObservedCursor::U64(1)));
        assert!(!ObservedCursor::F64(1.0).greater_than(&ObservedCursor::F64(1.0)));
        assert!(
            !ObservedCursor::TimestampMicros {
                micros: 1,
                timezone: Some("UTC".to_owned()),
            }
            .greater_than(&ObservedCursor::TimestampMicros {
                micros: 1,
                timezone: Some("UTC".to_owned()),
            })
        );

        assert!(bool_value("active", &Value::String("sometimes".to_owned())).is_err());
        assert_eq!(
            timestamp_micros_value("updated_at", &Value::Number(123.into())).unwrap(),
            123
        );
        assert_eq!(scalar_marker(&Value::String("   ".to_owned())), None);
        assert_eq!(
            origin("https://api.example.com/v1?existing=1").unwrap(),
            "https://api.example.com"
        );
        assert!(validate_http_url("https:///v1").is_err());
        assert!(validate_http_url("https://api example.com/v1").is_err());
        assert_eq!(days_in_month(2026, 4), 30);
        assert_eq!(days_in_month(2024, 2), 29);
        assert_eq!(days_in_month(2023, 2), 28);
        assert!(is_leap_year(2024));
        assert!(!is_leap_year(2023));
        assert!(!is_leap_year(1900));
        assert!(is_leap_year(2000));
        assert_eq!(days_from_civil(-1, 3, 1), -719_834);
        assert_eq!(days_from_civil(-400, 3, 1), -865_565);
        assert_eq!(sanitize_id_part("api.items/v1_*"), "api-items-v1_-");
    }

    #[test]
    fn predicate_literal_extraction_is_cursor_only() {
        let descriptor = ResourceDescriptor {
            resource_id: cdf_kernel::ResourceId::new("api.items").unwrap(),
            schema_source: SchemaSource::Discovered { schema_hash: None },
            primary_key: Vec::new(),
            merge_key: Vec::new(),
            cursor: Some(cdf_kernel::CursorSpec {
                field: "updated_at".to_owned(),
                ordering: cdf_kernel::CursorOrderingClaim::Inexact,
                lag_tolerance_ms: 0,
            }),
            write_disposition: cdf_kernel::WriteDisposition::Append,
            contract: None,
            state_scope: cdf_kernel::ScopeKey::Resource,
            freshness: None,
            trust_level: cdf_kernel::TrustLevel::Experimental,
        };
        let plan = RestResourcePlan {
            source: "api".to_owned(),
            base_url: "https://api.example.com".to_owned(),
            path: "/items".to_owned(),
            params: BTreeMap::new(),
            record_selector: "$".to_owned(),
            pagination: None,
            auth: None,
            rate_limit: cdf_http::RateLimitPolicy::unrestricted(),
            respect_headers: Vec::new(),
            allowlist: cdf_http::EgressAllowlist::allow_any(),
            cursor_param: Some("since".to_owned()),
            cursor_filter_fidelity: cdf_kernel::PushdownFidelity::Inexact,
            records_transform: None,
        };

        assert_eq!(
            cursor_pushdown_value(&descriptor, &plan, "updated_at >= \"2026-07-01T00:00:00Z\""),
            Some("2026-07-01T00:00:00Z".to_owned())
        );
        assert_eq!(cursor_pushdown_value(&descriptor, &plan, "id = 1"), None);
        assert_eq!(
            cursor_pushdown_value(&descriptor, &plan, "not_updated_at >= \"2026-07-01\""),
            None
        );
        assert_eq!(
            cursor_pushdown_value(&descriptor, &plan, "updated_at >= checkpoint.cursor"),
            None
        );
    }
}
