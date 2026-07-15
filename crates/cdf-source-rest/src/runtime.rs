use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
};

use arrow_array::{RecordBatch, new_null_array};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_formats::{JsonOptions, read_ndjson_bytes_with_declared_schema};
use cdf_http::{
    AuthRefreshHook, AuthScheme, AuthSession, HttpMethod, HttpRequest, HttpResponse, HttpTransport,
    Paginator, RateLimiter, RetryBudget, RetryDecision, RetryPolicy, RetryUnit, SecretProvider,
    SecretUri, send_with_policy,
};
use cdf_kernel::{
    Batch, BatchId, BatchStream, BoxFuture, CdfError, CursorPosition, CursorValue,
    DeliveryGuarantee, Expression, ExpressionLiteral, OpenedPartitionStream, PartitionId,
    PartitionPlan, PayloadRetention, PlanId, PreContractResidualCandidate, PushdownFidelity,
    PushedPredicate, QueryableResource, ResourceCapabilities, ResourceDescriptor, ResourceStream,
    Result, ScanPlan, ScanRequest, SchemaHash, SchemaSource, SourcePosition, TypePolicyAllowances,
    WriteDisposition, source_name,
};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest, reserve_blocking,
};
use cdf_runtime::{
    ExecutionServices, PreparedSourcePayload, PreparedSourcePayloadKey, PreparedSourcePayloads,
    ReadOptions, SourceDiscoveryRequest, SourceDriverId, artifact_hash,
};
use futures_util::stream;
use serde_json::{Map, Value};

use crate::RestResourcePlan;

pub const CURSOR_QUERY_PARAM_METADATA: &str = "cursor_query_param";
pub const CURSOR_QUERY_VALUE_METADATA: &str = "cursor_query_value";

#[derive(Clone)]
pub struct RestRuntimeDependencies {
    transport: Arc<dyn HttpTransport>,
    secret_provider: Option<Arc<dyn SecretProvider + Send + Sync>>,
    auth_refresh: Option<Arc<Mutex<Box<dyn AuthRefreshHook + Send>>>>,
    retry_policy: RetryPolicy,
    execution: Option<ExecutionServices>,
    prepared_payloads: PreparedSourcePayloads,
}

impl RestRuntimeDependencies {
    pub fn new(transport: impl HttpTransport + 'static) -> Self {
        Self::from_boxed_transport(Box::new(transport))
    }

    pub fn from_boxed_transport(transport: Box<dyn HttpTransport>) -> Self {
        Self {
            transport: Arc::from(transport),
            secret_provider: None,
            auth_refresh: None,
            retry_policy: RetryPolicy::default(),
            execution: None,
            prepared_payloads: PreparedSourcePayloads::default(),
        }
    }

    pub fn with_secret_provider(
        mut self,
        provider: impl SecretProvider + Send + Sync + 'static,
    ) -> Self {
        self.secret_provider = Some(Arc::new(provider));
        self
    }

    pub fn with_shared_secret_provider(
        mut self,
        provider: Arc<dyn SecretProvider + Send + Sync>,
    ) -> Self {
        self.secret_provider = Some(provider);
        self
    }

    pub fn with_execution_services(mut self, execution: ExecutionServices) -> Self {
        self.execution = Some(execution);
        self
    }

    pub fn with_prepared_payloads(mut self, prepared_payloads: PreparedSourcePayloads) -> Self {
        self.prepared_payloads = prepared_payloads;
        self
    }

    pub fn prepared_payloads(&self) -> &PreparedSourcePayloads {
        &self.prepared_payloads
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
            .field("managed_execution", &self.execution.is_some())
            .field("prepared_payloads", &self.prepared_payloads)
            .finish()
    }
}

#[derive(Clone)]
pub struct RestDiscoveryDependencies<'a> {
    transport: &'a dyn HttpTransport,
    secret_provider: &'a dyn SecretProvider,
    memory: Arc<dyn MemoryCoordinator>,
    prepared_payloads: PreparedSourcePayloads,
}

impl<'a> RestDiscoveryDependencies<'a> {
    pub fn new(
        transport: &'a dyn HttpTransport,
        secret_provider: &'a dyn SecretProvider,
        memory: Arc<dyn MemoryCoordinator>,
    ) -> Self {
        Self {
            transport,
            secret_provider,
            memory,
            prepared_payloads: PreparedSourcePayloads::default(),
        }
    }

    pub fn with_prepared_payloads(mut self, prepared_payloads: PreparedSourcePayloads) -> Self {
        self.prepared_payloads = prepared_payloads;
        self
    }

    pub fn prepared_payloads(&self) -> &PreparedSourcePayloads {
        &self.prepared_payloads
    }
}

impl fmt::Debug for RestDiscoveryDependencies<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestDiscoveryDependencies")
            .field("transport", &"<explicit>")
            .field("secret_provider", &"<explicit>")
            .field("prepared_payloads", &self.prepared_payloads)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct RestResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    capabilities: ResourceCapabilities,
    plan: RestResourcePlan,
    type_policy_allowances: TypePolicyAllowances,
    dependencies: RestRuntimeDependencies,
}

#[derive(Clone, Debug)]
pub struct RestSampleSchemaDiscovery {
    pub schema: SchemaRef,
    pub source_identity: BTreeMap<String, String>,
    pub bytes_read: u64,
    pub records_read: u64,
}

struct PreparedRestPage {
    state: Arc<PreparedRestPageState>,
}

struct PreparedRestPageState {
    response: Mutex<Option<HttpResponse>>,
    _lease: MemoryLease,
}

impl RestResource {
    pub fn new(
        descriptor: ResourceDescriptor,
        schema: SchemaRef,
        capabilities: ResourceCapabilities,
        plan: RestResourcePlan,
        type_policy_allowances: TypePolicyAllowances,
        dependencies: RestRuntimeDependencies,
    ) -> Result<Self> {
        Ok(Self {
            descriptor,
            schema,
            capabilities,
            plan,
            type_policy_allowances,
            dependencies,
        })
    }

    pub fn validate_runtime_dependencies(&self) -> Result<()> {
        if self.plan.auth.is_some() && self.dependencies.secret_provider.is_none() {
            return Err(CdfError::auth(
                "REST resource auth requires an explicit SecretProvider runtime dependency",
            ));
        }
        if let Some(auth) = &self.plan.auth {
            let provider = self
                .dependencies
                .secret_provider
                .as_deref()
                .ok_or_else(|| {
                    CdfError::auth(
                        "REST resource auth requires an explicit SecretProvider runtime dependency",
                    )
                })?;
            let secret = provider.resolve(auth_secret_uri(auth))?;
            if secret.as_str()?.trim().is_empty() {
                return Err(CdfError::auth(
                    "REST resource auth secret resolved to an empty value",
                ));
            }
        }
        Ok(())
    }
}

fn auth_secret_uri(auth: &AuthScheme) -> &SecretUri {
    match auth {
        AuthScheme::Bearer { token_uri } => token_uri,
        AuthScheme::Header { value_uri, .. } => value_uri,
    }
}

impl ResourceStream for RestResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn validate_runtime_dependencies(&self) -> Result<()> {
        RestResource::validate_runtime_dependencies(self)
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.type_policy_allowances
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        rest_partition(&self.descriptor, &self.plan, request).map(|partition| vec![partition])
    }

    fn open(&self, partition: PartitionPlan) -> BoxFuture<'_, Result<OpenedPartitionStream>> {
        let descriptor = self.descriptor.clone();
        let schema = Arc::clone(&self.schema);
        let plan = self.plan.clone();
        let dependencies = self.dependencies.clone();

        Box::pin(async move {
            let execution = dependencies.execution.clone();
            let batches = match execution {
                Some(execution) => execution.run_blocking("rest-source.sync", move || {
                    execute_rest(&descriptor, schema, &plan, &partition, dependencies)
                })?,
                None => execute_rest(&descriptor, schema, &plan, &partition, dependencies)?,
            };
            let stream = Box::pin(stream::iter(batches.into_iter().map(Ok))) as BatchStream;
            Ok(OpenedPartitionStream::without_completion(stream))
        })
    }
}

impl QueryableResource for RestResource {
    fn capabilities(&self) -> &cdf_kernel::ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        let partition = rest_partition(&self.descriptor, &self.plan, request)?;
        let selected_cursor =
            selected_cursor_pushdown(&self.descriptor, &self.plan, request).map(|(index, _)| index);
        let mut pushed_predicates = Vec::new();
        let mut unsupported_predicates = Vec::new();
        for (index, predicate) in request.filters.iter().enumerate() {
            if selected_cursor == Some(index) {
                pushed_predicates.push(PushedPredicate {
                    predicate: predicate.clone(),
                    fidelity: self.plan.cursor_filter_fidelity.clone(),
                });
            } else {
                unsupported_predicates.push(predicate.clone());
            }
        }
        Ok(ScanPlan {
            plan_id: PlanId::new(format!("plan-{}", self.descriptor.resource_id))?,
            request: request.clone(),
            partitions: vec![partition],
            pushed_predicates,
            unsupported_predicates,
            estimated_rows: None,
            estimated_bytes: None,
            delivery_guarantee: delivery_guarantee(&self.descriptor),
        })
    }
}

pub fn cursor_pushdown_value(
    descriptor: &ResourceDescriptor,
    plan: &RestResourcePlan,
    expression: &Expression,
) -> Option<String> {
    let cursor = descriptor.cursor.as_ref()?;
    let cursor_param = plan.cursor_param.as_deref();
    let (field, operator, literal) = expression.comparison()?;
    if !matches!(operator, "gte" | "gt" | "eq")
        || (field != cursor.field && cursor_param.is_none_or(|param| field != param))
    {
        return None;
    }
    match literal {
        ExpressionLiteral::Boolean(value) => Some(value.to_string()),
        ExpressionLiteral::Signed(value) => Some(value.to_string()),
        ExpressionLiteral::Unsigned(value) => Some(value.to_string()),
        ExpressionLiteral::Float64Bits(bits) => Some(f64::from_bits(*bits).to_string()),
        ExpressionLiteral::String(value) if !value.is_empty() => Some(value.clone()),
        ExpressionLiteral::Null
        | ExpressionLiteral::String(_)
        | ExpressionLiteral::StringList(_) => None,
        _ => None,
    }
}

pub fn rest_partition(
    descriptor: &ResourceDescriptor,
    plan: &RestResourcePlan,
    request: &ScanRequest,
) -> Result<PartitionPlan> {
    if request.resource_id != descriptor.resource_id {
        return Err(CdfError::contract(format!(
            "scan request resource `{}` does not match REST resource `{}`",
            request.resource_id, descriptor.resource_id
        )));
    }
    let mut metadata = BTreeMap::from([
        ("kind".to_owned(), "rest".to_owned()),
        ("path".to_owned(), plan.path.clone()),
        ("resource_id".to_owned(), descriptor.resource_id.to_string()),
    ]);
    if let Some(pagination) = &plan.pagination {
        metadata.insert("pagination".to_owned(), pagination.kind().to_string());
    }
    if let Some(cursor) = &descriptor.cursor {
        metadata.insert("cursor_field".to_owned(), cursor.field.clone());
    }
    if let Some(cursor_param) = &plan.cursor_param
        && let Some((_, value)) = selected_cursor_pushdown(descriptor, plan, request)
    {
        metadata.insert(CURSOR_QUERY_PARAM_METADATA.to_owned(), cursor_param.clone());
        metadata.insert(CURSOR_QUERY_VALUE_METADATA.to_owned(), value);
    }
    Ok(PartitionPlan {
        partition_id: PartitionId::new("rest")?,
        scope: descriptor.state_scope.clone(),
        start_position: None,
        metadata,
    })
}

fn selected_cursor_pushdown(
    descriptor: &ResourceDescriptor,
    plan: &RestResourcePlan,
    request: &ScanRequest,
) -> Option<(usize, String)> {
    if plan.cursor_filter_fidelity == PushdownFidelity::Unsupported {
        return None;
    }
    request
        .filters
        .iter()
        .enumerate()
        .find_map(|(index, predicate)| {
            cursor_pushdown_value(descriptor, plan, &predicate.canonical_expression)
                .map(|value| (index, value))
        })
}

fn delivery_guarantee(descriptor: &ResourceDescriptor) -> DeliveryGuarantee {
    match descriptor.write_disposition {
        WriteDisposition::Append => DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        WriteDisposition::Merge => DeliveryGuarantee::EffectivelyOncePerKey,
        WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
    }
}

fn execute_rest(
    descriptor: &ResourceDescriptor,
    schema: SchemaRef,
    plan: &RestResourcePlan,
    partition: &PartitionPlan,
    dependencies: RestRuntimeDependencies,
) -> Result<Vec<Batch>> {
    validate_partition(descriptor, plan, partition)?;
    execution_schema_hash(descriptor)?;
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
    let mut reconciliation_plan = None::<String>;

    while let Some(url) = next_url {
        let prepared_key = prepared_rest_page_key(descriptor, plan, partition, &url)?;
        let (mut response, prepared_retention) = match dependencies
            .prepared_payloads
            .take(&prepared_key)?
        {
            Some(payload) => {
                let (prepared, retention) =
                    payload.into_typed::<PreparedRestPage>("REST first-page execution")?;
                let response = prepared
                    .state
                    .response
                    .lock()
                    .map_err(|_| CdfError::internal("prepared REST page was poisoned"))?
                    .take()
                    .ok_or_else(|| CdfError::internal("prepared REST page was already consumed"))?;
                (response, Some(retention))
            }
            None => (
                send_page(
                    &dependencies,
                    plan,
                    &url,
                    &mut auth_session,
                    &mut retry_budget,
                    &mut limiter,
                )?,
                None,
            ),
        };
        if prepared_retention.is_some() {
            limiter.observe_response(&response, 0);
        }
        let body = response
            .body()
            .ok_or_else(|| CdfError::data("REST HTTP response did not include a JSON body"))?;
        let decoded = decode_response_page(body, &plan.record_selector)?;
        response.page.item_count = decoded.records.len();
        response.page.fields = decoded.pagination_fields;

        if !decoded.records.is_empty() {
            let page = reconcile_rest_page(&schema, descriptor, partition, &decoded.records)?;
            if let Some(page_plan) = &page.schema_coercion_plan {
                if let Some(previous) = &reconciliation_plan
                    && previous != page_plan
                {
                    return Err(CdfError::data(
                        "REST pages produced inconsistent schema coercion plans",
                    ));
                }
                reconciliation_plan = Some(page_plan.clone());
            }
            let mut batch = Batch::from_record_batch(
                BatchId::new(format!(
                    "{}-{}-{:06}",
                    sanitize_id_part(descriptor.resource_id.as_str()),
                    sanitize_id_part(partition.partition_id.as_str()),
                    page_index + 1
                ))?,
                descriptor.resource_id.clone(),
                partition.partition_id.clone(),
                page.observed_schema_hash.clone(),
                page.record_batch,
            )?;
            batch
                .header
                .mark_materialized_output(&page.physical_schema)?;
            batch.header.source_position = page.source_position;
            batch.header.pre_contract_quarantine = page.pre_contract_quarantine;
            batch
                .header
                .extend_residual_candidates(page.residual_candidates);
            batch.header.mark_materialized_residuals_complete();
            batch.header.schema_coercion_plan = page.schema_coercion_plan;
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

pub fn discover_rest_sample_schema(
    descriptor: &ResourceDescriptor,
    plan: &RestResourcePlan,
    partition: &PartitionPlan,
    dependencies: &RestDiscoveryDependencies<'_>,
    request: &SourceDiscoveryRequest,
) -> Result<RestSampleSchemaDiscovery> {
    request.validate()?;
    validate_partition(descriptor, plan, partition)?;

    let mut auth_session = plan.auth.clone().map(AuthSession::new);
    let mut retry_budget = RetryBudget::new(RetryPolicy::default());
    let mut limiter = RateLimiter::new(plan.rate_limit.clone(), 0);
    let base_request_url = build_request_url(plan, partition)?;
    let paginator = plan.pagination.clone().map(Paginator::new);
    let url = match &paginator {
        Some(paginator) => paginator.first_request(&base_request_url).url,
        None => base_request_url,
    };
    let mut send_context = RestSendContext {
        transport: dependencies.transport,
        secret_provider: Some(dependencies.secret_provider),
        auth_refresh: None,
    };
    let response = send_page_with_transport(
        &mut send_context,
        plan,
        &url,
        &mut auth_session,
        &mut retry_budget,
        &mut limiter,
    )?;
    let body = response
        .body()
        .ok_or_else(|| CdfError::data("REST HTTP response did not include a JSON body"))?;
    let body_bytes = u64::try_from(body.len())
        .map_err(|_| CdfError::data("REST discovery response exceeds u64"))?;
    if body_bytes > request.maximum_bytes {
        return Err(CdfError::data(format!(
            "REST discovery response contains {body_bytes} bytes, exceeding the configured {}-byte discovery limit",
            request.maximum_bytes
        )));
    }
    let decoded = decode_response_page(body, &plan.record_selector)?;
    let sampled_records = decoded.records.len().min(
        usize::try_from(request.maximum_records)
            .map_err(|_| CdfError::data("REST discovery record limit exceeds usize"))?,
    );
    let schema = Arc::new(infer_rest_sample_schema(
        &decoded.records[..sampled_records],
    )?);
    let retained_bytes = body_bytes;
    let lease = reserve_blocking(
        Arc::clone(&dependencies.memory),
        &ReservationRequest::new(
            ConsumerKey::new("rest-discovery-retained-page", MemoryClass::Source)?,
            retained_bytes,
        )?,
    )?;
    let state = Arc::new(PreparedRestPageState {
        response: Mutex::new(Some(response)),
        _lease: lease,
    });
    let owner: Arc<dyn std::any::Any + Send + Sync> = state.clone();
    dependencies.prepared_payloads.install(
        prepared_rest_page_key(descriptor, plan, partition, &url)?,
        PreparedSourcePayload::new(
            PreparedRestPage { state },
            PayloadRetention::new(owner, retained_bytes)?,
        ),
    )?;
    let source_identity = BTreeMap::from([
        ("source_kind".to_owned(), "rest".to_owned()),
        ("path".to_owned(), plan.path.clone()),
        ("record_selector".to_owned(), plan.record_selector.clone()),
        ("sample_pages".to_owned(), "1".to_owned()),
        ("sample_records".to_owned(), sampled_records.to_string()),
    ]);
    Ok(RestSampleSchemaDiscovery {
        schema,
        source_identity,
        bytes_read: body_bytes,
        records_read: u64::try_from(sampled_records)
            .map_err(|_| CdfError::data("REST discovery sample record count exceeds u64"))?,
    })
}

fn prepared_rest_page_key(
    descriptor: &ResourceDescriptor,
    plan: &RestResourcePlan,
    partition: &PartitionPlan,
    url: &str,
) -> Result<PreparedSourcePayloadKey> {
    let payload_hash = artifact_hash(&serde_json::json!({
        "version": 1,
        "resource_id": descriptor.resource_id.as_str(),
        "partition_id": partition.partition_id.as_str(),
        "url": url,
        "record_selector": plan.record_selector,
        "pagination": plan.pagination.as_ref().map(|pagination| pagination.kind().to_string()),
    }))?;
    PreparedSourcePayloadKey::new(
        descriptor.resource_id.clone(),
        SourceDriverId::new("rest")?,
        payload_hash,
    )
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

fn execution_schema_hash(descriptor: &ResourceDescriptor) -> Result<SchemaHash> {
    match &descriptor.schema_source {
        SchemaSource::Declared { schema_hash, .. } => Ok(schema_hash.clone()),
        SchemaSource::Discovered { snapshot } => Ok(snapshot.schema_hash.clone()),
        SchemaSource::Discover | SchemaSource::Hints { .. } | SchemaSource::Contract { .. } => {
            Err(CdfError::data(
                "declarative REST execution requires a declared or discovered schema hash",
            ))
        }
    }
}

struct RestSendContext<'a> {
    transport: &'a dyn HttpTransport,
    secret_provider: Option<&'a dyn SecretProvider>,
    auth_refresh: Option<&'a Arc<Mutex<Box<dyn AuthRefreshHook + Send>>>>,
}

fn send_page(
    dependencies: &RestRuntimeDependencies,
    plan: &RestResourcePlan,
    url: &str,
    auth_session: &mut Option<AuthSession>,
    retry_budget: &mut RetryBudget,
    limiter: &mut RateLimiter,
) -> Result<HttpResponse> {
    let mut send_context = RestSendContext {
        transport: dependencies.transport.as_ref(),
        secret_provider: dependencies
            .secret_provider
            .as_deref()
            .map(|provider| provider as &dyn SecretProvider),
        auth_refresh: dependencies.auth_refresh.as_ref(),
    };
    send_page_with_transport(
        &mut send_context,
        plan,
        url,
        auth_session,
        retry_budget,
        limiter,
    )
}

fn send_page_with_transport(
    context: &mut RestSendContext<'_>,
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

        let response = send_page_once(
            context.transport,
            context.secret_provider,
            plan,
            url,
            auth_session,
        );
        let response = match response {
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
        };
        limiter.observe_response(&response, 0);

        if matches!(response.status, 401 | 403)
            && auth_session.is_some()
            && let Some(hook) = context.auth_refresh
        {
            let provider = context.secret_provider.ok_or_else(|| {
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

fn send_page_once(
    transport: &dyn HttpTransport,
    secret_provider: Option<&dyn SecretProvider>,
    plan: &RestResourcePlan,
    url: &str,
    auth_session: &mut Option<AuthSession>,
) -> Result<HttpResponse> {
    let mut request = HttpRequest::new(HttpMethod::Get, url.to_owned());
    validate_http_url(&request.url)?;
    plan.allowlist.check(&request)?;
    if let Some(session) = auth_session {
        let provider = secret_provider.ok_or_else(|| {
            CdfError::auth(
                "REST resource auth requires an explicit SecretProvider runtime dependency",
            )
        })?;
        session.apply(provider, &mut request)?;
    }
    send_with_policy(transport, &plan.allowlist, request)
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

fn infer_rest_sample_schema(records: &[Map<String, Value>]) -> Result<Schema> {
    if records.is_empty() {
        return Err(CdfError::data(
            "REST schema discovery selector yielded no records to sample",
        ));
    }
    let mut fields = BTreeMap::<String, InferredRestField>::new();
    let mut records_seen = 0_usize;
    for record in records {
        for field in fields.values_mut() {
            field.seen_in_current_record = false;
        }
        for (name, value) in record {
            match fields.entry(name.clone()) {
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    entry.get_mut().observe(name, value)?;
                }
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let mut field = InferredRestField {
                        nullable: records_seen > 0,
                        ..InferredRestField::default()
                    };
                    field.observe(name, value)?;
                    entry.insert(field);
                }
            }
        }
        for field in fields.values_mut() {
            if !field.seen_in_current_record {
                field.nullable = true;
            }
        }
        records_seen = records_seen.saturating_add(1);
    }
    let fields = fields
        .into_iter()
        .map(|(name, field)| Ok(Field::new(name, field.data_type()?, field.nullable)))
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new(fields))
}

#[derive(Clone, Debug, Default)]
struct InferredRestField {
    nullable: bool,
    kind: Option<InferredRestKind>,
    seen_in_current_record: bool,
}

impl InferredRestField {
    fn observe(&mut self, field: &str, value: &Value) -> Result<()> {
        self.seen_in_current_record = true;
        if value.is_null() {
            self.nullable = true;
            return Ok(());
        }
        let observed = InferredRestKind::from_value(field, value)?;
        self.kind = Some(match self.kind.take() {
            Some(current) => current.merge(field, observed)?,
            None => observed,
        });
        Ok(())
    }

    fn data_type(&self) -> Result<DataType> {
        Ok(match self.kind {
            Some(InferredRestKind::Boolean) => DataType::Boolean,
            Some(InferredRestKind::Int64) => DataType::Int64,
            Some(InferredRestKind::UInt64) => DataType::UInt64,
            Some(InferredRestKind::Float64) => DataType::Float64,
            Some(InferredRestKind::Utf8) | None => DataType::Utf8,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InferredRestKind {
    Boolean,
    Int64,
    UInt64,
    Float64,
    Utf8,
}

impl InferredRestKind {
    fn from_value(field: &str, value: &Value) -> Result<Self> {
        Ok(match value {
            Value::Bool(_) => Self::Boolean,
            Value::Number(number) if number.as_i64().is_some() => Self::Int64,
            Value::Number(number) if number.as_u64().is_some() => Self::UInt64,
            Value::Number(number) if number.as_f64().is_some() => Self::Float64,
            Value::Number(_) => {
                return Err(CdfError::data(format!(
                    "REST field `{field}` contains a number outside supported int64/uint64/float64 inference"
                )));
            }
            Value::String(_) | Value::Array(_) | Value::Object(_) => Self::Utf8,
            Value::Null => unreachable!("null handled before inference"),
        })
    }

    fn merge(self, field: &str, observed: Self) -> Result<Self> {
        use InferredRestKind::*;
        Ok(match (self, observed) {
            (Boolean, Boolean) => Boolean,
            (Int64, Int64) => Int64,
            (UInt64, UInt64) => UInt64,
            (Float64, Float64) => Float64,
            (Utf8, Utf8) => Utf8,
            (Utf8, _) | (_, Utf8) | (Boolean, _) | (_, Boolean) => Utf8,
            (Float64, _) | (_, Float64) => Float64,
            (Int64, UInt64) | (UInt64, Int64) => {
                return Err(CdfError::data(format!(
                    "REST field `{field}` mixes signed and unsigned integer values that cannot be inferred losslessly"
                )));
            }
        })
    }
}

struct ReconciledRestPage {
    record_batch: RecordBatch,
    physical_schema: Schema,
    observed_schema_hash: SchemaHash,
    source_position: Option<SourcePosition>,
    pre_contract_quarantine: Vec<cdf_kernel::PreContractQuarantineFact>,
    residual_candidates: Vec<cdf_kernel::PreContractResidualCandidate>,
    schema_coercion_plan: Option<String>,
}

fn reconcile_rest_page(
    schema: &SchemaRef,
    descriptor: &ResourceDescriptor,
    partition: &PartitionPlan,
    records: &[Map<String, Value>],
) -> Result<ReconciledRestPage> {
    let mut ndjson = Vec::new();
    for record in records {
        serde_json::to_writer(&mut ndjson, record)
            .map_err(|error| CdfError::data(format!("serialize REST record: {error}")))?;
        ndjson.push(b'\n');
    }
    let read_options = ReadOptions::new(
        descriptor.resource_id.clone(),
        partition.partition_id.clone(),
    )
    .with_batch_size(records.len().max(1))?;
    let read = read_ndjson_bytes_with_declared_schema(
        &ndjson,
        &read_options,
        &JsonOptions::default(),
        schema.clone(),
    )?;
    let [format_batch] = read.batches.as_slice() else {
        return Err(CdfError::internal(
            "REST page reconciliation expected exactly one format batch",
        ));
    };
    let mut residual_candidates = format_batch.header.residual_candidates().to_vec();
    add_missing_cursor_candidates(schema, descriptor, records, &mut residual_candidates)?;
    let cursor_identities = descriptor
        .cursor
        .as_ref()
        .and_then(|cursor| {
            schema
                .fields()
                .iter()
                .map(|field| field.as_ref())
                .find(|field| {
                    cursor.field == field.name().as_str()
                        || source_name(field).is_some_and(|source| cursor.field == source)
                })
        })
        .map(|field| {
            BTreeSet::from([
                field.name().clone(),
                source_name(field)
                    .unwrap_or_else(|| field.name().as_str())
                    .to_owned(),
            ])
        })
        .unwrap_or_default();
    let excluded_cursor_rows = residual_candidates
        .iter()
        .filter(|candidate| {
            candidate
                .source_path()
                .first()
                .is_some_and(|field| cursor_identities.contains(field))
        })
        .map(PreContractResidualCandidate::batch_row_ordinal)
        .collect::<BTreeSet<_>>();
    let source_position =
        rest_page_cursor_position(schema, descriptor, records, &excluded_cursor_rows)?;
    let mut pre_contract_quarantine = format_batch.header.pre_contract_quarantine.clone();
    for fact in &mut pre_contract_quarantine {
        fact.source_position = source_position.clone();
    }

    let record_batch = format_batch
        .record_batch()
        .ok_or_else(|| CdfError::internal("REST format batch has no Arrow payload"))?
        .clone();
    let physical_schema = if format_batch.header.observation_representation
        == cdf_kernel::PhysicalObservationRepresentation::MaterializedOutput
    {
        format_batch.header.materialized_physical_schema()?
    } else {
        record_batch.schema().as_ref().clone()
    };

    Ok(ReconciledRestPage {
        record_batch,
        physical_schema,
        observed_schema_hash: format_batch.header.observed_schema_hash.clone(),
        source_position,
        pre_contract_quarantine,
        residual_candidates,
        schema_coercion_plan: format_batch.header.schema_coercion_plan.clone(),
    })
}

fn rest_page_cursor_position(
    schema: &SchemaRef,
    descriptor: &ResourceDescriptor,
    records: &[Map<String, Value>],
    excluded_rows: &BTreeSet<usize>,
) -> Result<Option<SourcePosition>> {
    let Some(cursor_spec) = &descriptor.cursor else {
        return Ok(None);
    };
    if records.is_empty() {
        return Ok(None);
    }
    let field = schema
        .fields()
        .iter()
        .map(|field| field.as_ref())
        .find(|field| {
            cursor_spec.field == field.name().as_str()
                || source_name(field).is_some_and(|source| cursor_spec.field == source)
        })
        .ok_or_else(|| {
            CdfError::data(format!(
                "REST cursor field `{}` is missing from declared schema",
                cursor_spec.field
            ))
        })?;
    let Some(value) = max_cursor_for_field(field, records, excluded_rows)? else {
        return Ok(None);
    };
    Ok(Some(SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: cursor_spec.field.clone(),
        value: value.into_cursor_value(),
    })))
}

fn max_cursor_for_field(
    field: &arrow_schema::Field,
    records: &[Map<String, Value>],
    excluded_rows: &BTreeSet<usize>,
) -> Result<Option<ObservedCursor>> {
    let mut max_value = None;
    let key = source_name(field).unwrap_or_else(|| field.name().as_str());
    for (row, record) in records.iter().enumerate() {
        if excluded_rows.contains(&row) {
            continue;
        }
        let value = record.get(key).ok_or_else(|| {
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
    Ok(max_value)
}

fn add_missing_cursor_candidates(
    schema: &SchemaRef,
    descriptor: &ResourceDescriptor,
    records: &[Map<String, Value>],
    candidates: &mut Vec<PreContractResidualCandidate>,
) -> Result<()> {
    let Some(cursor) = &descriptor.cursor else {
        return Ok(());
    };
    let expected = schema
        .fields()
        .iter()
        .map(|field| field.as_ref())
        .find(|field| {
            cursor.field == field.name().as_str()
                || source_name(field).is_some_and(|source| cursor.field == source)
        })
        .ok_or_else(|| {
            CdfError::data(format!(
                "REST cursor candidate field `{}` is missing from declared schema fields {:?}",
                cursor.field,
                schema
                    .fields()
                    .iter()
                    .map(|field| (field.name(), source_name(field.as_ref())))
                    .collect::<Vec<_>>()
            ))
        })?;
    let source = source_name(expected).unwrap_or_else(|| expected.name().as_str());
    for (row, record) in records.iter().enumerate() {
        if record.get(source).is_some_and(|value| !value.is_null())
            || candidates.iter().any(|candidate| {
                candidate.batch_row_ordinal() == row
                    && candidate
                        .source_path()
                        .first()
                        .is_some_and(|field| field == source)
            })
        {
            continue;
        }
        candidates.push(PreContractResidualCandidate::new(
            row as u64,
            row,
            vec![source.to_owned()],
            Field::new(source, DataType::Null, true),
            Some(expected.clone()),
            new_null_array(&DataType::Null, 1),
            0,
        )?);
    }
    Ok(())
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

#[cfg(test)]
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
    use std::{
        sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::*;

    struct ConcurrentProbeTransport {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    impl HttpTransport for ConcurrentProbeTransport {
        fn send(&self, _request: HttpRequest) -> Result<HttpResponse> {
            let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(current, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(50));
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(HttpResponse::new(200))
        }
    }

    #[test]
    fn shared_rest_transport_does_not_serialize_independent_requests() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let dependencies = RestRuntimeDependencies::new(ConcurrentProbeTransport {
            active: Arc::clone(&active),
            peak: Arc::clone(&peak),
        });
        let start = Arc::new(Barrier::new(3));
        let workers = (0..2)
            .map(|index| {
                let transport = Arc::clone(&dependencies.transport);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    transport
                        .send(HttpRequest::new(
                            HttpMethod::Get,
                            format!("https://api.example.test/{index}"),
                        ))
                        .unwrap();
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(peak.load(Ordering::SeqCst), 2);
    }

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
            schema_source: SchemaSource::Discover,
            primary_key: Vec::new(),
            merge_key: Vec::new(),
            cursor: Some(cdf_kernel::CursorSpec {
                field: "updated_at".to_owned(),
                ordering: cdf_kernel::CursorOrderingClaim::Inexact,
                lag_tolerance_ms: 0,
            }),
            write_disposition: cdf_kernel::WriteDisposition::Append,
            deduplication: None,
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
            cursor_pushdown_value(
                &descriptor,
                &plan,
                &Expression::parse_comparison("updated_at >= \"2026-07-01T00:00:00Z\"").unwrap(),
            ),
            Some("2026-07-01T00:00:00Z".to_owned())
        );
        assert_eq!(
            cursor_pushdown_value(
                &descriptor,
                &plan,
                &Expression::parse_comparison("id = 1").unwrap(),
            ),
            None
        );
        assert_eq!(
            cursor_pushdown_value(
                &descriptor,
                &plan,
                &Expression::parse_comparison("not_updated_at >= \"2026-07-01\"").unwrap(),
            ),
            None
        );
        assert!(Expression::parse_comparison("updated_at >= checkpoint.cursor").is_err());

        let request = ScanRequest {
            resource_id: descriptor.resource_id.clone(),
            projection: None,
            filters: vec![
                cdf_kernel::ScanPredicate::new(
                    cdf_kernel::PredicateId::new("cursor-5").unwrap(),
                    "updated_at >= 5",
                )
                .unwrap(),
                cdf_kernel::ScanPredicate::new(
                    cdf_kernel::PredicateId::new("cursor-10").unwrap(),
                    "updated_at >= 10",
                )
                .unwrap(),
            ],
            limit: None,
            order_by: Vec::new(),
            scope: cdf_kernel::ScopeKey::Resource,
        };
        assert_eq!(
            selected_cursor_pushdown(&descriptor, &plan, &request),
            Some((0, "5".to_owned()))
        );
        let partition = rest_partition(&descriptor, &plan, &request).unwrap();
        assert_eq!(
            partition.metadata.get(CURSOR_QUERY_VALUE_METADATA),
            Some(&"5".to_owned())
        );
    }
}
