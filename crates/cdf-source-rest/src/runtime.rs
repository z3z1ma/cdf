use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::{REST_MAXIMUM_RESPONSE_BYTES, RestResourcePlan};
use arrow_array::{
    Array, ArrayRef, Date32Array, Float64Array, Int64Array, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, UInt64Array, new_null_array,
};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use cdf_http::{
    AuthRefreshHook, AuthScheme, AuthSession, HttpMethod, HttpRequest, HttpResponse,
    HttpResponseBudget, HttpTransport, PaginationKind, Paginator, RateLimiter, SecretProvider,
    SecretUri, classify_response, send_with_policy,
};
use cdf_kernel::{
    BackpressureSupport, Batch, BatchStream, BoxFuture, CapabilitySupport, CdfError,
    CompiledScanIntent, CursorPosition, CursorValue, DeliveryGuarantee, EffectiveSchemaRuntime,
    EstimateSupport, Expression, ExpressionLiteral, FilterCapabilities, IncrementalShape,
    PLAN_SCHEMA_OBSERVATION_BINDING_KEY, PLAN_SCHEMA_OBSERVATION_ID_KEY, PartitionId,
    PartitionPlan, PartitioningCapabilities, PayloadRetention, PlanId,
    PreContractResidualCandidate, PushdownFidelity, PushedPredicate, QueryableResource,
    ReplaySupport, ResourceCapabilities, ResourceDescriptor, ResourceStream, Result, ScanPlan,
    ScanRequest, SchemaHash, SchemaSource, ScopeKind, SourcePosition, TypePolicyAllowances,
    WriteDisposition, source_name,
};
use cdf_memory::MemoryCoordinator;
use cdf_runtime::{
    BoundedFormatRequest, CpuTaskSpec, DecodeSchemaPlan, ExecutionServices, FormatDiscoveryRequest,
    FormatDriver, MemoryByteSource, PreparedSourcePayload, PreparedSourcePayloadKey,
    PreparedSourcePayloads, ReadOptions, RunCancellation, SourceDiscoveryRequest, SourceDriverId,
    SourceEgressScope, artifact_hash, decode_bounded_format,
};

#[derive(Clone)]
pub struct RestRuntimeDependencies {
    transport: Arc<dyn HttpTransport>,
    secret_provider: Option<Arc<dyn SecretProvider + Send + Sync>>,
    auth_refresh: Option<Arc<Mutex<Box<dyn AuthRefreshHook + Send>>>>,
    execution: ExecutionServices,
    egress: SourceEgressScope,
    prepared_payloads: PreparedSourcePayloads,
}

impl RestRuntimeDependencies {
    pub fn new(
        transport: impl HttpTransport + 'static,
        execution: ExecutionServices,
        egress: SourceEgressScope,
    ) -> Self {
        Self::from_boxed_transport(Box::new(transport), execution, egress)
    }

    pub fn from_boxed_transport(
        transport: Box<dyn HttpTransport>,
        execution: ExecutionServices,
        egress: SourceEgressScope,
    ) -> Self {
        Self {
            transport: Arc::from(transport),
            secret_provider: None,
            auth_refresh: None,
            execution,
            egress,
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
}

impl fmt::Debug for RestRuntimeDependencies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestRuntimeDependencies")
            .field("transport", &"<explicit>")
            .field("secret_provider", &self.secret_provider.is_some())
            .field("auth_refresh", &self.auth_refresh.is_some())
            .field("managed_execution", &true)
            .field("egress", &self.egress)
            .field("prepared_payloads", &self.prepared_payloads)
            .finish()
    }
}

#[derive(Clone)]
pub struct RestDiscoveryDependencies<'a> {
    transport: &'a dyn HttpTransport,
    secret_provider: &'a (dyn SecretProvider + Send + Sync),
    execution: ExecutionServices,
    egress: SourceEgressScope,
    prepared_payloads: PreparedSourcePayloads,
}

impl<'a> RestDiscoveryDependencies<'a> {
    pub fn new(
        transport: &'a dyn HttpTransport,
        secret_provider: &'a (dyn SecretProvider + Send + Sync),
        execution: ExecutionServices,
        egress: SourceEgressScope,
    ) -> Self {
        Self {
            transport,
            secret_provider,
            execution,
            egress,
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
            .field("managed_execution", &true)
            .field("egress", &self.egress)
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
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    compiled_source_plan_hash: Option<String>,
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
            effective_schema_runtime: None,
            compiled_source_plan_hash: None,
        })
    }

    pub fn with_compiled_source_plan_hash(mut self, hash: String) -> Self {
        self.compiled_source_plan_hash = Some(hash);
        self
    }

    pub fn with_effective_schema_runtime(
        mut self,
        runtime: Option<EffectiveSchemaRuntime>,
    ) -> Self {
        self.effective_schema_runtime = runtime;
        self
    }

    pub fn validate_runtime_dependencies(&self) -> Result<()> {
        let requires_secrets = self.plan.auth.is_some()
            || self
                .plan
                .params
                .values()
                .any(|value| matches!(value, crate::RestParameterValue::Secret(_)));
        if requires_secrets && self.dependencies.secret_provider.is_none() {
            return Err(CdfError::auth(
                "REST resource secrets require an explicit SecretProvider runtime dependency",
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

    fn compiled_source_plan_hash(&self) -> Option<&str> {
        self.compiled_source_plan_hash.as_deref()
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_ref()
    }

    fn validate_runtime_dependencies(&self) -> Result<()> {
        RestResource::validate_runtime_dependencies(self)
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.type_policy_allowances
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        rest_partition(&self.descriptor, &self.plan, request).map(|mut partition| {
            partition.scan_intent = CompiledScanIntent::full_scan();
            vec![partition]
        })
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        let descriptor = self.descriptor.clone();
        let schema = Arc::clone(&self.schema);
        let plan = self.plan.clone();
        let dependencies = self.dependencies.clone();
        let execution = dependencies.execution.clone();
        let task = execution.spawn_cpu_stream(
            "rest-source-open",
            CpuTaskSpec {
                task_kind: "source.rest.decode".to_owned(),
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
            },
            1,
            move |sender, cancellation| async move {
                cancellation.check()?;
                execute_rest(
                    RestExecutionInvocation {
                        descriptor,
                        schema,
                        plan,
                        partition,
                        dependencies,
                        cancellation: cancellation.clone(),
                    },
                    sender,
                )
                .await?;
                cancellation.check()?;
                Ok(())
            },
        );
        let stream = match task {
            Ok(stream) => stream,
            Err(error) => {
                return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
                    Err(error)
                }));
            }
        };
        let termination = stream.termination();
        let opening = Box::pin(async move {
            let stream = Box::pin(stream) as BatchStream;
            Ok(cdf_kernel::PartitionStreamPayload::new(
                stream,
                Box::pin(async { Ok(cdf_kernel::PartitionCompletion::default()) }),
            ))
        });
        cdf_kernel::PartitionOpenAttempt::with_termination(opening, termination)
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
    let cursor_param = plan.cursor_param.as_deref()?;
    let (field, operator, literal) = expression.comparison()?;
    if !matches!(operator, "gte" | "gt" | "eq") || (field != cursor.field && field != cursor_param)
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
        (
            PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
            descriptor.resource_id.to_string(),
        ),
        (
            PLAN_SCHEMA_OBSERVATION_BINDING_KEY.to_owned(),
            artifact_hash(&serde_json::json!({
                "resource_id": descriptor.resource_id,
                "path": plan.path,
            }))?,
        ),
    ]);
    if let Some(pagination) = &plan.pagination {
        metadata.insert("pagination".to_owned(), pagination.kind().to_string());
    }
    if let Some(cursor) = &descriptor.cursor {
        metadata.insert("cursor_field".to_owned(), cursor.field.clone());
    }
    let selected_cursor = selected_cursor_pushdown(descriptor, plan, request);
    let predicates = selected_cursor
        .map(|(index, _)| PushedPredicate {
            predicate: request.filters[index].clone(),
            fidelity: plan.cursor_filter_fidelity.clone(),
        })
        .into_iter()
        .collect();
    let scan_intent = CompiledScanIntent {
        version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
        projection: None,
        predicates,
        limit: None,
        order_by: Vec::new(),
    };
    scan_intent.validate()?;
    Ok(PartitionPlan {
        partition_id: PartitionId::new("rest")?,
        scope: descriptor.state_scope.clone(),
        planned_position: None,
        start_position: None,
        scan_intent,
        retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
        metadata,
    })
}

fn selected_cursor_pushdown(
    descriptor: &ResourceDescriptor,
    plan: &RestResourcePlan,
    request: &ScanRequest,
) -> Option<(usize, String)> {
    if plan.cursor_param.is_none() || plan.cursor_filter_fidelity == PushdownFidelity::Unsupported {
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

pub fn rest_resource_capabilities(
    descriptor: &ResourceDescriptor,
    plan: &RestResourcePlan,
) -> ResourceCapabilities {
    let cursor_pushdown = descriptor.cursor.is_some()
        && plan.cursor_param.is_some()
        && plan.cursor_filter_fidelity != PushdownFidelity::Unsupported;
    ResourceCapabilities {
        projection: CapabilitySupport::Unsupported,
        filters: FilterCapabilities {
            default_fidelity: if cursor_pushdown {
                plan.cursor_filter_fidelity.clone()
            } else {
                PushdownFidelity::Unsupported
            },
            supported_operators: if cursor_pushdown {
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
        backpressure: BackpressureSupport::Pausable,
        estimates: EstimateSupport::None,
    }
}

fn delivery_guarantee(descriptor: &ResourceDescriptor) -> DeliveryGuarantee {
    match descriptor.write_disposition {
        WriteDisposition::Append => DeliveryGuarantee::AtLeastOnceDuplicateRisk,
        WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        WriteDisposition::Merge => DeliveryGuarantee::EffectivelyOncePerKey,
        WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
    }
}

struct RestExecutionInvocation {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    plan: RestResourcePlan,
    partition: PartitionPlan,
    dependencies: RestRuntimeDependencies,
    cancellation: RunCancellation,
}

struct PendingRestPage {
    url: String,
    response: Option<HttpResponse>,
}

async fn execute_rest(
    invocation: RestExecutionInvocation,
    mut sender: cdf_runtime::TaskStreamSender<Batch>,
) -> Result<()> {
    let RestExecutionInvocation {
        descriptor,
        schema,
        plan,
        partition,
        dependencies,
        cancellation,
    } = invocation;
    let descriptor = &descriptor;
    let plan = &plan;
    let partition = &partition;
    validate_partition(descriptor, plan, partition)?;
    execution_schema_hash(descriptor)?;
    if schema.fields().is_empty() {
        return Err(CdfError::data(
            "declarative REST execution requires a non-empty compiled schema",
        ));
    }

    let mut limiter = response_quota_limiter(plan);
    let mut auth_session = plan.auth.clone().map(AuthSession::new);
    let mut paginator = plan.pagination.clone().map(Paginator::new);
    let pagination_kind = plan.pagination.as_ref().map(|pagination| pagination.kind());
    let base_request_url = build_request_url(
        descriptor,
        plan,
        partition,
        dependencies
            .secret_provider
            .as_deref()
            .map(|provider| provider as &dyn SecretProvider),
    )?;
    let first_url = match &paginator {
        Some(paginator) => paginator.first_request(&base_request_url).url,
        None => base_request_url,
    };
    let mut next_page = Some(PendingRestPage {
        url: first_url,
        response: None,
    });
    let mut page_index = 0_usize;
    while let Some(PendingRestPage {
        url,
        response: prefetched_response,
    }) = next_page.take()
    {
        let prepared_key = prepared_rest_page_key(descriptor, plan, partition, &url)?;
        let (mut response, _prepared_retention) = match prefetched_response {
            Some(response) => (response, None),
            None => match dependencies.prepared_payloads.take(&prepared_key)? {
                Some(payload) => {
                    let (prepared, retention) =
                        payload.into_typed::<PreparedRestPage>("REST first-page execution")?;
                    let response = prepared
                        .state
                        .response
                        .lock()
                        .map_err(|_| CdfError::internal("prepared REST page was poisoned"))?
                        .take()
                        .ok_or_else(|| {
                            CdfError::internal("prepared REST page was already consumed")
                        })?;
                    (response, Some(retention))
                }
                None => (
                    send_page(
                        &dependencies,
                        plan,
                        &url,
                        &mut auth_session,
                        &mut limiter,
                        &cancellation,
                    )
                    .await?,
                    None,
                ),
            },
        };
        let body = response
            .accounted_body()
            .ok_or_else(|| CdfError::data("REST HTTP response did not include a JSON body"))?;
        let body_bytes = u64::try_from(body.payload().len())
            .map_err(|_| CdfError::data("REST response body exceeds u64"))?;
        if body_bytes > REST_MAXIMUM_RESPONSE_BYTES {
            return Err(CdfError::data(format!(
                "REST response page contains {body_bytes} bytes above the compiled {REST_MAXIMUM_RESPONSE_BYTES}-byte page limit; configure smaller pages on the source endpoint"
            )));
        }
        let selection =
            cdf_format_json::select_bounded_json_records(body.payload(), &plan.record_selector)?;
        let selected = body.slice(selection.byte_range)?;
        response.page.fields = selection.top_level_scalar_fields;
        let prefetch_url = predecode_next_rest_url(
            paginator.as_mut(),
            pagination_kind,
            &url,
            &mut response,
            selection.records_present,
        );
        let decode = decode_selected_rest_page(
            Arc::clone(&schema),
            descriptor,
            partition,
            selected,
            page_index,
            &dependencies.execution,
            cancellation.clone(),
        );
        let prefetch = match prefetch_url {
            Some(next_url) => {
                let fetch_url = next_url.clone();
                let dependencies = &dependencies;
                let auth_session = &mut auth_session;
                let limiter = &mut limiter;
                let cancellation = &cancellation;
                let fetch: BoxFuture<'_, Result<HttpResponse>> = Box::pin(async move {
                    send_page(
                        dependencies,
                        plan,
                        &fetch_url,
                        auth_session,
                        limiter,
                        cancellation,
                    )
                    .await
                });
                Some((next_url, fetch))
            }
            None => None,
        };
        let (mut batches, prefetched_page) =
            decode_with_prefetch(Box::pin(decode), prefetch).await?;
        response.page.item_count = batches.iter().try_fold(0_usize, |total, batch| {
            let rows = usize::try_from(batch.header.row_count)
                .map_err(|_| CdfError::data("REST batch row count exceeds usize"))?;
            total
                .checked_add(rows)
                .ok_or_else(|| CdfError::data("REST page row count overflowed"))
        })?;
        let mut page_row_offset = 0_u64;
        for mut batch in batches.drain(..) {
            if batch.header.row_count == 0 {
                continue;
            }
            add_missing_cursor_candidates_from_batch(
                &schema,
                descriptor,
                &mut batch,
                page_row_offset,
            )?;
            batch.header.source_position = rest_batch_cursor_position(&schema, descriptor, &batch)?;
            page_row_offset = page_row_offset
                .checked_add(batch.header.row_count)
                .ok_or_else(|| CdfError::data("REST page row ordinal overflowed"))?;
            sender.send(batch).await?;
        }

        page_index = page_index.saturating_add(1);
        next_page = prefetched_page.or_else(|| {
            (pagination_kind == Some(PaginationKind::Offset))
                .then(|| {
                    paginator
                        .as_mut()
                        .and_then(|paginator| paginator.next_request(&url, &response))
                        .map(|request| PendingRestPage {
                            url: request.url,
                            response: None,
                        })
                })
                .flatten()
        });
    }

    Ok(())
}

async fn decode_with_prefetch(
    decode: BoxFuture<'_, Result<Vec<Batch>>>,
    prefetch: Option<(String, BoxFuture<'_, Result<HttpResponse>>)>,
) -> Result<(Vec<Batch>, Option<PendingRestPage>)> {
    match prefetch {
        Some((url, fetch)) => {
            let (batches, response) = futures_util::try_join!(decode, fetch)?;
            Ok((
                batches,
                Some(PendingRestPage {
                    url,
                    response: Some(response),
                }),
            ))
        }
        None => Ok((decode.await?, None)),
    }
}

fn predecode_next_rest_url(
    paginator: Option<&mut Paginator>,
    pagination_kind: Option<PaginationKind>,
    current_url: &str,
    response: &mut HttpResponse,
    records_present: bool,
) -> Option<String> {
    match pagination_kind? {
        PaginationKind::Offset => return None,
        PaginationKind::Page => response.page.item_count = usize::from(records_present),
        PaginationKind::Cursor | PaginationKind::LinkHeader | PaginationKind::NextToken => {}
    }
    paginator?
        .next_request(current_url, response)
        .map(|request| request.url)
}

pub async fn discover_rest_sample_schema(
    descriptor: &ResourceDescriptor,
    plan: &RestResourcePlan,
    partition: &PartitionPlan,
    dependencies: &RestDiscoveryDependencies<'_>,
    request: &SourceDiscoveryRequest,
) -> Result<RestSampleSchemaDiscovery> {
    request.validate()?;
    validate_partition(descriptor, plan, partition)?;

    let mut auth_session = plan.auth.clone().map(AuthSession::new);
    let mut limiter = response_quota_limiter(plan);
    let base_request_url = build_request_url(
        descriptor,
        plan,
        partition,
        Some(dependencies.secret_provider),
    )?;
    let paginator = plan.pagination.clone().map(Paginator::new);
    let url = match &paginator {
        Some(paginator) => paginator.first_request(&base_request_url).url,
        None => base_request_url,
    };
    let mut send_context = RestSendContext {
        transport: dependencies.transport,
        secret_provider: Some(dependencies.secret_provider),
        auth_refresh: None,
        egress: &dependencies.egress,
        maximum_response_bytes: request.maximum_bytes,
        memory: dependencies.execution.memory(),
        cancellation: request.cancellation.clone(),
        execution: &dependencies.execution,
    };
    let response = send_page_with_transport(
        &mut send_context,
        plan,
        &url,
        &mut auth_session,
        &mut limiter,
    )
    .await?;
    let body = response
        .accounted_body()
        .ok_or_else(|| CdfError::data("REST HTTP response did not include a JSON body"))?;
    let body_bytes = u64::try_from(body.payload().len())
        .map_err(|_| CdfError::data("REST discovery response exceeds u64"))?;
    if body_bytes > request.maximum_bytes {
        return Err(CdfError::data(format!(
            "REST discovery response contains {body_bytes} bytes, exceeding the configured {}-byte discovery limit",
            request.maximum_bytes
        )));
    }
    let selection =
        cdf_format_json::select_bounded_json_records(body.payload(), &plan.record_selector)?;
    let selected = body.slice(selection.byte_range)?;
    let selected_bytes = u64::try_from(selected.payload().len())
        .map_err(|_| CdfError::data("REST selected JSON body exceeds u64"))?;
    let source = Arc::new(MemoryByteSource::from_ephemeral_accounted_bytes(
        format!("rest-discovery:{}", descriptor.resource_id),
        selected,
    )?);
    let observation = cdf_format_json::JsonDocumentFormatDriver::new()?
        .discover(
            source,
            FormatDiscoveryRequest {
                options: serde_json::json!({}),
                maximum_bytes: selected_bytes,
                maximum_records: request.maximum_records,
                memory: dependencies.execution.memory(),
                cancellation: request.cancellation.clone(),
            },
        )
        .await?;
    let sampled_records = observation.sampled_records;
    let schema = observation.arrow_schema;
    let retained_bytes = body_bytes;
    let state = Arc::new(PreparedRestPageState {
        response: Mutex::new(Some(response)),
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
        records_read: sampled_records,
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
    secret_provider: Option<&'a (dyn SecretProvider + Send + Sync)>,
    auth_refresh: Option<&'a Arc<Mutex<Box<dyn AuthRefreshHook + Send>>>>,
    egress: &'a SourceEgressScope,
    maximum_response_bytes: u64,
    memory: Arc<dyn MemoryCoordinator>,
    cancellation: RunCancellation,
    execution: &'a ExecutionServices,
}

async fn send_page(
    dependencies: &RestRuntimeDependencies,
    plan: &RestResourcePlan,
    url: &str,
    auth_session: &mut Option<AuthSession>,
    limiter: &mut RateLimiter,
    cancellation: &RunCancellation,
) -> Result<HttpResponse> {
    let mut send_context = RestSendContext {
        transport: dependencies.transport.as_ref(),
        secret_provider: dependencies.secret_provider.as_deref(),
        auth_refresh: dependencies.auth_refresh.as_ref(),
        egress: &dependencies.egress,
        maximum_response_bytes: REST_MAXIMUM_RESPONSE_BYTES,
        memory: dependencies.execution.memory(),
        cancellation: cancellation.clone(),
        execution: &dependencies.execution,
    };
    send_page_with_transport(&mut send_context, plan, url, auth_session, limiter).await
}

async fn send_page_with_transport(
    context: &mut RestSendContext<'_>,
    plan: &RestResourcePlan,
    url: &str,
    auth_session: &mut Option<AuthSession>,
    limiter: &mut RateLimiter,
) -> Result<HttpResponse> {
    async fn send_once_with_rate_limit(
        context: &RestSendContext<'_>,
        plan: &RestResourcePlan,
        url: &str,
        auth_session: &mut Option<AuthSession>,
        limiter: &mut RateLimiter,
    ) -> Result<HttpResponse> {
        let rate_limit = plan
            .rate_limit
            .requests_per_minute
            .map(|requests_per_minute| cdf_runtime::SourceRateLimit {
                operations: u64::from(requests_per_minute),
                interval_ms: 60_000,
            });
        context.execution.admit_source_operation(
            &plan.quota_authority,
            rate_limit,
            context.cancellation.clone(),
        )?;
        let response = send_page_once(context, plan, url, auth_session).await?;
        let quota = limiter.observe_response(
            &response,
            duration_millis(context.execution.monotonic_now()),
            duration_millis(context.execution.unix_now()),
        );
        if !quota.allowed {
            context.execution.defer_source_operations(
                &plan.quota_authority,
                Duration::from_millis(quota.wait_ms),
            )?;
        }
        Ok(response)
    }

    let mut response = send_once_with_rate_limit(context, plan, url, auth_session, limiter).await?;
    if matches!(response.status, 401 | 403)
        && auth_session.is_some()
        && let Some(hook) = context.auth_refresh
    {
        let provider = context.secret_provider.ok_or_else(|| {
            CdfError::auth(
                "REST auth refresh requires an explicit SecretProvider runtime dependency",
            )
        })?;
        {
            let mut hook = hook.lock().map_err(|_| {
                CdfError::internal("REST auth refresh hook mutex was poisoned during refresh")
            })?;
            auth_session
                .as_mut()
                .expect("checked auth session availability")
                .refresh_once(provider, &mut **hook)?;
        }
        response = send_once_with_rate_limit(context, plan, url, auth_session, limiter).await?;
    }

    if let Some(error) = classify_response(&response) {
        return Err(error);
    }
    Ok(response)
}

fn response_quota_limiter(plan: &RestResourcePlan) -> RateLimiter {
    RateLimiter::new(
        cdf_http::RateLimitPolicy {
            requests_per_minute: None,
            quota_headers: plan.rate_limit.quota_headers.clone(),
        },
        0,
    )
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

async fn send_page_once(
    context: &RestSendContext<'_>,
    plan: &RestResourcePlan,
    url: &str,
    auth_session: &mut Option<AuthSession>,
) -> Result<HttpResponse> {
    let mut request = HttpRequest::new(HttpMethod::Get, url.to_owned());
    validate_http_url(&request.url)?;
    context.egress.authorize(&request.url)?;
    plan.allowlist.check(&request)?;
    if let Some(session) = auth_session {
        let provider = context.secret_provider.ok_or_else(|| {
            CdfError::auth(
                "REST resource auth requires an explicit SecretProvider runtime dependency",
            )
        })?;
        session.apply(provider, &mut request)?;
    }
    let run_cancellation = context.cancellation.clone();
    let cancellation: Arc<dyn cdf_http::HttpCancellation> =
        Arc::new(move || run_cancellation.check());
    let budget = HttpResponseBudget::new(
        context.maximum_response_bytes,
        Arc::clone(&context.memory),
        cancellation,
    )?;
    send_with_policy(context.transport, &plan.allowlist, request, budget).await
}

async fn decode_selected_rest_page(
    schema: SchemaRef,
    descriptor: &ResourceDescriptor,
    partition: &PartitionPlan,
    selected: cdf_memory::AccountedBytes,
    page_index: usize,
    execution: &ExecutionServices,
    cancellation: RunCancellation,
) -> Result<Vec<Batch>> {
    let source = Arc::new(MemoryByteSource::from_ephemeral_accounted_bytes(
        format!(
            "rest-page:{}:{}:{}",
            descriptor.resource_id, partition.partition_id, page_index
        ),
        selected,
    )?);
    let read_options = ReadOptions::new(
        descriptor.resource_id.clone(),
        partition.partition_id.clone(),
    )
    .with_batch_id_prefix(format!(
        "{}-{}-p{:06}",
        sanitize_id_part(descriptor.resource_id.as_str()),
        sanitize_id_part(partition.partition_id.as_str()),
        page_index + 1
    ))?;
    let decoded = decode_bounded_format(
        Arc::new(cdf_format_json::JsonDocumentFormatDriver::new()?),
        source,
        BoundedFormatRequest::new(read_options, execution.memory())
            .with_schema(DecodeSchemaPlan::fixed_admission(schema))
            .with_cancellation(cancellation),
    )
    .await?;
    Ok(decoded.batches)
}

fn cursor_field<'a>(
    schema: &'a SchemaRef,
    descriptor: &ResourceDescriptor,
) -> Result<Option<&'a Field>> {
    let Some(cursor) = &descriptor.cursor else {
        return Ok(None);
    };
    schema
        .fields()
        .iter()
        .map(|field| field.as_ref())
        .find(|field| {
            cursor.field == field.name().as_str()
                || source_name(field).is_some_and(|source| cursor.field == source)
        })
        .map(Some)
        .ok_or_else(|| {
            CdfError::data(format!(
                "REST cursor field `{}` is missing from the compiled schema",
                cursor.field
            ))
        })
}

fn cursor_array<'a>(batch: &'a Batch, field: &Field) -> Result<&'a ArrayRef> {
    let record_batch = batch
        .record_batch()
        .ok_or_else(|| CdfError::internal("REST format batch has no Arrow payload"))?;
    let source = source_name(field).unwrap_or_else(|| field.name());
    record_batch
        .column_by_name(source)
        .or_else(|| record_batch.column_by_name(field.name()))
        .ok_or_else(|| {
            CdfError::data(format!(
                "REST cursor field {:?} is missing from a decoded batch",
                field.name()
            ))
        })
}

fn add_missing_cursor_candidates_from_batch(
    schema: &SchemaRef,
    descriptor: &ResourceDescriptor,
    batch: &mut Batch,
    page_row_offset: u64,
) -> Result<()> {
    let Some(field) = cursor_field(schema, descriptor)? else {
        return Ok(());
    };
    let source = source_name(field)
        .unwrap_or_else(|| field.name())
        .to_owned();
    let null_rows = {
        let array = cursor_array(batch, field)?;
        (0..array.len())
            .filter(|row| array.is_null(*row))
            .collect::<Vec<_>>()
    };
    let existing = batch
        .header
        .residual_candidates()
        .iter()
        .filter(|candidate| candidate.source_path().first() == Some(&source))
        .map(PreContractResidualCandidate::batch_row_ordinal)
        .collect::<BTreeSet<_>>();
    let mut candidates = Vec::new();
    for row in null_rows {
        if existing.contains(&row) {
            continue;
        }
        let row = u64::try_from(row).map_err(|_| CdfError::data("REST cursor row exceeds u64"))?;
        candidates.push(PreContractResidualCandidate::new(
            page_row_offset
                .checked_add(row)
                .ok_or_else(|| CdfError::data("REST cursor row ordinal overflowed"))?,
            usize::try_from(row).map_err(|_| CdfError::data("REST cursor row exceeds usize"))?,
            vec![source.clone()],
            Field::new(&source, DataType::Null, true),
            Some(field.clone()),
            new_null_array(&DataType::Null, 1),
            0,
        )?);
    }
    batch.header.extend_residual_candidates(candidates);
    Ok(())
}

fn rest_batch_cursor_position(
    schema: &SchemaRef,
    descriptor: &ResourceDescriptor,
    batch: &Batch,
) -> Result<Option<SourcePosition>> {
    let Some(cursor) = &descriptor.cursor else {
        return Ok(None);
    };
    let field = cursor_field(schema, descriptor)?
        .ok_or_else(|| CdfError::internal("REST cursor authority disappeared"))?;
    let array = cursor_array(batch, field)?;
    let source = source_name(field).unwrap_or_else(|| field.name());
    let excluded = batch
        .header
        .residual_candidates()
        .iter()
        .filter(|candidate| candidate.source_path().first().map(String::as_str) == Some(source))
        .map(PreContractResidualCandidate::batch_row_ordinal)
        .collect::<BTreeSet<_>>();
    let mut maximum = None::<ObservedCursor>;
    for row in 0..array.len() {
        if excluded.contains(&row) {
            continue;
        }
        if array.is_null(row) {
            return Err(CdfError::data(format!(
                "REST cursor field {:?} is null in an accepted record",
                field.name()
            )));
        }
        let value = cursor_value_from_array(field, array, row)?;
        if maximum
            .as_ref()
            .is_none_or(|current| value.greater_than(current))
        {
            maximum = Some(value);
        }
    }
    Ok(maximum.map(|value| {
        SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: cursor.field.clone(),
            value: value.into_cursor_value(),
        })
    }))
}

fn cursor_value_from_array(field: &Field, array: &ArrayRef, row: usize) -> Result<ObservedCursor> {
    macro_rules! primitive {
        ($array:ty, $variant:ident) => {
            ObservedCursor::$variant(
                array
                    .as_any()
                    .downcast_ref::<$array>()
                    .ok_or_else(|| CdfError::internal("REST cursor Arrow type diverged"))?
                    .value(row),
            )
        };
    }
    Ok(match field.data_type() {
        DataType::Utf8 => ObservedCursor::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CdfError::internal("REST cursor Arrow type diverged"))?
                .value(row)
                .to_owned(),
        ),
        DataType::LargeUtf8 => ObservedCursor::String(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| CdfError::internal("REST cursor Arrow type diverged"))?
                .value(row)
                .to_owned(),
        ),
        DataType::Int64 => primitive!(Int64Array, I64),
        DataType::UInt64 => primitive!(UInt64Array, U64),
        DataType::Float64 => primitive!(Float64Array, F64),
        DataType::Date32 => ObservedCursor::I64(i64::from(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| CdfError::internal("REST cursor Arrow type diverged"))?
                .value(row),
        )),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => ObservedCursor::TimestampMicros {
            micros: array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| CdfError::internal("REST cursor Arrow type diverged"))?
                .value(row)
                .saturating_mul(1_000),
            timezone: timezone.as_ref().map(ToString::to_string),
        },
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => ObservedCursor::TimestampMicros {
            micros: array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| CdfError::internal("REST cursor Arrow type diverged"))?
                .value(row),
            timezone: timezone.as_ref().map(ToString::to_string),
        },
        other => {
            return Err(CdfError::data(format!(
                "REST cursor field {:?} has unsupported Arrow type {other}",
                field.name()
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

fn build_request_url(
    descriptor: &ResourceDescriptor,
    plan: &RestResourcePlan,
    partition: &PartitionPlan,
    secret_provider: Option<&dyn SecretProvider>,
) -> Result<String> {
    let mut url = join_base_url_and_path(&plan.base_url, &plan.path)?;
    for (name, value) in &plan.params {
        match value {
            crate::RestParameterValue::Literal(value) => {
                url = append_query_param(&url, name, value);
            }
            crate::RestParameterValue::Secret(uri) => {
                let provider = secret_provider.ok_or_else(|| {
                    CdfError::auth(format!(
                        "REST parameter `{name}` requires an explicit SecretProvider runtime dependency"
                    ))
                })?;
                let value = provider.resolve(uri)?;
                url = append_query_param(&url, name, value.as_str()?);
            }
        }
    }
    partition.scan_intent.validate()?;
    if partition.scan_intent.predicates.len() > 1 {
        return Err(CdfError::contract(
            "REST compiled scan intent may push at most one cursor predicate",
        ));
    }
    if let Some(pushed) = partition.scan_intent.predicates.first() {
        if pushed.fidelity != plan.cursor_filter_fidelity
            || pushed.fidelity == PushdownFidelity::Unsupported
        {
            return Err(CdfError::contract(
                "REST compiled cursor predicate fidelity does not match the adapter plan",
            ));
        }
        let param = plan.cursor_param.as_deref().ok_or_else(|| {
            CdfError::contract(
                "REST compiled cursor predicate requires the resource cursor query parameter",
            )
        })?;
        let value = cursor_pushdown_value(descriptor, plan, &pushed.predicate.canonical_expression)
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "REST compiled cursor predicate `{:?}` is not executable by the adapter",
                    pushed.predicate.canonical_expression
                ))
            })?;
        url = append_query_param(&url, param, &value);
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
    if path.contains(['?', '#']) {
        return Err(CdfError::contract(
            "REST resource path must not include query parameters or a fragment",
        ));
    }
    if url::Url::parse(base_url).is_ok_and(|url| url.query().is_some()) {
        return Err(CdfError::contract(
            "REST base_url must not include query parameters",
        ));
    }

    if path.is_empty() {
        Ok(base_url.to_owned())
    } else if path.starts_with('/') {
        Ok(format!("{}{}", origin(base_url)?, path))
    } else {
        Ok(format!(
            "{}/{}",
            base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        ))
    }
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
    if url
        .split_once("://")
        .is_none_or(|(_, authority)| authority.starts_with('/'))
    {
        return Err(CdfError::contract("REST request URL must include a host"));
    }
    let parsed = url::Url::parse(url)
        .map_err(|error| CdfError::contract(format!("invalid REST request URL: {error}")))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(CdfError::contract(
            "REST request URL must use the http or https scheme",
        ));
    }
    if parsed.host_str().is_none() {
        return Err(CdfError::contract("REST request URL must include a host"));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(CdfError::contract(
            "REST request URL must not contain user information",
        ));
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
            mpsc,
        },
        task::{Poll, Waker},
        time::Duration,
    };

    use super::*;

    struct TestExecutionHost {
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn cdf_runtime::SpillBudgetCoordinator>,
    }

    impl cdf_runtime::ExecutionHost for TestExecutionHost {
        fn capabilities(&self) -> cdf_runtime::ExecutionHostCapabilities {
            cdf_runtime::ExecutionHostCapabilities {
                logical_cpu_slots: 1,
                io_workers: 1,
                blocking_lanes: Vec::new(),
            }
        }

        fn memory(&self) -> Arc<dyn MemoryCoordinator> {
            Arc::clone(&self.memory)
        }

        fn spill(&self) -> Arc<dyn cdf_runtime::SpillBudgetCoordinator> {
            Arc::clone(&self.spill)
        }

        fn open_scope(&self, _run_id: &str) -> Result<Box<dyn cdf_runtime::ExecutionTaskScope>> {
            Err(CdfError::internal("REST unit host does not open scopes"))
        }

        fn run_io_blocking(&self, _task: cdf_runtime::IoValueTask) -> Result<cdf_runtime::IoValue> {
            Err(CdfError::internal("REST unit host does not run I/O"))
        }

        fn delay(
            &self,
            _duration: Duration,
            cancellation: RunCancellation,
        ) -> cdf_kernel::BoxFuture<'static, Result<()>> {
            Box::pin(async move { cancellation.check() })
        }

        fn monotonic_now(&self) -> Duration {
            Duration::ZERO
        }

        fn unix_now(&self) -> Duration {
            Duration::ZERO
        }

        fn entropy_u64(&self) -> u64 {
            0
        }

        fn ensure_blocking_lanes(&self, _lanes: &[cdf_runtime::BlockingLaneSpec]) -> Result<()> {
            Ok(())
        }

        fn run_blocking_value(
            &self,
            _lane: &str,
            task: cdf_runtime::BlockingValueTask,
        ) -> Result<cdf_runtime::IoValue> {
            task()
        }
    }

    fn test_execution_services() -> ExecutionServices {
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            cdf_memory::DeterministicMemoryCoordinator::new(
                crate::REST_MAXIMUM_DECODE_BYTES,
                BTreeMap::new(),
            )
            .unwrap(),
        );
        let spill: Arc<dyn cdf_runtime::SpillBudgetCoordinator> =
            Arc::new(cdf_runtime::FixedSpillBudget::new(64 * 1024 * 1024).unwrap());
        ExecutionServices::new(Arc::new(TestExecutionHost { memory, spill })).unwrap()
    }

    fn test_http_budget() -> HttpResponseBudget {
        let memory: Arc<dyn MemoryCoordinator> = Arc::new(
            cdf_memory::DeterministicMemoryCoordinator::new(
                REST_MAXIMUM_RESPONSE_BYTES,
                BTreeMap::new(),
            )
            .unwrap(),
        );
        HttpResponseBudget::new(REST_MAXIMUM_RESPONSE_BYTES, memory, Arc::new(|| Ok(()))).unwrap()
    }

    struct ConcurrentProbeTransport {
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    struct PollGate {
        started: std::sync::atomic::AtomicBool,
        signal: mpsc::Sender<()>,
        state: Mutex<PollGateState>,
    }

    struct PollGateState {
        released: bool,
        waker: Option<Waker>,
    }

    impl PollGate {
        fn new(signal: mpsc::Sender<()>) -> Arc<Self> {
            Arc::new(Self {
                started: std::sync::atomic::AtomicBool::new(false),
                signal,
                state: Mutex::new(PollGateState {
                    released: false,
                    waker: None,
                }),
            })
        }

        fn future<T: Send + 'static>(self: Arc<Self>, value: T) -> BoxFuture<'static, Result<T>> {
            let mut value = Some(value);
            Box::pin(futures_util::future::poll_fn(move |context| {
                if !self.started.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    self.signal.send(()).unwrap();
                }
                let mut state = self.state.lock().unwrap();
                if state.released {
                    Poll::Ready(Ok(value.take().unwrap()))
                } else {
                    state.waker = Some(context.waker().clone());
                    Poll::Pending
                }
            }))
        }

        fn release(&self) {
            let waker = {
                let mut state = self.state.lock().unwrap();
                state.released = true;
                state.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    fn delayed_future<T: Send + 'static>(
        delay: Duration,
        value: T,
    ) -> BoxFuture<'static, Result<T>> {
        struct State<T> {
            value: Option<T>,
            ready: bool,
            waker: Option<Waker>,
        }

        let state = Arc::new(Mutex::new(State {
            value: Some(value),
            ready: false,
            waker: None,
        }));
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        Box::pin(futures_util::future::poll_fn(move |context| {
            if !started.swap(true, std::sync::atomic::Ordering::SeqCst) {
                let state = Arc::clone(&state);
                std::thread::spawn(move || {
                    std::thread::sleep(delay);
                    let waker = {
                        let mut state = state.lock().unwrap();
                        state.ready = true;
                        state.waker.take()
                    };
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                });
            }
            let mut state = state.lock().unwrap();
            if state.ready {
                Poll::Ready(Ok(state.value.take().unwrap()))
            } else {
                state.waker = Some(context.waker().clone());
                Poll::Pending
            }
        }))
    }

    impl HttpTransport for ConcurrentProbeTransport {
        fn send(
            &self,
            _request: HttpRequest,
            _budget: HttpResponseBudget,
        ) -> cdf_kernel::BoxFuture<'_, Result<HttpResponse>> {
            Box::pin(async move {
                let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(current, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(50));
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(HttpResponse::new(200))
            })
        }
    }

    #[test]
    fn shared_rest_transport_does_not_serialize_independent_requests() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let transport = Arc::new(ConcurrentProbeTransport {
            active: Arc::clone(&active),
            peak: Arc::clone(&peak),
        });
        let start = Arc::new(Barrier::new(3));
        let workers = (0..2)
            .map(|index| {
                let transport = Arc::clone(&transport);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    futures_executor::block_on(transport.send(
                        HttpRequest::new(
                            HttpMethod::Get,
                            format!("https://api.example.test/{index}"),
                        ),
                        test_http_budget(),
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
    fn paginator_prefetches_only_from_predecode_evidence() {
        let mut pages = Paginator::new(cdf_http::PaginationConfig::Page {
            query_param: "page".to_owned(),
            start_page: 1,
        });
        let mut nonempty = HttpResponse::new(200);
        assert_eq!(
            predecode_next_rest_url(
                Some(&mut pages),
                Some(PaginationKind::Page),
                "https://api.example.test/items?page=1",
                &mut nonempty,
                true,
            )
            .as_deref(),
            Some("https://api.example.test/items?page=2")
        );

        let mut pages = Paginator::new(cdf_http::PaginationConfig::Page {
            query_param: "page".to_owned(),
            start_page: 1,
        });
        let mut empty = HttpResponse::new(200);
        assert_eq!(
            predecode_next_rest_url(
                Some(&mut pages),
                Some(PaginationKind::Page),
                "https://api.example.test/items?page=1",
                &mut empty,
                false,
            ),
            None
        );

        let mut offsets = Paginator::new(cdf_http::PaginationConfig::Offset {
            offset_param: "offset".to_owned(),
            limit_param: "limit".to_owned(),
            start_offset: 0,
            limit: 100,
        });
        let mut response = HttpResponse::new(200).with_item_count(100);
        assert_eq!(
            predecode_next_rest_url(
                Some(&mut offsets),
                Some(PaginationKind::Offset),
                "https://api.example.test/items?offset=0&limit=100",
                &mut response,
                true,
            ),
            None
        );
        assert_eq!(
            offsets
                .next_request(
                    "https://api.example.test/items?offset=0&limit=100",
                    &response,
                )
                .unwrap()
                .url,
            "https://api.example.test/items?limit=100&offset=100"
        );
    }

    #[test]
    fn page_decode_and_prefetch_are_polled_concurrently() {
        let (started_tx, started_rx) = mpsc::channel();
        let decode_gate = PollGate::new(started_tx.clone());
        let fetch_gate = PollGate::new(started_tx);
        let decode = Arc::clone(&decode_gate).future(Vec::new());
        let fetch = Arc::clone(&fetch_gate).future(HttpResponse::new(200));
        let worker = std::thread::spawn(move || {
            futures_executor::block_on(decode_with_prefetch(
                decode,
                Some(("https://api.example.test/page-2".to_owned(), fetch)),
            ))
        });

        let decode_or_fetch_started = started_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        let both_started = started_rx.recv_timeout(Duration::from_millis(100)).is_ok();
        decode_gate.release();
        fetch_gate.release();
        let (batches, prefetched) = worker.join().unwrap().unwrap();

        assert!(decode_or_fetch_started);
        assert!(both_started, "decode and fetch were polled serially");
        assert!(batches.is_empty());
        assert_eq!(prefetched.unwrap().url, "https://api.example.test/page-2");
    }

    #[test]
    fn host_egress_denial_precedes_rest_policy_and_transport_contact() {
        struct CountingTransport(Arc<AtomicUsize>);

        impl HttpTransport for CountingTransport {
            fn send(
                &self,
                _request: HttpRequest,
                _budget: HttpResponseBudget,
            ) -> cdf_kernel::BoxFuture<'_, Result<HttpResponse>> {
                Box::pin(async move {
                    self.0.fetch_add(1, Ordering::SeqCst);
                    Ok(HttpResponse::new(200))
                })
            }
        }

        let sends = Arc::new(AtomicUsize::new(0));
        let transport = CountingTransport(Arc::clone(&sends));
        let egress = SourceEgressScope::new(
            SourceDriverId::new("rest").unwrap(),
            Arc::new(cdf_http::EgressAllowlist::from_hosts([
                "host-permitted.example.org",
            ])),
        );
        let plan = RestResourcePlan {
            source: "api".to_owned(),
            base_url: "https://adapter-permitted.example.org".to_owned(),
            path: "/items".to_owned(),
            params: BTreeMap::new(),
            record_selector: "$".to_owned(),
            pagination: None,
            auth: None,
            rate_limit: cdf_http::RateLimitPolicy::unrestricted(),
            quota_authority: "https://adapter-permitted.example.org:443".to_owned(),
            respect_headers: Vec::new(),
            allowlist: cdf_http::EgressAllowlist::allow_any(),
            cursor_param: None,
            cursor_filter_fidelity: PushdownFidelity::Unsupported,
            records_transform: None,
        };
        let mut auth_session = None;
        let execution = test_execution_services();
        let context = RestSendContext {
            transport: &transport,
            secret_provider: None,
            auth_refresh: None,
            egress: &egress,
            maximum_response_bytes: 1024,
            memory: Arc::new(
                cdf_memory::DeterministicMemoryCoordinator::new(1024, BTreeMap::new()).unwrap(),
            ),
            cancellation: RunCancellation::default(),
            execution: &execution,
        };

        let error = futures_executor::block_on(send_page_once(
            &context,
            &plan,
            "https://adapter-permitted.example.org/items",
            &mut auth_session,
        ))
        .unwrap_err();

        assert_eq!(error.kind, cdf_kernel::ErrorKind::Auth);
        assert_eq!(sends.load(Ordering::SeqCst), 0);
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

        assert_eq!(
            origin("https://api.example.com/v1?existing=1").unwrap(),
            "https://api.example.com"
        );
        assert!(validate_http_url("https:///v1").is_err());
        assert!(validate_http_url("https://api example.com/v1").is_err());
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
            quota_authority: "https://api.example.com:443".to_owned(),
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
        assert!(!partition.metadata.contains_key("cursor_query_value"));
        assert_eq!(partition.scan_intent.predicates.len(), 1);
        assert!(
            build_request_url(&descriptor, &plan, &partition, None)
                .unwrap()
                .contains("since=5")
        );

        let mut no_parameter = plan.clone();
        no_parameter.cursor_param = None;
        assert_eq!(
            rest_resource_capabilities(&descriptor, &no_parameter)
                .filters
                .default_fidelity,
            PushdownFidelity::Unsupported
        );
        assert_eq!(
            selected_cursor_pushdown(&descriptor, &no_parameter, &request),
            None
        );
        assert!(
            rest_partition(&descriptor, &no_parameter, &request)
                .unwrap()
                .scan_intent
                .predicates
                .is_empty()
        );
    }

    #[test]
    #[ignore = "release performance envelope"]
    fn page_prefetch_hides_one_network_or_decode_window() {
        const WINDOW: Duration = Duration::from_millis(75);
        let serial_started = std::time::Instant::now();
        futures_executor::block_on(async {
            delayed_future(WINDOW, Vec::<Batch>::new()).await.unwrap();
            delayed_future(WINDOW, HttpResponse::new(200))
                .await
                .unwrap();
        });
        let serial = serial_started.elapsed();

        let overlapped_started = std::time::Instant::now();
        futures_executor::block_on(decode_with_prefetch(
            delayed_future(WINDOW, Vec::<Batch>::new()),
            Some((
                "https://api.example.test/page-2".to_owned(),
                delayed_future(WINDOW, HttpResponse::new(200)),
            )),
        ))
        .unwrap();
        let overlapped = overlapped_started.elapsed();
        let speedup = serial.as_secs_f64() / overlapped.as_secs_f64();
        eprintln!(
            "REST page overlap: serial={serial:?}, overlapped={overlapped:?}, speedup={speedup:.2}x"
        );
        assert!(
            overlapped.as_secs_f64() < serial.as_secs_f64() * 0.70,
            "REST page overlap failed to hide one window: serial={serial:?}, overlapped={overlapped:?}"
        );
    }
}
