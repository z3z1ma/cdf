use std::{collections::BTreeMap, path::Path, sync::Arc};

use cdf_http::{HttpTransport, SecretProvider};
use cdf_kernel::{
    BackpressureSupport, CapabilitySupport, CdfError, EffectiveSchemaCatalogEntry,
    EffectiveSchemaRuntime, EstimateSupport, FilterCapabilities, IncrementalShape,
    PartitionOpenAttempt, PartitionPlan, PartitioningCapabilities, PayloadRetention,
    QueryableResource, ReplaySupport, ResourceCapabilities, ResourceDescriptor, ResourceStream,
    Result, ScanPlan, ScanRequest, ScopeKind, TypePolicyAllowances,
};
use cdf_object_access::FileTransport;
use cdf_runtime::{
    BlockingLaneBinding, BlockingLaneSpec, CompiledSourcePlan, ExecutionServices,
    InterruptionSafety, LaneAffinity, PreparedSourcePayload, PreparedSourcePayloadKey,
    SourceAttestationStrength, SourceBatchMemoryContract, SourceCompileRequest,
    SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest, SourceDiscoverySession,
    SourceDriver, SourceDriverDescriptor, SourceExecutionCapabilities, SourceExecutorClass,
    SourceHealthRequest, SourceHealthResult, SourceHealthSink, SourceHealthStatus,
    SourceResolutionContext, SourceRetryGranularity, artifact_hash,
};
use cdf_task_store::ExternalTaskStore;
use serde::{Deserialize, Serialize};

use crate::{
    GlueCatalogClient, IcebergCatalogContext, IcebergCatalogLoadRequest, IcebergCatalogRegistry,
    IcebergResourceOptions, IcebergSourceOptions, LoadedIcebergTable, iceberg_option_schema,
    iceberg_source_descriptor,
    planner::{IcebergPlanningContext, plan_snapshot_scan},
};

const PLANNING_ARTIFACT_NAMESPACE: &str = "planner-artifacts";
pub const ICEBERG_SOURCE_BLOCKING_LANE_ID: &str = "iceberg-source.control";

pub fn iceberg_source_blocking_lane(maximum_concurrency: u16) -> BlockingLaneSpec {
    BlockingLaneSpec {
        lane_id: ICEBERG_SOURCE_BLOCKING_LANE_ID.to_owned(),
        binding: BlockingLaneBinding::Static,
        maximum_concurrency,
        cpu_slot_cost: 1,
        native_internal_parallelism: 1,
        affinity: LaneAffinity::Shared,
        interruption: InterruptionSafety::CooperativeOnly,
    }
}

type RuntimeFactory = dyn Fn(
        Arc<dyn SecretProvider + Send + Sync>,
        ExecutionServices,
        cdf_runtime::SourceEgressScope,
        BlockingLaneSpec,
    ) -> Result<IcebergRuntimeDependencies>
    + Send
    + Sync
    + 'static;

#[derive(Clone)]
pub struct IcebergRuntimeDependencies {
    object_access: Arc<dyn FileTransport>,
    rest_http: Arc<dyn HttpTransport>,
    glue: Arc<dyn GlueCatalogClient>,
}

impl std::fmt::Debug for IcebergRuntimeDependencies {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergRuntimeDependencies")
            .finish_non_exhaustive()
    }
}

impl IcebergRuntimeDependencies {
    pub fn new(
        object_access: Arc<dyn FileTransport>,
        rest_http: Arc<dyn HttpTransport>,
        glue: Arc<dyn GlueCatalogClient>,
    ) -> Self {
        Self {
            object_access,
            rest_http,
            glue,
        }
    }
}

#[derive(Clone)]
pub struct IcebergSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
    catalogs: Arc<IcebergCatalogRegistry>,
    runtime_factory: Arc<RuntimeFactory>,
}

impl std::fmt::Debug for IcebergSourceDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergSourceDriver")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

impl IcebergSourceDriver {
    pub fn new<F>(runtime_factory: F) -> Result<Self>
    where
        F: Fn(
                Arc<dyn SecretProvider + Send + Sync>,
                ExecutionServices,
                cdf_runtime::SourceEgressScope,
                BlockingLaneSpec,
            ) -> Result<IcebergRuntimeDependencies>
            + Send
            + Sync
            + 'static,
    {
        Ok(Self {
            descriptor: iceberg_source_descriptor()?,
            option_schema: iceberg_option_schema(),
            catalogs: Arc::new(IcebergCatalogRegistry::standard()?),
            runtime_factory: Arc::new(runtime_factory),
        })
    }

    fn physical_plan(&self, plan: &CompiledSourcePlan) -> Result<IcebergPhysicalPlan> {
        let physical: IcebergPhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid Iceberg source plan: {error}")))?;
        physical.validate()?;
        Ok(physical)
    }

    fn catalog_context(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<IcebergCatalogContext> {
        let dependencies = (self.runtime_factory)(
            Arc::clone(context.secret_provider()),
            context.execution().clone(),
            context.egress_scope(&plan.driver.driver_id),
            plan.execution_capabilities
                .blocking_lane
                .clone()
                .ok_or_else(|| {
                    CdfError::contract(
                        "Iceberg compiled execution requires its source-owned blocking lane",
                    )
                })?,
        )?;
        Ok(IcebergCatalogContext {
            object_access: dependencies.object_access,
            rest_http: dependencies.rest_http,
            glue: dependencies.glue,
            secrets: Arc::clone(context.secret_provider()),
            execution: context.execution().clone(),
            egress: context.egress_scope(&plan.driver.driver_id),
            project_root: context.project_root().to_path_buf(),
        })
    }

    fn load_table(
        &self,
        plan: &CompiledSourcePlan,
        physical: &IcebergPhysicalPlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<(LoadedIcebergTable, IcebergCatalogContext)> {
        let catalog = self.catalog_context(plan, context)?;
        let table = self.catalogs.load_table(
            &IcebergCatalogLoadRequest {
                source: physical.source.clone(),
                resource: physical.resource.clone(),
                cancellation: context.cancellation(),
            },
            &catalog,
        )?;
        Ok((table, catalog))
    }
}

impl SourceDriver for IcebergSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn validate_portable_plan(&self, plan: &CompiledSourcePlan) -> Result<()> {
        plan.validate()?;
        let physical = self.physical_plan(plan)?;
        if let crate::IcebergCatalogOptions::Filesystem { warehouse } = &physical.source.catalog
            && (Path::new(warehouse).is_absolute() || warehouse.starts_with("file://"))
        {
            return Err(CdfError::contract(
                "portable Iceberg source plans cannot bind coordinator-local filesystem warehouses; use an object-store/REST/Glue catalog or a typed staged artifact",
            ));
        }
        Ok(())
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        let source: IcebergSourceOptions =
            decode_options("Iceberg source", request.source_options)?;
        let resource: IcebergResourceOptions =
            decode_options("Iceberg resource", request.resource_options)?;
        source.validate()?;
        resource.validate()?;
        let physical = IcebergPhysicalPlan { source, resource };
        physical.validate()?;
        let encoded = serde_json::to_value(&physical)
            .map_err(|error| CdfError::internal(format!("serialize Iceberg plan: {error}")))?;
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            iceberg_resource_capabilities(),
            execution_capabilities(&physical.source),
            cdf_runtime::CompiledSourcePlanInput {
                descriptor: request.descriptor,
                schema: request.schema,
                type_policy_allowances: request.type_policy_allowances,
                effective_schema_runtime: request.effective_schema_runtime,
                baseline_observation_schema_catalog: request.baseline_observation_schema_catalog,
                redacted_options: encoded.clone(),
                physical_plan: encoded,
            },
        )
    }

    fn discovery_session(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Box<dyn SourceDiscoverySession>> {
        let physical = self.physical_plan(plan)?;
        let (table, _) = self.load_table(plan, &physical, context)?;
        let candidate = discovery_candidate(&table)?;
        let retention = PayloadRetention::new(Arc::new(table.clone()), table.retained_bytes())?;
        context.prepared_payloads().install(
            prepared_table_key(plan)?,
            PreparedSourcePayload::new(table.clone(), retention),
        )?;
        Ok(Box::new(IcebergDiscoverySession { table, candidate }))
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        let physical = self.physical_plan(plan)?;
        let catalog = self.catalog_context(plan, context)?;
        let (table, prepared_retention) = match context
            .prepared_payloads()
            .take(&prepared_table_key(plan)?)?
        {
            Some(payload) => {
                let (table, retention) =
                    payload.into_typed::<LoadedIcebergTable>("Iceberg table metadata")?;
                (table, Some(retention))
            }
            None => (
                self.catalogs.load_table(
                    &IcebergCatalogLoadRequest {
                        source: physical.source.clone(),
                        resource: physical.resource.clone(),
                        cancellation: context.cancellation(),
                    },
                    &catalog,
                )?,
                None,
            ),
        };
        let task_store = ExternalTaskStore::new(
            context.project_root().join(".cdf"),
            cdf_kernel::ContentStoreNamespace::new(PLANNING_ARTIFACT_NAMESPACE)?,
        )?;
        Ok(Arc::new(IcebergResource {
            descriptor: plan.descriptor.clone(),
            schema: Arc::new(plan.schema.clone()),
            capabilities: plan.resource_capabilities.clone(),
            type_policy_allowances: plan.type_policy_allowances,
            effective_schema_runtime: plan.effective_schema_runtime.clone(),
            baseline_observation_schema_catalog: plan.baseline_observation_schema_catalog.clone(),
            compiled_source_plan_hash: artifact_hash(plan)?,
            source: physical.source,
            table,
            catalog,
            task_store,
            cancellation: context.cancellation(),
            _prepared_retention: prepared_retention,
        }))
    }

    fn health(
        &self,
        request: SourceHealthRequest,
        context: &SourceResolutionContext<'_>,
        output: &mut dyn SourceHealthSink,
    ) -> Result<()> {
        if request.compiled_plans.is_empty() {
            return output.emit(SourceHealthResult {
                probe_id: "iceberg-inventory".to_owned(),
                status: SourceHealthStatus::Skipped,
                message: "no Iceberg resources are compiled".to_owned(),
                details: serde_json::json!({"resources": 0}),
            });
        }
        for plan in &request.compiled_plans {
            request.budget.consume_work(1)?;
            let physical = self.physical_plan(plan)?;
            let result = match self.load_table(plan, &physical, context) {
                Ok((table, _)) => {
                    request.budget.consume_payload_bytes(table.bytes_read)?;
                    SourceHealthResult {
                        probe_id: plan.descriptor.resource_id.as_str().to_owned(),
                        status: SourceHealthStatus::Passed,
                        message:
                            "Iceberg catalog, table, metadata, and selected snapshot are readable"
                                .to_owned(),
                        details: serde_json::json!({
                            "resource_id": plan.descriptor.resource_id.as_str(),
                            "table_uuid": table.metadata.uuid().to_string(),
                            "snapshot_id": table.selected.as_ref().map(|value| value.position.snapshot_id),
                            "schema_id": table.selected.as_ref().map_or_else(
                                || table.metadata.current_schema_id(),
                                |value| value.schema_id,
                            ),
                            "metadata_objects": table.objects_read,
                            "metadata_bytes": table.bytes_read,
                        }),
                    }
                }
                Err(error) => SourceHealthResult::failed(
                    plan.descriptor.resource_id.as_str(),
                    "Iceberg catalog/table metadata probe failed",
                    &plan.descriptor.resource_id,
                    &error,
                ),
            };
            output.emit(result)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IcebergPhysicalPlan {
    source: IcebergSourceOptions,
    resource: IcebergResourceOptions,
}

impl IcebergPhysicalPlan {
    fn validate(&self) -> Result<()> {
        self.source.validate()?;
        self.resource.validate()
    }
}

struct IcebergDiscoverySession {
    table: LoadedIcebergTable,
    candidate: SourceDiscoveryCandidate,
}

impl SourceDiscoverySession for IcebergDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        SourceDiscoveryKind::SchemaMetadata
    }

    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        Ok(vec![self.candidate.clone()])
    }

    fn observe(
        &self,
        candidate: &SourceDiscoveryCandidate,
        request: &SourceDiscoveryRequest,
    ) -> Result<cdf_runtime::SourceSchemaObservation> {
        request.validate()?;
        request.cancellation.check()?;
        if candidate.discovery_binding()? != self.candidate.discovery_binding()? {
            return Err(CdfError::contract(
                "Iceberg discovery candidate does not match the loaded table observation",
            ));
        }
        if self.table.bytes_read > request.maximum_bytes {
            return Err(CdfError::data(format!(
                "Iceberg metadata discovery read {} bytes but the command budget permits {}; increase the discovery byte budget or lower maximum_metadata_bytes",
                self.table.bytes_read, request.maximum_bytes
            )));
        }
        cdf_runtime::SourceSchemaObservation::new(
            candidate,
            self.table.arrow_schema.as_ref().clone(),
            discovery_identity(&self.table),
            self.table.bytes_read,
            0,
        )
    }
}

struct IcebergResource {
    descriptor: ResourceDescriptor,
    schema: arrow_schema::SchemaRef,
    capabilities: ResourceCapabilities,
    type_policy_allowances: TypePolicyAllowances,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
    compiled_source_plan_hash: String,
    source: IcebergSourceOptions,
    table: LoadedIcebergTable,
    catalog: IcebergCatalogContext,
    task_store: ExternalTaskStore,
    cancellation: cdf_runtime::RunCancellation,
    _prepared_retention: Option<PayloadRetention>,
}

impl ResourceStream for IcebergResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn compiled_source_plan_hash(&self) -> Option<&str> {
        Some(&self.compiled_source_plan_hash)
    }

    fn validate_runtime_dependencies(&self) -> Result<()> {
        self.source.validate()?;
        self.table.table_identity().validate()
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        Err(CdfError::contract(
            "Iceberg uses external canonical task authority and must be planned through QueryableResource::negotiate",
        ))
    }

    fn open(&self, _partition: PartitionPlan) -> PartitionOpenAttempt<'_> {
        PartitionOpenAttempt::materialized(Box::pin(async {
            Err(CdfError::contract(
                "Iceberg data task execution is owned by I2 and is not yet installed",
            ))
        }))
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_ref()
    }

    fn baseline_observation_schema_catalog(&self) -> &[EffectiveSchemaCatalogEntry] {
        &self.baseline_observation_schema_catalog
    }

    fn type_policy_allowances(&self) -> TypePolicyAllowances {
        self.type_policy_allowances
    }
}

impl QueryableResource for IcebergResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        plan_snapshot_scan(
            &self.descriptor,
            &self.source,
            &self.table,
            request,
            IcebergPlanningContext {
                catalog: &self.catalog,
                task_store: &self.task_store,
                cancellation: self.cancellation.clone(),
            },
        )
    }
}

fn prepared_table_key(plan: &CompiledSourcePlan) -> Result<PreparedSourcePayloadKey> {
    PreparedSourcePayloadKey::new(
        plan.descriptor.resource_id.clone(),
        plan.driver.driver_id.clone(),
        artifact_hash(&serde_json::json!({
            "kind": "iceberg_table_metadata_v1",
            "source_discovery_binding": plan.discovery_binding_hash()?,
        }))?,
    )
}

fn discovery_candidate(table: &LoadedIcebergTable) -> Result<SourceDiscoveryCandidate> {
    SourceDiscoveryCandidate::new(
        table.metadata_location.clone(),
        Some(table.bytes_read),
        None,
        discovery_identity(table),
    )
}

fn discovery_identity(table: &LoadedIcebergTable) -> BTreeMap<String, String> {
    let mut identity = BTreeMap::from([
        (
            "metadata_generation".to_owned(),
            table.metadata_generation.clone(),
        ),
        ("table_uuid".to_owned(), table.metadata.uuid().to_string()),
    ]);
    if let Some(catalog_generation) = &table.catalog_generation {
        identity.insert("catalog_generation".to_owned(), catalog_generation.clone());
    }
    if let Some(selected) = &table.selected {
        identity.insert(
            "snapshot_id".to_owned(),
            selected.position.snapshot_id.to_string(),
        );
        identity.insert(
            "snapshot_sequence".to_owned(),
            selected.position.sequence_number.to_string(),
        );
    } else {
        identity.insert("empty_table".to_owned(), "true".to_owned());
    }
    identity
}

fn decode_options<T: for<'de> Deserialize<'de>>(
    label: &str,
    options: BTreeMap<String, serde_json::Value>,
) -> Result<T> {
    serde_json::from_value(serde_json::Value::Object(options.into_iter().collect()))
        .map_err(|error| CdfError::contract(format!("{label} options are invalid: {error}")))
}

fn iceberg_resource_capabilities() -> ResourceCapabilities {
    ResourceCapabilities {
        projection: CapabilitySupport::Supported,
        filters: FilterCapabilities::default(),
        limits: CapabilitySupport::Unsupported,
        ordering: CapabilitySupport::Unsupported,
        partitioning: PartitioningCapabilities {
            parallel_partitions: true,
            supported_scopes: vec![ScopeKind::Partition],
        },
        incremental: IncrementalShape::Full,
        replay: ReplaySupport::ExactRecordedBatches,
        idempotent_reads: true,
        backpressure: BackpressureSupport::Pausable,
        estimates: EstimateSupport::RowsAndBytes,
    }
}

fn execution_capabilities(source: &IcebergSourceOptions) -> SourceExecutionCapabilities {
    SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: 32 * 1024 * 1024,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: 32 * 1024 * 1024,
        maximum_concurrency: source.maximum_concurrency,
        useful_concurrency: source.maximum_concurrency,
        executor_class: SourceExecutorClass::BlockingLane,
        blocking_lane: Some(iceberg_source_blocking_lane(source.maximum_concurrency)),
        pausable: true,
        spillable: true,
        idempotent_reads: true,
        reopenable: true,
        resumable: true,
        speculative_safe: true,
        retry_granularity: SourceRetryGranularity::Unit,
        retryable_errors: vec![
            cdf_kernel::ErrorKind::Transient,
            cdf_kernel::ErrorKind::RateLimited,
        ],
        retry_policy: Some(cdf_runtime::SourceRetryPolicy::default()),
        attestation: SourceAttestationStrength::Snapshot,
        rate_limit: None,
        quota_authority: None,
        canonical_order: true,
        bounded: true,
        batch_memory: SourceBatchMemoryContract::Preaccounted,
        telemetry_version: "v1".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        fs,
        path::Path,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use arrow_schema::Schema;
    use cdf_http::{
        HttpRequest, HttpResponse, HttpResponseBudget, HttpTransport, SecretProvider, SecretUri,
        SecretValue,
    };
    use cdf_kernel::{
        BoxFuture, ResourceDescriptor, ResourceId, SchemaSource, ScopeKey, TrustLevel,
        WriteDisposition,
    };
    use cdf_object_access::FileTransportFacade;
    use cdf_runtime::{
        BlockingTask, CpuFutureTask, CpuTaskSpec, ExecutionHost, ExecutionHostCapabilities,
        ExecutionServices, ExecutionTaskScope, FixedSpillBudget, IoTask, IoValue, IoValueTask,
        RunCancellation, SourceCompileContext, SourceHealthStatus, SourceResolutionContext,
        SpillBudgetCoordinator, TaskScopeReport,
    };

    use super::*;
    use crate::UnsupportedGlueCatalogClient;

    struct NoopSecretProvider;

    impl SecretProvider for NoopSecretProvider {
        fn resolve(&self, _uri: &SecretUri) -> Result<SecretValue> {
            Err(CdfError::auth(
                "Iceberg local test does not resolve secrets",
            ))
        }
    }

    #[derive(Clone)]
    struct NoopHttpTransport;

    impl HttpTransport for NoopHttpTransport {
        fn send(
            &self,
            _request: HttpRequest,
            _budget: HttpResponseBudget,
        ) -> BoxFuture<'_, Result<HttpResponse>> {
            Box::pin(async { Err(CdfError::internal("unexpected Iceberg local HTTP request")) })
        }
    }

    #[derive(Clone)]
    struct SequenceHttpTransport {
        responses: Arc<Mutex<VecDeque<Vec<u8>>>>,
        requests: Arc<Mutex<Vec<String>>>,
    }

    impl SequenceHttpTransport {
        fn new(responses: impl IntoIterator<Item = Vec<u8>>) -> Self {
            Self {
                responses: Arc::new(Mutex::new(responses.into_iter().collect())),
                requests: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl HttpTransport for SequenceHttpTransport {
        fn send(
            &self,
            request: HttpRequest,
            budget: HttpResponseBudget,
        ) -> BoxFuture<'_, Result<HttpResponse>> {
            self.requests.lock().unwrap().push(request.url);
            let body = self.responses.lock().unwrap().pop_front();
            Box::pin(async move {
                let body = body.ok_or_else(|| {
                    CdfError::internal("Iceberg REST test received an unexpected request")
                })?;
                Ok(HttpResponse::new(200).with_body(budget.account_body(body).await?))
            })
        }
    }

    struct TestIoHost {
        runtime: tokio::runtime::Runtime,
        memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
        lanes: Mutex<BTreeMap<String, BlockingLaneSpec>>,
    }

    impl ExecutionHost for TestIoHost {
        fn capabilities(&self) -> ExecutionHostCapabilities {
            ExecutionHostCapabilities {
                logical_cpu_slots: 2,
                io_workers: 2,
                blocking_lanes: self.lanes.lock().unwrap().values().cloned().collect(),
            }
        }

        fn memory(&self) -> Arc<dyn cdf_memory::MemoryCoordinator> {
            Arc::clone(&self.memory)
        }

        fn spill(&self) -> Arc<dyn SpillBudgetCoordinator> {
            Arc::clone(&self.spill)
        }

        fn open_scope(&self, _run_id: &str) -> Result<Box<dyn ExecutionTaskScope>> {
            Ok(Box::new(TestIoScope {
                handle: self.runtime.handle().clone(),
                cancellation: RunCancellation::default(),
                tasks: Vec::new(),
                submitted_io: 0,
                submitted_cpu: 0,
                submitted_blocking: 0,
            }))
        }

        fn run_io_blocking(&self, task: IoValueTask) -> Result<IoValue> {
            self.runtime.block_on(task)
        }

        fn delay(
            &self,
            duration: Duration,
            cancellation: RunCancellation,
        ) -> BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                cancellation.check()?;
                tokio::time::sleep(duration).await;
                cancellation.check()
            })
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

        fn ensure_blocking_lanes(&self, lanes: &[BlockingLaneSpec]) -> Result<()> {
            let mut registered = self.lanes.lock().unwrap();
            for lane in lanes {
                lane.validate()?;
                if let Some(existing) = registered.get(&lane.lane_id) {
                    if existing != lane {
                        return Err(CdfError::contract(format!(
                            "test host blocking lane `{}` conflicts with its registered authority",
                            lane.lane_id
                        )));
                    }
                } else {
                    registered.insert(lane.lane_id.clone(), lane.clone());
                }
            }
            Ok(())
        }

        fn run_blocking_value(
            &self,
            _lane: &str,
            task: cdf_runtime::BlockingValueTask,
        ) -> Result<IoValue> {
            task()
        }
    }

    struct TestIoScope {
        handle: tokio::runtime::Handle,
        cancellation: RunCancellation,
        tasks: Vec<tokio::task::JoinHandle<Result<()>>>,
        submitted_io: u64,
        submitted_cpu: u64,
        submitted_blocking: u64,
    }

    impl Drop for TestIoScope {
        fn drop(&mut self) {
            self.cancellation.cancel();
            for task in &self.tasks {
                task.abort();
            }
        }
    }

    impl ExecutionTaskScope for TestIoScope {
        fn cancellation(&self) -> RunCancellation {
            self.cancellation.clone()
        }

        fn spawn_io(&mut self, task: IoTask) -> Result<()> {
            self.tasks.push(self.handle.spawn(task));
            self.submitted_io += 1;
            Ok(())
        }

        fn spawn_cpu(&mut self, _spec: CpuTaskSpec, task: BlockingTask) -> Result<()> {
            self.tasks.push(self.handle.spawn_blocking(task));
            self.submitted_cpu += 1;
            Ok(())
        }

        fn spawn_cpu_future(&mut self, spec: CpuTaskSpec, task: CpuFutureTask) -> Result<()> {
            spec.validate()?;
            self.tasks.push(self.handle.spawn(task));
            self.submitted_cpu += 1;
            Ok(())
        }

        fn spawn_blocking(&mut self, _lane: &str, task: BlockingTask) -> Result<()> {
            self.tasks.push(self.handle.spawn_blocking(task));
            self.submitted_blocking += 1;
            Ok(())
        }

        fn cancel(&self) {
            self.cancellation.cancel();
        }

        fn join(mut self: Box<Self>) -> BoxFuture<'static, Result<TaskScopeReport>> {
            Box::pin(async move {
                let mut report = TaskScopeReport {
                    submitted_io: self.submitted_io,
                    submitted_cpu: self.submitted_cpu,
                    submitted_blocking: self.submitted_blocking,
                    ..TaskScopeReport::default()
                };
                for task in self.tasks.drain(..) {
                    match task.await {
                        Ok(Ok(())) => report.completed += 1,
                        Ok(Err(error)) => return Err(error),
                        Err(error) => {
                            return Err(CdfError::internal(format!(
                                "Iceberg metadata test task failed: {error}"
                            )));
                        }
                    }
                }
                Ok(report)
            })
        }
    }

    fn execution_services() -> ExecutionServices {
        let memory: Arc<dyn cdf_memory::MemoryCoordinator> = Arc::new(
            cdf_memory::DeterministicMemoryCoordinator::new(256 * 1024 * 1024, BTreeMap::new())
                .unwrap(),
        );
        let spill: Arc<dyn SpillBudgetCoordinator> =
            Arc::new(FixedSpillBudget::new(256 * 1024 * 1024).unwrap());
        ExecutionServices::new(Arc::new(TestIoHost {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap(),
            memory,
            spill,
            lanes: Mutex::new(BTreeMap::new()),
        }))
        .unwrap()
    }

    fn empty_table_metadata(location: &Path) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": location.display().to_string(),
            "last-sequence-number": 0,
            "last-updated-ms": 1602638573590_i64,
            "last-column-id": 2,
            "current-schema-id": 0,
            "schemas": [{
                "type": "struct",
                "schema-id": 0,
                "fields": [
                    {"id": 1, "name": "id", "required": true, "type": "long", "doc": "stable id"},
                    {"id": 2, "name": "label", "required": false, "type": "string"}
                ]
            }],
            "default-spec-id": 0,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "last-partition-id": 999,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "refs": {}
        }))
        .unwrap()
    }

    fn compile_request(warehouse: &Path) -> SourceCompileRequest {
        SourceCompileRequest {
            source_kind: "iceberg".to_owned(),
            context: SourceCompileContext {
                source_name: "lake".to_owned(),
                project_root: Some(warehouse.to_path_buf()),
                cursor_pushdown: None,
            },
            source_options: BTreeMap::from([
                (
                    "catalog".to_owned(),
                    serde_json::json!({"kind": "filesystem", "warehouse": warehouse}),
                ),
                ("egress_allowlist".to_owned(), serde_json::json!([])),
            ]),
            resource_options: BTreeMap::from([
                ("namespace".to_owned(), serde_json::json!(["analytics"])),
                ("table".to_owned(), serde_json::json!("events")),
            ]),
            descriptor: ResourceDescriptor {
                resource_id: ResourceId::new("lake.events").unwrap(),
                schema_source: SchemaSource::Discover,
                primary_key: Vec::new(),
                merge_key: Vec::new(),
                cursor: None,
                write_disposition: WriteDisposition::Append,
                deduplication: None,
                contract: None,
                state_scope: ScopeKey::Resource,
                freshness: None,
                trust_level: TrustLevel::Governed,
            },
            schema: Schema::empty(),
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        }
    }

    #[test]
    fn filesystem_discovery_reuses_exact_empty_table_metadata_and_plans_no_tasks() {
        let root = tempfile::tempdir().unwrap();
        let table = root.path().join("analytics/events");
        let metadata = table.join("metadata");
        fs::create_dir_all(&metadata).unwrap();
        fs::write(
            metadata.join("v1.metadata.json"),
            empty_table_metadata(&table),
        )
        .unwrap();
        fs::write(metadata.join("version-hint.text"), "1\n").unwrap();
        let execution = execution_services();
        let http = NoopHttpTransport;
        let driver = IcebergSourceDriver::new(move |secrets, execution, _egress, lane| {
            Ok(IcebergRuntimeDependencies::new(
                Arc::new(
                    FileTransportFacade::new()
                        .with_shared_secret_provider(secrets)
                        .with_execution_services(execution)
                        .with_local_listing_lane(lane)?,
                ),
                Arc::new(http.clone()),
                Arc::new(UnsupportedGlueCatalogClient),
            ))
        })
        .unwrap();
        let plan = driver.compile(compile_request(root.path())).unwrap();
        let context = SourceResolutionContext::new(
            root.path(),
            Arc::new(NoopSecretProvider),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let session = driver.discovery_session(&plan, &context).unwrap();
        let candidate = session.candidates().unwrap().remove(0);
        let observation = session
            .observe(
                &candidate,
                &SourceDiscoveryRequest::new(64 * 1024 * 1024, 1).unwrap(),
            )
            .unwrap();
        assert_eq!(observation.schema.fields().len(), 2);
        assert_eq!(
            observation.schema.field(0).metadata()["PARQUET:field_id"],
            "1"
        );
        assert_eq!(context.prepared_payloads().pending_count().unwrap(), 1);

        let resource = driver.resolve(&plan, &context).unwrap();
        assert_eq!(context.prepared_payloads().pending_count().unwrap(), 0);
        let scan = resource
            .negotiate(&ScanRequest {
                resource_id: plan.descriptor.resource_id.clone(),
                projection: None,
                filters: Vec::new(),
                limit: None,
                order_by: Vec::new(),
                scope: ScopeKey::Resource,
            })
            .unwrap();
        assert!(scan.partitions.is_empty());
        assert_eq!(scan.estimated_rows, Some(0));
        assert_eq!(scan.estimated_bytes, Some(0));
        assert_eq!(scan.planned_task_set.as_ref().unwrap().task_count, 0);

        let mut sink = VecHealthSink::default();
        driver
            .health(
                cdf_runtime::SourceHealthRequest {
                    compiled_plans: vec![plan],
                    configured_resource_ids: Vec::new(),
                    budget: cdf_runtime::SourceHealthBudget::new(
                        cdf_runtime::SourceHealthLimits::default(),
                        execution.clone(),
                        RunCancellation::default(),
                    )
                    .unwrap(),
                },
                &context,
                &mut sink,
            )
            .unwrap();
        assert_eq!(sink.0[0].status, SourceHealthStatus::Passed);
    }

    #[test]
    fn rest_discovery_reuses_negotiated_table_response_without_a_second_catalog_read() {
        let root = tempfile::tempdir().unwrap();
        let table_location = root.path().join("warehouse/analytics/events");
        let metadata: serde_json::Value =
            serde_json::from_slice(&empty_table_metadata(&table_location)).unwrap();
        let transport = SequenceHttpTransport::new([
            serde_json::to_vec(&serde_json::json!({
                "defaults": {"prefix": "prod"},
                "overrides": {}
            }))
            .unwrap(),
            serde_json::to_vec(&serde_json::json!({
                "metadata-location": "s3://lake/analytics/events/metadata/v1.metadata.json",
                "metadata": metadata
            }))
            .unwrap(),
        ]);
        let observed_requests = Arc::clone(&transport.requests);
        let transport_for_runtime = transport.clone();
        let driver = IcebergSourceDriver::new(move |secrets, execution, _egress, lane| {
            Ok(IcebergRuntimeDependencies::new(
                Arc::new(
                    FileTransportFacade::new()
                        .with_shared_secret_provider(secrets)
                        .with_execution_services(execution)
                        .with_local_listing_lane(lane)?,
                ),
                Arc::new(transport_for_runtime.clone()),
                Arc::new(UnsupportedGlueCatalogClient),
            ))
        })
        .unwrap();
        let mut request = compile_request(root.path());
        request.source_options.insert(
            "catalog".to_owned(),
            serde_json::json!({
                "kind": "rest",
                "uri": "https://catalog.example.test/api",
                "warehouse": "primary"
            }),
        );
        request.source_options.insert(
            "egress_allowlist".to_owned(),
            serde_json::json!(["catalog.example.test"]),
        );
        let plan = driver.compile(request).unwrap();
        let execution = execution_services();
        let context = SourceResolutionContext::new(
            root.path(),
            Arc::new(NoopSecretProvider),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let session = driver.discovery_session(&plan, &context).unwrap();
        let candidate = session.candidates().unwrap().remove(0);
        let observation = session
            .observe(
                &candidate,
                &SourceDiscoveryRequest::new(64 * 1024 * 1024, 1).unwrap(),
            )
            .unwrap();
        assert_eq!(observation.schema.fields().len(), 2);
        assert_eq!(observed_requests.lock().unwrap().len(), 2);

        let resource = driver.resolve(&plan, &context).unwrap();
        let scan = resource
            .negotiate(&ScanRequest {
                resource_id: plan.descriptor.resource_id.clone(),
                projection: None,
                filters: Vec::new(),
                limit: None,
                order_by: Vec::new(),
                scope: ScopeKey::Resource,
            })
            .unwrap();
        assert_eq!(scan.planned_task_set.as_ref().unwrap().task_count, 0);
        assert_eq!(observed_requests.lock().unwrap().len(), 2);
        assert!(observed_requests.lock().unwrap()[0].contains("warehouse=primary"));
        assert!(
            observed_requests.lock().unwrap()[1]
                .contains("/v1/prod/namespaces/analytics/tables/events")
        );
    }

    #[derive(Default)]
    struct VecHealthSink(Vec<cdf_runtime::SourceHealthResult>);

    impl SourceHealthSink for VecHealthSink {
        fn emit(&mut self, result: cdf_runtime::SourceHealthResult) -> Result<()> {
            self.0.push(result);
            Ok(())
        }
    }
}
