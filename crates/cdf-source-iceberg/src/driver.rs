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
    CompiledSourcePlan, ExecutionServices, PreparedSourcePayload, PreparedSourcePayloadKey,
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

type RuntimeFactory = dyn Fn(
        Arc<dyn SecretProvider + Send + Sync>,
        ExecutionServices,
        cdf_runtime::SourceEgressScope,
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
        executor_class: SourceExecutorClass::Io,
        blocking_lane: None,
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
