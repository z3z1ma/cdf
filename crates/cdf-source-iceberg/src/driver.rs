use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};

use cdf_http::{HttpTransport, SecretProvider};
use cdf_kernel::{
    BackpressureSupport, BatchStream, CapabilitySupport, CdfError, EffectiveSchemaCatalogEntry,
    EffectiveSchemaRuntime, EstimateSupport, ExecutablePartition, FilterCapabilities,
    IncrementalShape, PartitionAttestation, PartitionAttestationAttempt, PartitionAuthority,
    PartitionOpenAttempt, PartitionPlan, PartitionStreamPayload, PartitioningCapabilities,
    PayloadRetention, PlannedPartitionReader, PlannedTaskSetReference, QueryableResource,
    ReplaySupport, ResourceCapabilities, ResourceDescriptor, ResourceStream, Result, ScanPlan,
    ScanRequest, ScopeKind, SourcePosition, TypePolicyAllowances,
};
use cdf_object_access::FileTransport;
use cdf_runtime::{
    BlockingLaneBinding, BlockingLaneSpec, CompiledSourcePlan, CpuTaskSpec, ExecutionServices,
    InterruptionSafety, LaneAffinity, PreparedSourcePayload, PreparedSourcePayloadKey,
    SourceAddPlanner, SourceAddProposal, SourceAddRequest, SourceAttestationStrength,
    SourceBatchMemoryContract, SourceCompileRequest, SourceDiscoveryCandidate, SourceDiscoveryKind,
    SourceDiscoveryRequest, SourceDiscoverySession, SourceDriver, SourceDriverDescriptor,
    SourceEvidenceLocation, SourceExecutionCapabilities, SourceExecutorClass, SourceHealthRequest,
    SourceHealthResult, SourceHealthSink, SourceHealthStatus, SourceResolutionContext,
    SourceRetryGranularity, artifact_hash,
};
use cdf_task_store::{ExternalTaskStore, TaskSetLimits};
use serde::{Deserialize, Serialize};

use crate::{
    GlueCatalogClient, ICEBERG_TASK_SET_TYPE, IcebergCatalogContext, IcebergCatalogLoadRequest,
    IcebergCatalogRegistry, IcebergResourceOptions, IcebergScanMode, IcebergScanTask,
    IcebergSourceOptions, IcebergTaskSetAuthority, LoadedIcebergTable,
    execution::{
        IcebergTaskExecution, execute_task_scan, prepare_task_scan, project_output_schema,
    },
    iceberg_option_schema, iceberg_source_descriptor,
    planner::{IcebergPlanningContext, plan_snapshot_scan},
    task_reader::{IcebergExecutableTask, IcebergPlannedPartitionReader},
};

const PLANNING_ARTIFACT_NAMESPACE: &str = "planner-artifacts";
pub const ICEBERG_SOURCE_BLOCKING_LANE_ID: &str = "iceberg-source.control";

pub fn iceberg_source_blocking_lane() -> BlockingLaneSpec {
    BlockingLaneSpec {
        lane_id: ICEBERG_SOURCE_BLOCKING_LANE_ID.to_owned(),
        binding: BlockingLaneBinding::RuntimeResolvedRequired,
        maximum_concurrency: u16::MAX,
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
        let resolved_lane = self.resolved_blocking_lane(plan, context)?;
        let dependencies = (self.runtime_factory)(
            Arc::clone(context.secret_provider()),
            context.execution().clone(),
            context.egress_scope(&plan.driver.driver_id),
            resolved_lane.clone(),
        )?;
        Ok(IcebergCatalogContext {
            object_access: dependencies.object_access,
            rest_http: dependencies.rest_http,
            glue: dependencies.glue,
            secrets: Arc::clone(context.secret_provider()),
            execution: context.execution().clone(),
            blocking_lane: resolved_lane,
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

    fn resolved_blocking_lane(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<BlockingLaneSpec> {
        let compiled = plan
            .execution_capabilities
            .blocking_lane
            .as_ref()
            .ok_or_else(|| {
                CdfError::contract("Iceberg compiled execution omitted its blocking lane")
            })?;
        let mut resolved = compiled.clone();
        resolved.binding = BlockingLaneBinding::RuntimeResolved;
        resolved.maximum_concurrency = resolved
            .maximum_concurrency
            .min(context.execution().capabilities().logical_cpu_slots.max(1));
        resolved.validate_tightening_of(compiled)?;
        Ok(resolved)
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

    fn add_planner(&self) -> Option<&dyn SourceAddPlanner> {
        Some(self)
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
            execution_capabilities(&physical.source)?,
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

    fn resolve_blocking_lane(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Option<BlockingLaneSpec>> {
        self.resolved_blocking_lane(plan, context).map(Some)
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
            context.artifact_root().join(".cdf"),
            cdf_kernel::ContentStoreNamespace::new(PLANNING_ARTIFACT_NAMESPACE)?,
        )?;
        Ok(Arc::new(IcebergResource {
            descriptor: plan.descriptor.clone(),
            schema: Arc::new(plan.schema.clone()),
            compiled_source_plan_hash: plan.compiled_source_plan_hash()?,
            capabilities: plan.resource_capabilities.clone(),
            type_policy_allowances: plan.type_policy_allowances,
            effective_schema_runtime: plan.effective_schema_runtime.clone(),
            baseline_observation_schema_catalog: plan.baseline_observation_schema_catalog.clone(),
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

impl SourceAddPlanner for IcebergSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        request.validate()?;
        let Some(kind) = request.options.get("catalog") else {
            return Ok(None);
        };
        let namespace = request.options.get("namespace").ok_or_else(|| {
            CdfError::contract(
                "Iceberg cdf add requires `--option namespace=<name>`; use a JSON string array for a multipart namespace",
            )
        })?;
        let namespace = parse_add_string_list("Iceberg namespace", namespace)?;
        if namespace.is_empty() {
            return Err(CdfError::contract(
                "Iceberg cdf add namespace must contain at least one component",
            ));
        }
        let table = request
            .options
            .get("table")
            .cloned()
            .unwrap_or_else(|| request.resource_name.clone());
        let selector = request.options.get("selector").map_or_else(
            || Ok(serde_json::json!({"kind": "current"})),
            |value| parse_add_selector(value),
        )?;
        let mode = request.options.get("mode").map_or_else(
            || Ok(IcebergScanMode::Snapshot),
            |value| match value.as_str() {
                "snapshot" => Ok(IcebergScanMode::Snapshot),
                "append_snapshots" => Ok(IcebergScanMode::AppendSnapshots),
                other => Err(CdfError::contract(format!(
                    "Iceberg cdf add mode must be `snapshot` or `append_snapshots`, not `{other}`"
                ))),
            },
        )?;
        let mut catalog = serde_json::Map::new();
        catalog.insert("kind".to_owned(), serde_json::Value::String(kind.clone()));
        let allowed = match kind.as_str() {
            "filesystem" => {
                catalog.insert(
                    "warehouse".to_owned(),
                    serde_json::Value::String(request.location.clone()),
                );
                &[
                    "catalog",
                    "namespace",
                    "table",
                    "selector",
                    "mode",
                    "object_credentials",
                    "egress_allowlist",
                ] as &[_]
            }
            "rest" => {
                catalog.insert(
                    "uri".to_owned(),
                    serde_json::Value::String(request.location.clone()),
                );
                copy_add_option(&request.options, "warehouse", &mut catalog);
                copy_add_option(&request.options, "credentials", &mut catalog);
                &[
                    "catalog",
                    "namespace",
                    "table",
                    "selector",
                    "mode",
                    "warehouse",
                    "credentials",
                    "object_credentials",
                    "egress_allowlist",
                ] as &[_]
            }
            "glue" => {
                catalog.insert(
                    "region".to_owned(),
                    serde_json::Value::String(request.location.clone()),
                );
                for key in ["catalog_id", "warehouse", "endpoint", "credentials"] {
                    copy_add_option(&request.options, key, &mut catalog);
                }
                &[
                    "catalog",
                    "namespace",
                    "table",
                    "selector",
                    "mode",
                    "catalog_id",
                    "warehouse",
                    "endpoint",
                    "credentials",
                    "object_credentials",
                    "egress_allowlist",
                ] as &[_]
            }
            other => {
                return Err(CdfError::contract(format!(
                    "Iceberg cdf add catalog must be `filesystem`, `rest`, or `glue`, not `{other}`"
                )));
            }
        };
        let unknown = request
            .options
            .keys()
            .filter(|key| !allowed.contains(&key.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Err(CdfError::contract(format!(
                "Iceberg {kind} cdf add received unsupported options: {}",
                unknown.join(", ")
            )));
        }
        let mut source_options =
            BTreeMap::from([("catalog".to_owned(), serde_json::Value::Object(catalog))]);
        if let Some(reference) = request.options.get("object_credentials") {
            source_options.insert(
                "object_credentials".to_owned(),
                serde_json::Value::String(reference.clone()),
            );
        }
        if let Some(hosts) = request.options.get("egress_allowlist") {
            source_options.insert(
                "egress_allowlist".to_owned(),
                serde_json::to_value(parse_add_string_list("Iceberg egress allowlist", hosts)?)
                    .map_err(|error| {
                        CdfError::internal(format!("encode Iceberg add allowlist: {error}"))
                    })?,
            );
        }
        let resource = IcebergResourceOptions {
            namespace,
            table,
            selector: serde_json::from_value(selector.clone()).map_err(|error| {
                CdfError::contract(format!("Iceberg cdf add selector is invalid: {error}"))
            })?,
            mode,
        };
        resource.validate()?;
        let source: IcebergSourceOptions = serde_json::from_value(serde_json::Value::Object(
            source_options.clone().into_iter().collect(),
        ))
        .map_err(|error| {
            CdfError::contract(format!("Iceberg cdf add source is invalid: {error}"))
        })?;
        source.validate()?;
        Ok(Some(SourceAddProposal {
            source_kind: "iceberg".to_owned(),
            source_options,
            resource_options: BTreeMap::from([
                (
                    "namespace".to_owned(),
                    serde_json::json!(resource.namespace),
                ),
                ("table".to_owned(), serde_json::json!(resource.table)),
                ("selector".to_owned(), selector),
                ("mode".to_owned(), serde_json::json!(resource.mode)),
            ]),
            cursor: None,
            display_location: SourceEvidenceLocation::from_operational(&request.location)?,
            display_selection: resource.display_name(),
            private_files: Vec::new(),
        }))
    }
}

fn copy_add_option(
    options: &BTreeMap<String, String>,
    key: &str,
    output: &mut serde_json::Map<String, serde_json::Value>,
) {
    if let Some(value) = options.get(key) {
        output.insert(key.to_owned(), serde_json::Value::String(value.clone()));
    }
}

fn parse_add_string_list(label: &str, value: &str) -> Result<Vec<String>> {
    if value.starts_with('[') {
        return serde_json::from_str::<Vec<String>>(value).map_err(|error| {
            CdfError::contract(format!("{label} JSON array is invalid: {error}"))
        });
    }
    Ok(vec![value.to_owned()])
}

fn parse_add_selector(value: &str) -> Result<serde_json::Value> {
    if value == "current" {
        return Ok(serde_json::json!({"kind": "current"}));
    }
    let (kind, argument) = value.split_once(':').ok_or_else(|| {
        CdfError::contract(
            "Iceberg cdf add selector must be `current`, `branch:<name>`, `tag:<name>`, `snapshot:<id>`, or `timestamp:<epoch-ms>`",
        )
    })?;
    match kind {
        "branch" | "tag" => Ok(serde_json::json!({"kind": kind, "name": argument})),
        "snapshot" => Ok(serde_json::json!({
            "kind": "snapshot",
            "snapshot_id": argument.parse::<i64>().map_err(|_| {
                CdfError::contract("Iceberg snapshot selector id must be a positive integer")
            })?
        })),
        "timestamp" => Ok(serde_json::json!({
            "kind": "timestamp",
            "timestamp_ms": argument.parse::<i64>().map_err(|_| {
                CdfError::contract("Iceberg timestamp selector must be epoch milliseconds")
            })?
        })),
        _ => Err(CdfError::contract(
            "Iceberg cdf add selector must be `current`, `branch:<name>`, `tag:<name>`, `snapshot:<id>`, or `timestamp:<epoch-ms>`",
        )),
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
    compiled_source_plan_hash: cdf_kernel::CompiledSourcePlanHash,
    capabilities: ResourceCapabilities,
    type_policy_allowances: TypePolicyAllowances,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
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

    fn compiled_source_plan_hash(&self) -> Option<&cdf_kernel::CompiledSourcePlanHash> {
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

    fn planned_partition_reader(
        &self,
        reference: &PlannedTaskSetReference,
    ) -> Result<Box<dyn PlannedPartitionReader>> {
        Ok(Box::new(IcebergPlannedPartitionReader::open(
            &self.task_store,
            reference.clone(),
            &self.source,
            self.catalog.execution.memory(),
        )?))
    }

    fn rebind_scan_for_resume(
        &self,
        scan: ScanPlan,
        committed_frontier: &SourcePosition,
    ) -> Result<ScanPlan> {
        committed_frontier.validate()?;
        let SourcePosition::TableSnapshot(committed) = committed_frontier else {
            return Err(CdfError::data(format!(
                "Iceberg resource `{}` cannot resume from a {} position",
                self.descriptor.resource_id,
                committed_frontier.kind().as_str()
            )));
        };
        let selected = self.table.selected.as_ref().ok_or_else(|| {
            CdfError::data(format!(
                "Iceberg resource `{}` has a committed table snapshot but the selected table is empty",
                self.descriptor.resource_id
            ))
        })?;
        if committed.as_ref() == &selected.position {
            return self.retain_append_snapshot_tasks(scan, &BTreeSet::new());
        }
        if self.table.resource.mode == IcebergScanMode::AppendSnapshots {
            let admitted_snapshots = append_snapshot_ancestry(&self.table, committed, selected)?;
            return self.retain_append_snapshot_tasks(scan, &admitted_snapshots);
        }
        Ok(scan)
    }

    fn open(&self, _partition: PartitionPlan) -> PartitionOpenAttempt<'_> {
        PartitionOpenAttempt::materialized(Box::pin(async {
            Err(CdfError::contract(
                "Iceberg executes retained external tasks; open an executable partition from its planned task-set reader",
            ))
        }))
    }

    fn open_executable(&self, partition: ExecutablePartition) -> PartitionOpenAttempt<'_> {
        let retained = partition
            .retention()
            .and_then(PayloadRetention::downcast_ref::<IcebergExecutableTask>)
            .cloned();
        let Some(executable) = retained else {
            return PartitionOpenAttempt::materialized(Box::pin(async {
                Err(CdfError::contract(
                    "Iceberg executable partition omitted its retained canonical task payload",
                ))
            }));
        };
        let execution = self.catalog.execution.clone();
        if let Err(error) =
            execution.ensure_blocking_lanes(std::slice::from_ref(&self.catalog.blocking_lane))
        {
            return PartitionOpenAttempt::materialized(Box::pin(async move { Err(error) }));
        }
        let partition = partition.into_plan();
        let scope_id = format!("iceberg-open-{}", partition.partition_id.as_str());
        let prepare_context = self.catalog.clone();
        let prepare_source = self.source.clone();
        let producer_source = self.source.clone();
        let descriptor = self.descriptor.clone();
        let output_schema = Arc::clone(&self.schema);
        let producer_partition = partition.clone();
        let memory = execution.memory();
        let maximum_items = usize::from(self.source.stream_buffer_batches);
        let (completion_sender, completion_receiver) = futures_channel::oneshot::channel();
        let task = execution.spawn_blocking_prepared_cpu_stream(
            &scope_id,
            ICEBERG_SOURCE_BLOCKING_LANE_ID,
            CpuTaskSpec {
                task_kind: "source.iceberg.parquet".to_owned(),
                cpu_slot_cost: 1,
                native_internal_parallelism: 1,
            },
            maximum_items,
            move |cancellation| {
                prepare_task_scan(&prepare_context, &prepare_source, executable, cancellation)
            },
            move |prepared, sender, cancellation| async move {
                let completion = execute_task_scan(
                    prepared,
                    IcebergTaskExecution {
                        descriptor,
                        output_schema,
                        partition: producer_partition,
                        source: producer_source,
                        memory,
                        sender,
                        cancellation,
                    },
                )
                .await?;
                let _ = completion_sender.send(completion);
                Ok(())
            },
        );
        let stream = match task {
            Ok(stream) => stream,
            Err(error) => {
                return PartitionOpenAttempt::materialized(Box::pin(async move { Err(error) }));
            }
        };
        let termination = stream.termination();
        let opening = Box::pin(async move {
            let stream = Box::pin(stream) as BatchStream;
            let completion = Box::pin(async move {
                completion_receiver.await.map_err(|_| {
                    CdfError::internal(
                        "Iceberg source scope reached EOF without publishing completion evidence",
                    )
                })
            });
            Ok(PartitionStreamPayload::new(stream, completion))
        });
        PartitionOpenAttempt::with_termination(opening, termination)
    }

    fn attest_executable(&self, partition: ExecutablePartition) -> PartitionAttestationAttempt<'_> {
        let executable = partition
            .retention()
            .and_then(PayloadRetention::downcast_ref::<IcebergExecutableTask>)
            .cloned();
        let output_schema = Arc::clone(&self.schema);
        PartitionAttestationAttempt::materialized(Box::pin(async move {
            let executable = executable.ok_or_else(|| {
                CdfError::contract(
                    "Iceberg executable attestation omitted its retained canonical task payload",
                )
            })?;
            executable.task.validate_against(executable.authority())?;
            let snapshot = executable.authority().snapshot.clone().ok_or_else(|| {
                CdfError::contract("Iceberg executable task omitted immutable snapshot authority")
            })?;
            let output_schema = project_output_schema(
                output_schema.as_ref(),
                &executable.authority().projected_field_ids,
            )?;
            Ok(Some(PartitionAttestation::new(
                SourcePosition::TableSnapshot(Box::new(snapshot)),
                Some(cdf_kernel::canonical_arrow_schema_hash(
                    output_schema.as_ref(),
                )?),
            )))
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

impl IcebergResource {
    fn retain_append_snapshot_tasks(
        &self,
        scan: ScanPlan,
        admitted_snapshots: &BTreeSet<i64>,
    ) -> Result<ScanPlan> {
        let reference = scan.external_task_set().cloned().ok_or_else(|| {
            CdfError::data(format!(
                "Iceberg append_snapshots resource `{}` has no planned task-set authority",
                self.descriptor.resource_id
            ))
        })?;
        let mut reader = self.task_store.reader(
            reference,
            ICEBERG_TASK_SET_TYPE,
            self.source.maximum_task_bytes,
            self.source.maximum_task_authority_bytes,
            self.catalog.execution.memory(),
        )?;
        let authority: IcebergTaskSetAuthority =
            serde_json::from_slice(reader.authority().payload()).map_err(|error| {
                CdfError::data(format!("decode Iceberg task-set authority: {error}"))
            })?;
        let authority = authority.into_validated()?;
        let spill = self.catalog.execution.spill();
        let mut writer = self.task_store.writer(
            ICEBERG_TASK_SET_TYPE,
            TaskSetLimits {
                maximum_task_bytes: self.source.maximum_task_bytes,
                maximum_authority_bytes: self.source.maximum_task_authority_bytes,
                writer_buffer_bytes: self.source.task_writer_buffer_bytes,
            },
            self.catalog.execution.memory(),
            spill.as_ref(),
        )?;
        let mut next_ordinal = 0_u64;
        let mut estimated_rows = 0_u64;
        let mut estimated_bytes = 0_u64;
        while let Some(record) = reader.next_record()? {
            let mut task: IcebergScanTask = serde_json::from_slice(record.payload.payload())
                .map_err(|error| CdfError::data(format!("decode Iceberg scan task: {error}")))?;
            task.validate_against(&authority)?;
            if !admitted_snapshots.contains(&task.data_file.added_snapshot_id) {
                continue;
            }
            task.canonical_ordinal = next_ordinal;
            estimated_rows = estimated_rows
                .checked_add(task.data_file.record_count.unwrap_or(0))
                .ok_or_else(|| CdfError::data("Iceberg append row estimate exceeds u64"))?;
            estimated_bytes = estimated_bytes
                .checked_add(task.data_file.file_size_bytes)
                .ok_or_else(|| CdfError::data("Iceberg append byte estimate exceeds u64"))?;
            task.append_to(&mut writer)?;
            next_ordinal = next_ordinal
                .checked_add(1)
                .ok_or_else(|| CdfError::data("Iceberg append task ordinal exceeds u64"))?;
        }
        let artifact = writer.finalize(|output| authority.encode_to(output))?;
        if artifact.authority_sha256 != authority.content_sha256() {
            return Err(CdfError::internal(
                "filtered Iceberg task-set authority hash changed during append binding",
            ));
        }
        let mut rebound = scan.try_map_partition_authority(|planned| match planned {
            PartitionAuthority::External(_) => Ok(PartitionAuthority::External(artifact.reference)),
            PartitionAuthority::Inline(_) => Err(CdfError::contract(
                "Iceberg append resume binding requires external task authority",
            )),
        })?;
        rebound.estimated_rows = Some(estimated_rows);
        rebound.planned_source_bytes = Some(cdf_kernel::PlannedSourceBytes::new(estimated_bytes));
        Ok(rebound)
    }
}

fn append_snapshot_ancestry(
    table: &LoadedIcebergTable,
    committed: &cdf_kernel::TableSnapshotPosition,
    selected: &crate::SelectedIcebergSnapshot,
) -> Result<BTreeSet<i64>> {
    const REMEDIES: &str =
        "use mode = `snapshot` with replace semantics, or use a changelog-capable disposition";
    if committed.protocol != "iceberg"
        || committed.catalog != selected.position.catalog
        || committed.namespace != selected.position.namespace
        || committed.table != selected.position.table
        || committed.selector != selected.position.selector
    {
        return Err(CdfError::data(format!(
            "Iceberg append_snapshots checkpoint does not identify the selected catalog, table, and ref; {REMEDIES}"
        )));
    }
    let committed_snapshot = table
        .metadata
        .snapshot_by_id(committed.snapshot_id)
        .ok_or_else(|| {
            CdfError::data(format!(
                "Iceberg append_snapshots history no longer contains committed snapshot {}; {REMEDIES}",
                committed.snapshot_id
            ))
        })?;
    if committed_snapshot.sequence_number() != committed.sequence_number
        || committed_snapshot.parent_snapshot_id() != committed.parent_snapshot_id
    {
        return Err(CdfError::data(format!(
            "Iceberg committed snapshot {} does not match its recorded sequence/parent authority; {REMEDIES}",
            committed.snapshot_id
        )));
    }

    let mut admitted = BTreeSet::new();
    let mut current_id = selected.position.snapshot_id;
    while current_id != committed.snapshot_id {
        if !admitted.insert(current_id) {
            return Err(CdfError::data("Iceberg snapshot ancestry contains a cycle"));
        }
        let snapshot = table.metadata.snapshot_by_id(current_id).ok_or_else(|| {
            CdfError::data(format!(
                "Iceberg append_snapshots history is missing intervening snapshot {current_id}; {REMEDIES}"
            ))
        })?;
        if snapshot.summary().operation != iceberg::spec::Operation::Append {
            return Err(CdfError::data(format!(
                "Iceberg append_snapshots cannot cross snapshot {current_id} operation `{}`; {REMEDIES}",
                snapshot.summary().operation.as_str()
            )));
        }
        let Some(parent_id) = snapshot.parent_snapshot_id() else {
            return Err(CdfError::data(format!(
                "Iceberg committed snapshot {} is not an ancestor of selected snapshot {}; {REMEDIES}",
                committed.snapshot_id, selected.position.snapshot_id
            )));
        };
        current_id = parent_id;
    }
    Ok(admitted)
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
        incremental: IncrementalShape::TableSnapshot,
        replay: ReplaySupport::ExactRecordedBatches,
        idempotent_reads: true,
        backpressure: BackpressureSupport::Pausable,
        estimates: EstimateSupport::RowsAndBytes,
    }
}

fn execution_capabilities(source: &IcebergSourceOptions) -> Result<SourceExecutionCapabilities> {
    Ok(SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: 32 * 1024 * 1024,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: source.execution_working_set_bytes()?,
        maximum_emitted_batch_bytes: source.maximum_emitted_batch_bytes,
        maximum_concurrency: source.maximum_concurrency,
        useful_concurrency: source.maximum_concurrency,
        executor_class: SourceExecutorClass::BlockingLane,
        blocking_lane: Some(iceberg_source_blocking_lane()),
        pausable: true,
        spillable: true,
        idempotent_reads: true,
        reopenable: true,
        resumable: true,
        speculative_safe: true,
        retry_granularity: SourceRetryGranularity::Partition,
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
    })
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap, VecDeque},
        fs,
        io::Write,
        path::Path,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
    use cdf_engine::{EngineExecutionConfig, EnginePlanInput, EnginePreviewLimits, Planner};
    use cdf_http::{
        HttpRequest, HttpResponse, HttpResponseBudget, HttpTransport, SecretProvider, SecretUri,
        SecretValue,
    };
    use cdf_kernel::{
        BoxFuture, ContentStoreNamespace, DiscoveryManifestHash, DiscoveryManifestReference,
        EffectiveSchemaEvidence, ExecutionExtent, PredicateId, ResourceDescriptor, ResourceId,
        SchemaSnapshotReference, SchemaSource, ScopeKey, TrustLevel, WriteDisposition,
    };
    use cdf_object_access::{
        FileIdentityMetadata, FileIdentityStream, FileMetadataObservation, FileTransport,
        FileTransportControl, FileTransportFacade, FileTransportLocation, FileTransportResource,
    };
    use cdf_runtime::{
        BlockingTask, CanonicalPartitionSchedule, CompiledSourceExecutionPlan, CpuFutureTask,
        CpuTaskSpec, ExecutionHost, ExecutionHostCapabilities, ExecutionServices,
        ExecutionTaskScope, FixedSpillBudget, IoTask, IoValue, IoValueTask, RunCancellation,
        SourceCompileContext, SourceHealthStatus, SourceRegistry, SourceResolutionContext,
        SpillBudgetCoordinator, TaskScopeReport,
    };
    use cdf_task_store::ExternalTaskStore;
    use flate2::{Compression, write::GzEncoder};
    use futures_util::TryStreamExt;
    use iceberg::{
        io::FileIO,
        spec::{
            DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter,
            ManifestWriterBuilder, Struct, TableMetadata,
        },
    };
    use parquet::{arrow::ArrowWriter, basic::Compression as ParquetCompression};

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
    struct FaultInjectingFileTransport {
        inner: Arc<dyn FileTransport>,
        metadata_failures: Arc<AtomicUsize>,
        metadata_cancellation: Arc<Mutex<Option<RunCancellation>>>,
        target_suffix: Arc<str>,
    }

    impl FaultInjectingFileTransport {
        fn targets(&self, resource: &FileTransportResource) -> bool {
            let location = match &resource.location {
                FileTransportLocation::LocalPath { path } => path,
                FileTransportLocation::FileUrl { url }
                | FileTransportLocation::HttpUrl { url }
                | FileTransportLocation::RemoteUrl { url } => url,
            };
            location.ends_with(self.target_suffix.as_ref())
        }

        fn before_metadata(&self, resource: &FileTransportResource) -> Result<()> {
            if !self.targets(resource) {
                return Ok(());
            }
            if let Some(cancellation) = self
                .metadata_cancellation
                .lock()
                .map_err(|_| CdfError::internal("Iceberg test fault control is poisoned"))?
                .take()
            {
                cancellation.cancel();
            }
            if self
                .metadata_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(CdfError::transient(
                    "injected Iceberg data-object metadata failure",
                ));
            }
            Ok(())
        }
    }

    impl FileTransport for FaultInjectingFileTransport {
        fn metadata(
            &self,
            egress: &cdf_runtime::SourceEgressScope,
            resource: &FileTransportResource,
            control: &FileTransportControl,
        ) -> Result<FileMetadataObservation> {
            self.before_metadata(resource)?;
            self.inner.metadata(egress, resource, control)
        }

        fn metadata_if_exists(
            &self,
            egress: &cdf_runtime::SourceEgressScope,
            resource: &FileTransportResource,
            control: &FileTransportControl,
        ) -> Result<Option<FileMetadataObservation>> {
            self.before_metadata(resource)?;
            self.inner.metadata_if_exists(egress, resource, control)
        }

        fn list(
            &self,
            egress: &cdf_runtime::SourceEgressScope,
            resource: &FileTransportResource,
            maximum_results: usize,
            control: &FileTransportControl,
        ) -> Result<FileIdentityStream> {
            self.inner.list(egress, resource, maximum_results, control)
        }

        fn open_byte_source(
            &self,
            egress: &cdf_runtime::SourceEgressScope,
            resource: &FileTransportResource,
            expected: &FileIdentityMetadata,
            memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        ) -> Result<Arc<dyn cdf_runtime::ByteSource>> {
            self.inner
                .open_byte_source(egress, resource, expected, memory)
        }
    }

    #[derive(Clone)]
    struct StaticGlueCatalogClient {
        pointer: crate::GlueTablePointer,
        requests: Arc<Mutex<Vec<crate::GlueGetTableRequest>>>,
    }

    impl crate::GlueCatalogClient for StaticGlueCatalogClient {
        fn get_table(
            &self,
            request: crate::GlueGetTableRequest,
        ) -> cdf_kernel::BoxFuture<'_, Result<crate::GlueTablePointer>> {
            self.requests.lock().unwrap().push(request);
            let pointer = self.pointer.clone();
            Box::pin(async move { Ok(pointer) })
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
            let runtime = self.runtime.handle().clone();
            Box::pin(async move {
                cancellation.check()?;
                let sleep = runtime.spawn(async move {
                    tokio::time::sleep(duration).await;
                });
                sleep.await.map_err(|error| {
                    CdfError::internal(format!("Iceberg test delay task failed: {error}"))
                })?;
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

    fn nonempty_table_metadata(location: &Path, manifest_list: &Path) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "format-version": 2,
            "table-uuid": "9c12d441-03fe-4693-9a96-a0705ddf69c1",
            "location": location.display().to_string(),
            "last-sequence-number": 1,
            "last-updated-ms": 1_602_638_573_590_i64,
            "last-column-id": 2,
            "current-schema-id": 1,
            "schemas": [
                {
                    "type": "struct",
                    "schema-id": 0,
                    "fields": [
                        {"id": 1, "name": "id", "required": true, "type": "long"}
                    ]
                },
                {
                    "type": "struct",
                    "schema-id": 1,
                    "fields": [
                        {"id": 1, "name": "id", "required": true, "type": "long"},
                        {"id": 2, "name": "label", "required": false, "type": "string"}
                    ]
                },
                {
                    "type": "struct",
                    "schema-id": 99,
                    "fields": [
                        {"id": 1, "name": "id", "required": true, "type": "long"},
                        {"id": 2, "name": "label", "required": false, "type": "string"}
                    ]
                }
            ],
            "default-spec-id": 1,
            "partition-specs": [
                {"spec-id": 0, "fields": []},
                {"spec-id": 1, "fields": [{
                    "source-id": 1,
                    "field-id": 1000,
                    "name": "id_bucket",
                    "transform": "bucket[1]"
                }]}
            ],
            "last-partition-id": 1000,
            "default-sort-order-id": 0,
            "sort-orders": [{"order-id": 0, "fields": []}],
            "properties": {},
            "current-snapshot-id": 101,
            "snapshots": [{
                "snapshot-id": 101,
                "timestamp-ms": 1_602_638_573_590_i64,
                "sequence-number": 1,
                "schema-id": 1,
                "summary": {
                    "operation": "append",
                    "added-data-files": "2",
                    "added-records": "8",
                    "added-files-size": "333"
                },
                "manifest-list": manifest_list.display().to_string()
            }],
            "snapshot-log": [{
                "snapshot-id": 101,
                "timestamp-ms": 1_602_638_573_590_i64
            }],
            "metadata-log": [],
            "refs": {"main": {"snapshot-id": 101, "type": "branch"}}
        }))
        .unwrap()
    }

    fn write_nonempty_table_fixture(execution: &ExecutionServices, table: &Path) {
        let table = table.to_path_buf();
        execution
            .run_io(async move {
                let metadata_dir = table.join("metadata");
                fs::create_dir_all(&metadata_dir).unwrap();
                let data_dir = table.join("data");
                fs::create_dir_all(&data_dir).unwrap();
                let field_id = |value: i32| {
                    HashMap::from([("PARQUET:field_id".to_owned(), value.to_string())])
                };
                let old_schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false).with_metadata(field_id(1)),
                ]));
                let old_batch = RecordBatch::try_new(
                    Arc::clone(&old_schema),
                    vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
                )
                .unwrap();
                let old_data_path = data_dir.join("old.parquet");
                let mut old_data = ArrowWriter::try_new(
                    fs::File::create(&old_data_path).unwrap(),
                    old_schema,
                    Some(
                        parquet::file::properties::WriterProperties::builder()
                            .set_compression(ParquetCompression::SNAPPY)
                            .build(),
                    ),
                )
                .unwrap();
                old_data.write(&old_batch).unwrap();
                old_data.close().unwrap();

                let current_schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false).with_metadata(field_id(1)),
                    Field::new("label", DataType::Utf8, true).with_metadata(field_id(2)),
                ]));
                let current_batch = RecordBatch::try_new(
                    Arc::clone(&current_schema),
                    vec![
                        Arc::new(Int64Array::from(vec![4_i64, 5, 6, 7, 8])),
                        Arc::new(StringArray::from(vec![
                            Some("four"),
                            Some("five"),
                            None,
                            Some("seven"),
                            Some("eight"),
                        ])),
                    ],
                )
                .unwrap();
                let current_data_path = data_dir.join("current.parquet");
                let mut current_data = ArrowWriter::try_new(
                    fs::File::create(&current_data_path).unwrap(),
                    current_schema,
                    Some(
                        parquet::file::properties::WriterProperties::builder()
                            .set_compression(ParquetCompression::SNAPPY)
                            .build(),
                    ),
                )
                .unwrap();
                current_data.write(&current_batch).unwrap();
                current_data.close().unwrap();

                let position_delete_path = data_dir.join("position-delete.parquet");
                let position_delete_schema = Arc::new(Schema::new(vec![
                    Field::new("file_path", DataType::Utf8, false)
                        .with_metadata(field_id(i32::MAX - 101)),
                    Field::new("pos", DataType::Int64, false)
                        .with_metadata(field_id(i32::MAX - 102)),
                ]));
                let position_delete_batch = RecordBatch::try_new(
                    Arc::clone(&position_delete_schema),
                    vec![
                        Arc::new(StringArray::from(vec![old_data_path.display().to_string()])),
                        Arc::new(Int64Array::from(vec![1_i64])),
                    ],
                )
                .unwrap();
                let mut position_delete = ArrowWriter::try_new(
                    fs::File::create(&position_delete_path).unwrap(),
                    position_delete_schema,
                    None,
                )
                .unwrap();
                position_delete.write(&position_delete_batch).unwrap();
                position_delete.close().unwrap();

                let equality_delete_path = data_dir.join("equality-delete.parquet");
                let equality_delete_schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false).with_metadata(field_id(1)),
                ]));
                let equality_delete_batch = RecordBatch::try_new(
                    Arc::clone(&equality_delete_schema),
                    vec![Arc::new(Int64Array::from(vec![5_i64]))],
                )
                .unwrap();
                let mut equality_delete = ArrowWriter::try_new(
                    fs::File::create(&equality_delete_path).unwrap(),
                    equality_delete_schema,
                    None,
                )
                .unwrap();
                equality_delete.write(&equality_delete_batch).unwrap();
                equality_delete.close().unwrap();

                let partition_equality_delete_path =
                    data_dir.join("partition-equality-delete.parquet");
                let partition_equality_delete_schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false).with_metadata(field_id(1)),
                ]));
                let partition_equality_delete_batch = RecordBatch::try_new(
                    Arc::clone(&partition_equality_delete_schema),
                    vec![Arc::new(Int64Array::from(vec![7_i64]))],
                )
                .unwrap();
                let mut partition_equality_delete = ArrowWriter::try_new(
                    fs::File::create(&partition_equality_delete_path).unwrap(),
                    partition_equality_delete_schema,
                    None,
                )
                .unwrap();
                partition_equality_delete
                    .write(&partition_equality_delete_batch)
                    .unwrap();
                partition_equality_delete.close().unwrap();

                let manifest_list_path = metadata_dir.join("snap-101.avro");
                let metadata_bytes = nonempty_table_metadata(&table, &manifest_list_path);
                let table_metadata: TableMetadata =
                    serde_json::from_slice(&metadata_bytes).unwrap();
                let file_io = FileIO::new_with_fs();
                let unpartitioned_spec = table_metadata
                    .partition_spec_by_id(0)
                    .unwrap()
                    .as_ref()
                    .clone();
                let partitioned_spec = table_metadata
                    .partition_spec_by_id(1)
                    .unwrap()
                    .as_ref()
                    .clone();

                let old_manifest_path = metadata_dir.join("manifest-a.avro");
                let mut old_writer = ManifestWriterBuilder::new(
                    file_io
                        .new_output(old_manifest_path.to_string_lossy())
                        .unwrap(),
                    Some(101),
                    table_metadata.schema_by_id(0).unwrap().clone(),
                    unpartitioned_spec.clone(),
                )
                .build_v2_data();
                old_writer
                    .add_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(old_data_path.display().to_string())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(fs::metadata(&old_data_path).unwrap().len())
                            .record_count(3)
                            .partition(Struct::empty())
                            .build()
                            .unwrap(),
                        1,
                    )
                    .unwrap();
                let old_manifest = old_writer.write_manifest_file().await.unwrap();

                let current_manifest_path = metadata_dir.join("manifest-b.avro");
                let mut current_writer = ManifestWriterBuilder::new(
                    file_io
                        .new_output(current_manifest_path.to_string_lossy())
                        .unwrap(),
                    Some(101),
                    table_metadata.schema_by_id(1).unwrap().clone(),
                    partitioned_spec.clone(),
                )
                .build_v2_data();
                current_writer
                    .add_file(
                        DataFileBuilder::default()
                            .partition_spec_id(1)
                            .content(DataContentType::Data)
                            .file_path(current_data_path.display().to_string())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(fs::metadata(&current_data_path).unwrap().len())
                            .record_count(5)
                            .partition(Struct::from_iter([Some(iceberg::spec::Literal::int(0))]))
                            .build()
                            .unwrap(),
                        1,
                    )
                    .unwrap();
                let current_manifest = current_writer.write_manifest_file().await.unwrap();

                let delete_manifest_path = metadata_dir.join("manifest-deletes.avro");
                let mut delete_writer = ManifestWriterBuilder::new(
                    file_io
                        .new_output(delete_manifest_path.to_string_lossy())
                        .unwrap(),
                    Some(101),
                    table_metadata.schema_by_id(1).unwrap().clone(),
                    unpartitioned_spec,
                )
                .build_v2_deletes();
                delete_writer
                    .add_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::PositionDeletes)
                            .file_path(position_delete_path.display().to_string())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(fs::metadata(&position_delete_path).unwrap().len())
                            .record_count(1)
                            .partition(Struct::empty())
                            .referenced_data_file(Some(old_data_path.display().to_string()))
                            .build()
                            .unwrap(),
                        2,
                    )
                    .unwrap();
                delete_writer
                    .add_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::EqualityDeletes)
                            .file_path(equality_delete_path.display().to_string())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(fs::metadata(&equality_delete_path).unwrap().len())
                            .record_count(1)
                            .partition(Struct::empty())
                            .equality_ids(Some(vec![1]))
                            .build()
                            .unwrap(),
                        2,
                    )
                    .unwrap();
                let delete_manifest = delete_writer.write_manifest_file().await.unwrap();

                let partition_delete_manifest_path =
                    metadata_dir.join("manifest-partition-deletes.avro");
                let mut partition_delete_writer = ManifestWriterBuilder::new(
                    file_io
                        .new_output(partition_delete_manifest_path.to_string_lossy())
                        .unwrap(),
                    Some(101),
                    table_metadata.schema_by_id(1).unwrap().clone(),
                    partitioned_spec,
                )
                .build_v2_deletes();
                partition_delete_writer
                    .add_file(
                        DataFileBuilder::default()
                            .partition_spec_id(1)
                            .content(DataContentType::EqualityDeletes)
                            .file_path(partition_equality_delete_path.display().to_string())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(
                                fs::metadata(&partition_equality_delete_path).unwrap().len(),
                            )
                            .record_count(1)
                            .partition(Struct::from_iter([Some(iceberg::spec::Literal::int(0))]))
                            .equality_ids(Some(vec![1]))
                            .build()
                            .unwrap(),
                        2,
                    )
                    .unwrap();
                let partition_delete_manifest =
                    partition_delete_writer.write_manifest_file().await.unwrap();

                let list_output = file_io
                    .new_output(manifest_list_path.to_string_lossy())
                    .unwrap()
                    .writer()
                    .await
                    .unwrap();
                let mut list_writer = ManifestListWriter::v2(list_output, 101, None, 1);
                // Deliberately reverse canonical path order; CDF planning must normalize it.
                list_writer
                    .add_manifests(
                        [
                            current_manifest,
                            delete_manifest,
                            old_manifest,
                            partition_delete_manifest,
                        ]
                        .into_iter(),
                    )
                    .unwrap();
                list_writer.close().await.unwrap();

                fs::write(metadata_dir.join("v1.metadata.json"), metadata_bytes).unwrap();
                fs::write(metadata_dir.join("version-hint.text"), "1\n").unwrap();
                Ok(())
            })
            .unwrap();
    }

    fn write_append_history_fixture(
        execution: &ExecutionServices,
        table: &Path,
        current_operation: &'static str,
    ) {
        let table = table.to_path_buf();
        execution
            .run_io(async move {
                let metadata_dir = table.join("metadata");
                let data_dir = table.join("data");
                fs::create_dir_all(&metadata_dir).unwrap();
                fs::create_dir_all(&data_dir).unwrap();
                let old_data = data_dir.join("old.parquet");
                let appended_data = data_dir.join("appended.parquet");
                fs::write(&old_data, [0_u8]).unwrap();
                fs::write(&appended_data, [1_u8]).unwrap();
                let old_list = metadata_dir.join("snap-100.avro");
                let current_list = metadata_dir.join("snap-101.avro");
                let metadata_bytes = serde_json::to_vec(&serde_json::json!({
                    "format-version": 2,
                    "table-uuid": "a9eac340-e346-4a86-9b8e-68aef02da467",
                    "location": table.display().to_string(),
                    "last-sequence-number": 2,
                    "last-updated-ms": 2000_i64,
                    "last-column-id": 1,
                    "current-schema-id": 0,
                    "schemas": [{
                        "type": "struct",
                        "schema-id": 0,
                        "fields": [{"id": 1, "name": "id", "required": true, "type": "long"}]
                    }],
                    "default-spec-id": 0,
                    "partition-specs": [{"spec-id": 0, "fields": []}],
                    "last-partition-id": 999,
                    "default-sort-order-id": 0,
                    "sort-orders": [{"order-id": 0, "fields": []}],
                    "properties": {},
                    "current-snapshot-id": 101,
                    "snapshots": [
                        {
                            "snapshot-id": 99,
                            "timestamp-ms": 900_i64,
                            "sequence-number": 1,
                            "schema-id": 0,
                            "summary": {"operation": "append"},
                            "manifest-list": metadata_dir.join("snap-99.avro").display().to_string()
                        },
                        {
                            "snapshot-id": 100,
                            "timestamp-ms": 1000_i64,
                            "sequence-number": 1,
                            "schema-id": 0,
                            "summary": {"operation": "append"},
                            "manifest-list": old_list.display().to_string()
                        },
                        {
                            "snapshot-id": 101,
                            "parent-snapshot-id": 100,
                            "timestamp-ms": 2000_i64,
                            "sequence-number": 2,
                            "schema-id": 0,
                            "summary": {"operation": current_operation},
                            "manifest-list": current_list.display().to_string()
                        }
                    ],
                    "snapshot-log": [
                        {"snapshot-id": 99, "timestamp-ms": 900_i64},
                        {"snapshot-id": 100, "timestamp-ms": 1000_i64},
                        {"snapshot-id": 101, "timestamp-ms": 2000_i64}
                    ],
                    "metadata-log": [],
                    "refs": {"main": {"snapshot-id": 101, "type": "branch"}}
                }))
                .unwrap();
                let metadata: TableMetadata = serde_json::from_slice(&metadata_bytes).unwrap();
                let file_io = FileIO::new_with_fs();
                let schema = metadata.current_schema().clone();
                let spec = metadata.default_partition_spec().as_ref().clone();
                let mut old_writer = ManifestWriterBuilder::new(
                    file_io
                        .new_output(metadata_dir.join("manifest-old.avro").to_string_lossy())
                        .unwrap(),
                    Some(100),
                    Arc::clone(&schema),
                    spec.clone(),
                )
                .build_v2_data();
                old_writer
                    .add_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(old_data.display().to_string())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(1)
                            .record_count(3)
                            .partition(Struct::empty())
                            .build()
                            .unwrap(),
                        1,
                    )
                    .unwrap();
                let mut old_manifest = old_writer.write_manifest_file().await.unwrap();
                old_manifest.sequence_number = 1;
                old_manifest.min_sequence_number = 1;
                let old_output = file_io
                    .new_output(old_list.to_string_lossy())
                    .unwrap()
                    .writer()
                    .await
                    .unwrap();
                let mut old_list_writer = ManifestListWriter::v2(old_output, 100, None, 1);
                old_list_writer
                    .add_manifests([old_manifest.clone()].into_iter())
                    .unwrap();
                old_list_writer.close().await.unwrap();
                let mut appended_writer = ManifestWriterBuilder::new(
                    file_io
                        .new_output(
                            metadata_dir
                                .join("manifest-appended.avro")
                                .to_string_lossy(),
                        )
                        .unwrap(),
                    Some(101),
                    schema,
                    spec,
                )
                .build_v2_data();
                appended_writer
                    .add_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(appended_data.display().to_string())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(1)
                            .record_count(5)
                            .partition(Struct::empty())
                            .build()
                            .unwrap(),
                        2,
                    )
                    .unwrap();
                let appended_manifest = appended_writer.write_manifest_file().await.unwrap();
                let output = file_io
                    .new_output(current_list.to_string_lossy())
                    .unwrap()
                    .writer()
                    .await
                    .unwrap();
                let mut list = ManifestListWriter::v2(output, 101, Some(100), 2);
                list.add_manifests([old_manifest, appended_manifest].into_iter())
                    .unwrap();
                list.close().await.unwrap();
                fs::write(metadata_dir.join("v1.metadata.json"), metadata_bytes).unwrap();
                fs::write(metadata_dir.join("version-hint.text"), "1\n").unwrap();
                Ok(())
            })
            .unwrap();
    }

    fn write_v1_table_fixture(execution: &ExecutionServices, table: &Path) {
        let table = table.to_path_buf();
        execution
            .run_io(async move {
                let metadata_dir = table.join("metadata");
                let data_dir = table.join("data");
                fs::create_dir_all(&metadata_dir).unwrap();
                fs::create_dir_all(&data_dir).unwrap();
                let field_id = HashMap::from([("PARQUET:field_id".to_owned(), "1".to_owned())]);
                let arrow_schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false).with_metadata(field_id),
                ]));
                let batch = RecordBatch::try_new(
                    Arc::clone(&arrow_schema),
                    vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))],
                )
                .unwrap();
                let data_path = data_dir.join("v1-data.parquet");
                let mut data_writer = ArrowWriter::try_new(
                    fs::File::create(&data_path).unwrap(),
                    arrow_schema,
                    Some(
                        parquet::file::properties::WriterProperties::builder()
                            .set_compression(ParquetCompression::SNAPPY)
                            .build(),
                    ),
                )
                .unwrap();
                data_writer.write(&batch).unwrap();
                data_writer.close().unwrap();

                let manifest_list_path = metadata_dir.join("snap-101-v1.avro");
                let metadata_bytes = serde_json::to_vec(&serde_json::json!({
                    "format-version": 1,
                    "table-uuid": "f0614672-8242-4f06-80ef-38b03381d1a4",
                    "location": table.display().to_string(),
                    "last-updated-ms": 1_602_638_573_590_i64,
                    "last-column-id": 1,
                    "schema": {
                        "type": "struct",
                        "schema-id": 0,
                        "fields": [
                            {"id": 1, "name": "id", "required": true, "type": "long"}
                        ]
                    },
                    "partition-spec": [],
                    "last-partition-id": 999,
                    "default-sort-order-id": 0,
                    "sort-orders": [{"order-id": 0, "fields": []}],
                    "properties": {},
                    "current-snapshot-id": 101,
                    "snapshots": [{
                        "snapshot-id": 101,
                        "timestamp-ms": 1_602_638_573_590_i64,
                        "sequence-number": 0,
                        "schema-id": 0,
                        "summary": {
                            "operation": "append",
                            "added-data-files": "1",
                            "added-records": "3",
                            "added-files-size": fs::metadata(&data_path).unwrap().len().to_string()
                        },
                        "manifest-list": manifest_list_path.display().to_string()
                    }],
                    "snapshot-log": [{
                        "snapshot-id": 101,
                        "timestamp-ms": 1_602_638_573_590_i64
                    }],
                    "metadata-log": []
                }))
                .unwrap();
                let table_metadata: TableMetadata =
                    serde_json::from_slice(&metadata_bytes).unwrap();
                let file_io = FileIO::new_with_fs();
                let manifest_path = metadata_dir.join("manifest-v1.avro");
                let mut manifest_writer = ManifestWriterBuilder::new(
                    file_io.new_output(manifest_path.to_string_lossy()).unwrap(),
                    Some(101),
                    table_metadata.current_schema().clone(),
                    table_metadata.default_partition_spec().as_ref().clone(),
                )
                .build_v1();
                manifest_writer
                    .add_file(
                        DataFileBuilder::default()
                            .partition_spec_id(0)
                            .content(DataContentType::Data)
                            .file_path(data_path.display().to_string())
                            .file_format(DataFileFormat::Parquet)
                            .file_size_in_bytes(fs::metadata(&data_path).unwrap().len())
                            .record_count(3)
                            .partition(Struct::empty())
                            .build()
                            .unwrap(),
                        0,
                    )
                    .unwrap();
                let manifest = manifest_writer.write_manifest_file().await.unwrap();
                let list_output = file_io
                    .new_output(manifest_list_path.to_string_lossy())
                    .unwrap()
                    .writer()
                    .await
                    .unwrap();
                let mut list_writer = ManifestListWriter::v1(list_output, 101, Some(0));
                list_writer.add_manifests([manifest].into_iter()).unwrap();
                list_writer.close().await.unwrap();

                fs::write(metadata_dir.join("v1.metadata.json"), metadata_bytes).unwrap();
                fs::write(metadata_dir.join("version-hint.text"), "1\n").unwrap();
                Ok(())
            })
            .unwrap();
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

    fn planned_append_resource(
        root: &Path,
        operation: &'static str,
    ) -> (
        ExecutionServices,
        Arc<dyn QueryableResource>,
        ScanPlan,
        cdf_kernel::TableSnapshotPosition,
    ) {
        let table = root.join("analytics/events");
        let execution = execution_services();
        write_append_history_fixture(&execution, &table, operation);
        let driver = filesystem_driver();
        let mut request = compile_request(root);
        request
            .resource_options
            .insert("mode".to_owned(), serde_json::json!("append_snapshots"));
        let mut plan = driver.compile(request).unwrap();
        let context = SourceResolutionContext::new(
            root,
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
        plan.schema = observation.schema.as_ref().clone();
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
        let store = ExternalTaskStore::new(
            root.join(".cdf"),
            ContentStoreNamespace::new(PLANNING_ARTIFACT_NAMESPACE).unwrap(),
        )
        .unwrap();
        let mut reader = store
            .reader(
                scan.external_task_set().cloned().unwrap(),
                crate::ICEBERG_TASK_SET_TYPE,
                crate::DEFAULT_MAXIMUM_TASK_BYTES,
                crate::DEFAULT_MAXIMUM_TASK_AUTHORITY_BYTES,
                execution.memory(),
            )
            .unwrap();
        let authority: IcebergTaskSetAuthority =
            serde_json::from_slice(reader.authority().payload()).unwrap();
        while reader.next_record().unwrap().is_some() {}
        let mut committed = authority.snapshot.unwrap();
        committed.snapshot_id = 100;
        committed.sequence_number = 1;
        committed.parent_snapshot_id = None;
        (execution, resource, scan, committed)
    }

    fn rest_compile_request(project_root: &Path) -> SourceCompileRequest {
        let mut request = compile_request(project_root);
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
        request
    }

    fn rest_driver(transport: SequenceHttpTransport) -> IcebergSourceDriver {
        IcebergSourceDriver::new(move |secrets, execution, _egress, lane| {
            Ok(IcebergRuntimeDependencies::new(
                Arc::new(
                    FileTransportFacade::new()
                        .with_shared_secret_provider(secrets)
                        .with_execution_services(execution)
                        .with_local_listing_lane(lane)?,
                ),
                Arc::new(transport.clone()),
                Arc::new(UnsupportedGlueCatalogClient),
            ))
        })
        .unwrap()
    }

    fn filesystem_driver() -> IcebergSourceDriver {
        let http = NoopHttpTransport;
        IcebergSourceDriver::new(move |secrets, execution, _egress, lane| {
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
        .unwrap()
    }

    fn filesystem_driver_with_metadata_faults(
        metadata_failures: Arc<AtomicUsize>,
        metadata_cancellation: Arc<Mutex<Option<RunCancellation>>>,
        target_suffix: &'static str,
    ) -> IcebergSourceDriver {
        let http = NoopHttpTransport;
        IcebergSourceDriver::new(move |secrets, execution, _egress, lane| {
            let inner: Arc<dyn FileTransport> = Arc::new(
                FileTransportFacade::new()
                    .with_shared_secret_provider(secrets)
                    .with_execution_services(execution)
                    .with_local_listing_lane(lane)?,
            );
            Ok(IcebergRuntimeDependencies::new(
                Arc::new(FaultInjectingFileTransport {
                    inner,
                    metadata_failures: Arc::clone(&metadata_failures),
                    metadata_cancellation: Arc::clone(&metadata_cancellation),
                    target_suffix: Arc::from(target_suffix),
                }),
                Arc::new(http.clone()),
                Arc::new(UnsupportedGlueCatalogClient),
            ))
        })
        .unwrap()
    }

    fn add_request(
        location: &str,
        options: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> SourceAddRequest {
        SourceAddRequest {
            source_name: "lake".to_owned(),
            resource_name: "events".to_owned(),
            location: location.to_owned(),
            project_root: std::path::PathBuf::from("/project"),
            current_dir: std::path::PathBuf::from("/project"),
            options: options
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
            project_options: None,
        }
    }

    #[test]
    fn add_hook_compiles_explicit_catalogs_without_uri_guessing() {
        let driver = filesystem_driver();
        assert!(
            driver
                .propose_add(&add_request("/warehouse", []))
                .unwrap()
                .is_none()
        );

        let filesystem = driver
            .propose_add(&add_request(
                "/warehouse",
                [
                    ("catalog", "filesystem"),
                    ("namespace", r#"["org","analytics"]"#),
                    ("selector", "branch:main"),
                ],
            ))
            .unwrap()
            .unwrap();
        filesystem.validate().unwrap();
        assert_eq!(
            filesystem.source_options["catalog"],
            serde_json::json!({"kind": "filesystem", "warehouse": "/warehouse"})
        );
        assert_eq!(
            filesystem.resource_options["namespace"],
            serde_json::json!(["org", "analytics"])
        );
        assert_eq!(
            filesystem.resource_options["selector"],
            serde_json::json!({"kind": "branch", "name": "main"})
        );

        let glue = driver
            .propose_add(&add_request(
                "us-east-1",
                [
                    ("catalog", "glue"),
                    ("namespace", "analytics"),
                    ("catalog_id", "123456789012"),
                    ("credentials", "secret://aws/lake-reader"),
                    ("object_credentials", "secret://aws/lake-reader"),
                    (
                        "egress_allowlist",
                        r#"["glue.us-east-1.amazonaws.com","lake.s3.us-east-1.amazonaws.com"]"#,
                    ),
                ],
            ))
            .unwrap()
            .unwrap();
        glue.validate().unwrap();
        assert_eq!(glue.display_selection, "analytics.events");
        assert_eq!(
            glue.source_options["catalog"],
            serde_json::json!({
                "kind": "glue",
                "region": "us-east-1",
                "catalog_id": "123456789012",
                "credentials": "secret://aws/lake-reader"
            })
        );
        assert_eq!(
            glue.source_options["egress_allowlist"],
            serde_json::json!([
                "glue.us-east-1.amazonaws.com",
                "lake.s3.us-east-1.amazonaws.com"
            ])
        );
    }

    #[test]
    fn add_hook_rejects_cross_catalog_and_ambiguous_selector_options() {
        let driver = filesystem_driver();
        let error = driver
            .propose_add(&add_request(
                "/warehouse",
                [
                    ("catalog", "filesystem"),
                    ("namespace", "analytics"),
                    ("region", "us-east-1"),
                ],
            ))
            .unwrap_err();
        assert!(error.message.contains("unsupported options: region"));

        let error = driver
            .propose_add(&add_request(
                "https://catalog.example.test",
                [
                    ("catalog", "rest"),
                    ("namespace", "analytics"),
                    ("selector", "snapshot:not-a-number"),
                ],
            ))
            .unwrap_err();
        assert!(error.message.contains("positive integer"));
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
        let driver = filesystem_driver();
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
        assert_eq!(execution.capabilities().blocking_lanes.len(), 1);
        assert_eq!(
            execution.capabilities().blocking_lanes[0],
            BlockingLaneSpec {
                binding: BlockingLaneBinding::RuntimeResolved,
                maximum_concurrency: 2,
                ..iceberg_source_blocking_lane()
            }
        );

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
        assert_eq!(scan.partition_count().unwrap(), 0);
        assert_eq!(scan.estimated_rows, Some(0));
        assert_eq!(scan.planned_source_bytes.unwrap().get(), 0);
        assert_eq!(scan.external_task_set().unwrap().task_count, 0);

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
    fn filesystem_discovery_reads_bounded_gzip_table_metadata() {
        let root = tempfile::tempdir().unwrap();
        let table = root.path().join("analytics/events");
        let metadata = table.join("metadata");
        fs::create_dir_all(&metadata).unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&empty_table_metadata(&table)).unwrap();
        fs::write(
            metadata.join("v1.gz.metadata.json"),
            encoder.finish().unwrap(),
        )
        .unwrap();
        fs::write(metadata.join("version-hint.text"), "1\n").unwrap();

        let execution = execution_services();
        let driver = filesystem_driver();
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
            observation.source_identity["catalog_generation"],
            "hadoop-version:1"
        );
    }

    #[test]
    fn glue_binding_produces_the_same_pinned_table_semantics() {
        let root = tempfile::tempdir().unwrap();
        let table = root.path().join("analytics/events");
        let metadata_dir = table.join("metadata");
        fs::create_dir_all(&metadata_dir).unwrap();
        let metadata_location = metadata_dir.join("v1.metadata.json");
        fs::write(&metadata_location, empty_table_metadata(&table)).unwrap();

        let execution = execution_services();
        let filesystem_driver = filesystem_driver();
        let filesystem_plan = filesystem_driver
            .compile(compile_request(root.path()))
            .unwrap();
        let filesystem_context = SourceResolutionContext::new(
            root.path(),
            Arc::new(NoopSecretProvider),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let filesystem_session = filesystem_driver
            .discovery_session(&filesystem_plan, &filesystem_context)
            .unwrap();
        let filesystem_candidate = filesystem_session.candidates().unwrap().remove(0);
        let filesystem_observation = filesystem_session
            .observe(
                &filesystem_candidate,
                &SourceDiscoveryRequest::new(64 * 1024 * 1024, 1).unwrap(),
            )
            .unwrap();

        let requests = Arc::new(Mutex::new(Vec::new()));
        let glue = StaticGlueCatalogClient {
            pointer: crate::GlueTablePointer {
                metadata_location: metadata_location.display().to_string(),
                catalog_generation: Some("glue-version-7".to_owned()),
                bytes_read: 123,
                retained_bytes: 64,
            },
            requests: Arc::clone(&requests),
        };
        let driver = IcebergSourceDriver::new(move |secrets, execution, _egress, lane| {
            Ok(IcebergRuntimeDependencies::new(
                Arc::new(
                    FileTransportFacade::new()
                        .with_shared_secret_provider(secrets)
                        .with_execution_services(execution)
                        .with_local_listing_lane(lane)?,
                ),
                Arc::new(NoopHttpTransport),
                Arc::new(glue.clone()),
            ))
        })
        .unwrap();
        let mut glue_request = compile_request(root.path());
        glue_request.source_options.insert(
            "catalog".to_owned(),
            serde_json::json!({"kind": "glue", "region": "us-east-1"}),
        );
        let glue_plan = driver.compile(glue_request).unwrap();
        let glue_context = SourceResolutionContext::new(
            root.path(),
            Arc::new(NoopSecretProvider),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let glue_session = driver.discovery_session(&glue_plan, &glue_context).unwrap();
        let glue_candidate = glue_session.candidates().unwrap().remove(0);
        let glue_observation = glue_session
            .observe(
                &glue_candidate,
                &SourceDiscoveryRequest::new(64 * 1024 * 1024, 1).unwrap(),
            )
            .unwrap();

        assert_eq!(glue_observation.schema, filesystem_observation.schema);
        assert_eq!(
            glue_observation.source_identity["table_uuid"],
            filesystem_observation.source_identity["table_uuid"]
        );
        assert_eq!(
            glue_observation.source_identity["metadata_generation"],
            filesystem_observation.source_identity["metadata_generation"]
        );
        assert_eq!(
            glue_observation.source_identity["catalog_generation"],
            "glue-version-7"
        );
        {
            let observed = requests.lock().unwrap();
            assert_eq!(observed.len(), 1);
            assert_eq!(observed[0].database, "analytics");
            assert_eq!(observed[0].table, "events");
        }
        assert_eq!(glue_context.prepared_payloads().pending_count().unwrap(), 1);
        driver.resolve(&glue_plan, &glue_context).unwrap();
        assert_eq!(requests.lock().unwrap().len(), 1);
    }

    #[test]
    fn nonempty_snapshot_plans_canonical_tasks_independent_of_source_jobs() {
        let root = tempfile::tempdir().unwrap();
        let table = root.path().join("analytics/events");
        let execution = execution_services();
        write_nonempty_table_fixture(&execution, &table);
        let metadata_failures = Arc::new(AtomicUsize::new(0));
        let metadata_cancellation = Arc::new(Mutex::new(None));
        let driver = filesystem_driver_with_metadata_faults(
            Arc::clone(&metadata_failures),
            Arc::clone(&metadata_cancellation),
            "data/old.parquet",
        );
        let mut registry = SourceRegistry::new();
        registry.register(driver.clone()).unwrap();
        let context = SourceResolutionContext::new(
            root.path(),
            Arc::new(NoopSecretProvider),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );

        let mut one_job_request = compile_request(root.path());
        one_job_request
            .source_options
            .insert("maximum_concurrency".to_owned(), serde_json::json!(1));
        let mut one_job_plan = registry.compile(one_job_request).unwrap();
        let session = registry.discovery_session(&one_job_plan, &context).unwrap();
        let candidate = session.candidates().unwrap().remove(0);
        let observation = session
            .observe(
                &candidate,
                &SourceDiscoveryRequest::new(64 * 1024 * 1024, 1).unwrap(),
            )
            .unwrap();
        assert_eq!(observation.schema.fields().len(), 2);
        let effective_schema_hash =
            cdf_kernel::canonical_arrow_schema_hash(observation.schema.as_ref()).unwrap();
        let discovery_manifest = DiscoveryManifestReference {
            manifest_hash: DiscoveryManifestHash::new(
                "sha256:iceberg-external-task-lifecycle-manifest",
            )
            .unwrap(),
            path: ".cdf/discovery/iceberg-external-task-lifecycle.json".to_owned(),
        };
        let mut snapshot_metadata = BTreeMap::new();
        snapshot_metadata.insert(
            cdf_kernel::DISCOVERY_MANIFEST_HASH_METADATA_KEY.to_owned(),
            discovery_manifest.manifest_hash.to_string(),
        );
        snapshot_metadata.insert(
            cdf_kernel::DISCOVERY_MANIFEST_PATH_METADATA_KEY.to_owned(),
            discovery_manifest.path.clone(),
        );
        let mut pinned_descriptor = one_job_plan.descriptor.clone();
        pinned_descriptor.schema_source = SchemaSource::Discovered {
            snapshot: SchemaSnapshotReference {
                schema_hash: effective_schema_hash.clone(),
                path: ".cdf/schemas/lake.events@test.json".to_owned(),
                metadata: snapshot_metadata,
            },
        };
        let effective_schema_runtime = EffectiveSchemaRuntime::new(
            EffectiveSchemaEvidence::new(
                pinned_descriptor
                    .schema_source
                    .baseline_reference()
                    .unwrap(),
                effective_schema_hash,
                discovery_manifest,
                Vec::new(),
            )
            .unwrap(),
            Vec::new(),
        )
        .unwrap();
        one_job_plan = one_job_plan
            .bind_schema_authority(
                &pinned_descriptor,
                observation.schema.as_ref(),
                Some(effective_schema_runtime),
                Vec::new(),
            )
            .unwrap();
        let resource = registry.resolve(&one_job_plan, &context).unwrap();
        let request = ScanRequest {
            resource_id: one_job_plan.descriptor.resource_id.clone(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        };
        let one_job_scan = resource.negotiate(&request).unwrap();
        assert_eq!(one_job_scan.estimated_rows, Some(8));
        let one_job_source_bytes = one_job_scan.planned_source_bytes.unwrap().get();
        assert!(one_job_source_bytes > 0);
        let reference = one_job_scan.external_task_set().cloned().unwrap();
        assert_eq!(reference.task_count, 2);

        let store = ExternalTaskStore::new(
            root.path().join(".cdf"),
            ContentStoreNamespace::new(PLANNING_ARTIFACT_NAMESPACE).unwrap(),
        )
        .unwrap();
        let mut reader = store
            .reader(
                reference.clone(),
                crate::ICEBERG_TASK_SET_TYPE,
                crate::DEFAULT_MAXIMUM_TASK_BYTES,
                crate::DEFAULT_MAXIMUM_TASK_AUTHORITY_BYTES,
                execution.memory(),
            )
            .unwrap();
        let authority: crate::IcebergTaskSetAuthority =
            serde_json::from_slice(reader.authority().payload()).unwrap();
        let unchanged_scan = resource
            .rebind_scan_for_resume(
                one_job_scan.clone(),
                &SourcePosition::TableSnapshot(Box::new(
                    authority.snapshot.clone().expect("selected snapshot"),
                )),
            )
            .unwrap();
        assert_eq!(unchanged_scan.partition_count().unwrap(), 0);
        assert_eq!(unchanged_scan.external_task_set().unwrap().task_count, 0);
        assert_eq!(authority.output_schema_id, 1);
        assert_eq!(authority.projected_field_ids, vec![1, 2]);
        assert_eq!(
            authority.schemas.keys().copied().collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(
            authority
                .partition_specs
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [0, 1]
        );
        assert_eq!(authority.default_sort_order_id, 0);
        assert!(
            authority
                .reader
                .required_capabilities
                .contains("position-delete")
        );
        assert!(
            authority
                .reader
                .required_capabilities
                .contains("equality-delete")
        );
        assert_eq!(
            authority.sort_orders.keys().copied().collect::<Vec<_>>(),
            [0]
        );
        let authority = authority.into_validated().unwrap();
        let mut tasks = Vec::new();
        while let Some(record) = reader.next_record().unwrap() {
            let task: crate::IcebergScanTask =
                serde_json::from_slice(record.payload.payload()).unwrap();
            task.validate_against(&authority).unwrap();
            tasks.push(task);
        }
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].canonical_ordinal, 0);
        assert_eq!(tasks[0].file_schema_id, 0);
        assert!(tasks[0].data_file.path.ends_with("data/old.parquet"));
        assert_eq!(tasks[0].deletes.len(), 2);
        assert!(tasks[0].deletes.iter().any(|delete| {
            delete.content == crate::IcebergDeleteContent::Position
                && delete.referenced_data_file.as_deref() == Some(tasks[0].data_file.path.as_str())
        }));
        assert_eq!(tasks[1].canonical_ordinal, 1);
        assert_eq!(tasks[1].file_schema_id, 1);
        assert_eq!(tasks[1].partition_spec_id, 1);
        assert_eq!(tasks[1].partition_values, [Some(serde_json::json!(0))]);
        assert!(tasks[1].data_file.path.ends_with("data/current.parquet"));
        assert_eq!(tasks[1].deletes.len(), 2);
        assert!(
            tasks[1]
                .deletes
                .iter()
                .all(|delete| delete.content == crate::IcebergDeleteContent::Equality)
        );
        assert_eq!(
            one_job_source_bytes,
            tasks
                .iter()
                .map(|task| task.data_file.file_size_bytes)
                .sum::<u64>()
        );

        let mut planned = resource.planned_partition_reader(&reference).unwrap();
        let compiled_execution = CompiledSourceExecutionPlan::compile(&one_job_plan).unwrap();
        let schedule =
            CanonicalPartitionSchedule::compile(&compiled_execution, &one_job_scan).unwrap();
        let mut observation_ids = std::collections::BTreeSet::new();
        for ordinal in 0..2 {
            let executable = planned.next_partition(ordinal).unwrap().unwrap();
            assert_eq!(
                executable.plan().partition_id.as_str(),
                format!("iceberg-task-{ordinal:020}")
            );
            assert!(matches!(
                executable.plan().planned_position,
                Some(cdf_kernel::SourcePosition::TableSnapshot(_))
            ));
            let observation_id = executable
                .plan()
                .metadata
                .get(cdf_kernel::PLAN_SCHEMA_OBSERVATION_ID_KEY)
                .unwrap();
            assert_eq!(observation_id, executable.plan().partition_id.as_str());
            assert!(observation_ids.insert(observation_id.clone()));
            let observation_binding = executable
                .plan()
                .metadata
                .get(cdf_kernel::PLAN_SCHEMA_OBSERVATION_BINDING_KEY)
                .unwrap();
            assert_eq!(
                cdf_kernel::SchemaObservationBinding::new(observation_binding.clone()).unwrap(),
                crate::task_reader::derived_partition_observation_binding(executable.plan())
                    .unwrap()
            );
            let mut tampered = executable.plan().clone();
            tampered.metadata.insert(
                cdf_kernel::PLAN_SCHEMA_OBSERVATION_BINDING_KEY.to_owned(),
                format!("sha256:{}", "0".repeat(64)),
            );
            assert!(
                crate::task_reader::validate_partition_observation_authority(&tampered).is_err()
            );
            let task = executable
                .retention()
                .unwrap()
                .downcast_ref::<IcebergExecutableTask>()
                .unwrap();
            assert_eq!(task.task.canonical_ordinal, ordinal);
            task.task.validate_against(task.authority()).unwrap();
            assert_eq!(
                executable
                    .plan()
                    .metadata
                    .get("cdf:external_task_set_authority_sha256")
                    .map(String::as_str),
                Some(task.authority().content_sha256())
            );
            let mut changed_authority = executable.plan().clone();
            changed_authority.metadata.insert(
                "cdf:external_task_set_authority_sha256".to_owned(),
                format!("sha256:{}", "1".repeat(64)),
            );
            assert_ne!(
                crate::task_reader::derived_partition_observation_binding(&changed_authority)
                    .unwrap()
                    .as_str(),
                observation_binding
            );
            assert!(
                crate::task_reader::validate_partition_observation_authority(&changed_authority)
                    .is_err()
            );
            let scheduled = schedule
                .scheduled_partition(&compiled_execution, ordinal, executable.plan())
                .unwrap();
            assert_eq!(
                scheduled.retry.unwrap().granularity,
                SourceRetryGranularity::Partition
            );
        }
        assert!(planned.next_partition(2).unwrap().is_none());

        let mut executable_reader = resource.planned_partition_reader(&reference).unwrap();
        let executable = [
            executable_reader.next_partition(0).unwrap().unwrap(),
            executable_reader.next_partition(1).unwrap().unwrap(),
        ];
        let (rows, null_labels) = futures_executor::block_on(async {
            let mut rows = 0_usize;
            let mut null_labels = 0_usize;
            for task in executable {
                let attestation = resource
                    .attest_executable(task.clone())
                    .await?
                    .expect("Iceberg task has immutable snapshot authority");
                assert!(matches!(
                    attestation.processed_position(),
                    cdf_kernel::SourcePosition::TableSnapshot(_)
                ));
                assert_eq!(
                    attestation.physical_schema_hash(),
                    Some(&cdf_kernel::canonical_arrow_schema_hash(
                        resource.schema().as_ref()
                    )?)
                );
                let mut opened = resource.open_executable(task).await?;
                while let Some(batch) = opened.try_next().await? {
                    assert!(batch.retained_bytes() > 0);
                    assert!(matches!(
                        batch.header.source_position,
                        Some(cdf_kernel::SourcePosition::TableSnapshot(_))
                    ));
                    let record_batch = batch.record_batch().unwrap();
                    assert_eq!(record_batch.schema().fields().len(), 2);
                    rows += record_batch.num_rows();
                    null_labels += record_batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .null_count();
                }
                let completion = opened.completion().await?;
                assert!(matches!(
                    completion.attestation().unwrap().processed_position(),
                    cdf_kernel::SourcePosition::TableSnapshot(_)
                ));
            }
            Ok::<_, CdfError>((rows, null_labels))
        })
        .unwrap();
        assert_eq!(rows, 5);
        assert_eq!(null_labels, 3);

        let validation_program = compile_validation_program(
            &ContractPolicy::for_trust(TrustLevel::Governed),
            &ObservedSchema::from_arrow(resource.schema().as_ref()),
        )
        .unwrap();
        let engine_plan = Planner::new()
            .plan_tier_b(
                resource.as_ref(),
                EnginePlanInput {
                    request: request.clone(),
                    validation_program,
                    execution_extent: ExecutionExtent::bounded(),
                    segmentation: cdf_engine::CanonicalSegmentationPolicy::performance_default(),
                    package_id: "pkg-iceberg-external-preview-run".to_owned(),
                },
            )
            .unwrap()
            .bind_compiled_source(&one_job_plan)
            .unwrap();
        let preview = futures_executor::block_on(cdf_engine::preview_resource(
            &engine_plan,
            resource.as_ref(),
            EnginePreviewLimits::default(),
        ))
        .unwrap();
        assert_eq!(preview.planned_partition_count, 2);
        assert_eq!(preview.payload_eligible_partition_count, 2);
        assert_eq!(preview.payload_opened_partition_count, 2);
        assert_eq!(preview.row_count, 5);

        metadata_failures.store(1, Ordering::SeqCst);
        let options = EngineExecutionConfig::default()
            .with_execution_services(execution.clone())
            .new_invocation();
        let retry_evidence = options.source_retry_evidence();
        let package = tempfile::tempdir().unwrap();
        let run = futures_executor::block_on(
            cdf_engine::execute_to_package_with_segment_positions_and_pre_finalize(
                &engine_plan,
                resource.as_ref(),
                package.path(),
                &|_, _| Ok(()),
                options,
            ),
        )
        .unwrap();
        assert_eq!(run.output.profile.output_rows, 5);
        assert_eq!(
            run.output
                .lineage
                .input_observations
                .iter()
                .map(|observation| &observation.partition_id)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            2
        );
        assert!(run.execution_evidence().checkpoint_eligible());
        assert!(run.segment_positions.iter().all(|position| matches!(
            position.output_position,
            Some(cdf_kernel::SourcePosition::TableSnapshot(_))
        )));
        assert_eq!(metadata_failures.load(Ordering::SeqCst), 0);
        let retry_evidence = retry_evidence.snapshot().unwrap();
        assert_eq!(retry_evidence.len(), 1);
        assert_eq!(retry_evidence[0].partition_ordinal(), 0);
        assert_eq!(retry_evidence[0].history().len(), 1);
        assert_eq!(
            retry_evidence[0].history()[0].cause,
            cdf_kernel::ErrorKind::Transient
        );
        assert!(retry_evidence[0].history()[0].selected_delay_ms.is_some());
        assert!(retry_evidence[0].history()[0].exhaustion.is_none());

        let filtered_request = ScanRequest {
            resource_id: one_job_plan.descriptor.resource_id.clone(),
            projection: Some(vec!["label".to_owned()]),
            filters: vec![
                cdf_kernel::ScanPredicate::new(
                    PredicateId::new("iceberg-residual-id").unwrap(),
                    "id > 4",
                )
                .unwrap(),
            ],
            limit: None,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        };
        let filtered_validation = compile_validation_program(
            &ContractPolicy::for_trust(TrustLevel::Governed),
            &ObservedSchema::from_arrow(resource.schema().as_ref()),
        )
        .unwrap();
        let filtered_plan = Planner::new()
            .plan_tier_b(
                resource.as_ref(),
                EnginePlanInput {
                    request: filtered_request,
                    validation_program: filtered_validation,
                    execution_extent: ExecutionExtent::bounded(),
                    segmentation: cdf_engine::CanonicalSegmentationPolicy::performance_default(),
                    package_id: "pkg-iceberg-residual-projection".to_owned(),
                },
            )
            .unwrap()
            .bind_compiled_source(&one_job_plan)
            .unwrap();
        assert_eq!(filtered_plan.scan.pushed_predicates.len(), 0);
        assert_eq!(filtered_plan.scan.unsupported_predicates.len(), 1);
        let filtered_package = tempfile::tempdir().unwrap();
        let filtered = futures_executor::block_on(
            cdf_engine::execute_to_package_with_segment_positions_and_pre_finalize(
                &filtered_plan,
                resource.as_ref(),
                filtered_package.path(),
                &|_, _| Ok(()),
                EngineExecutionConfig::default()
                    .with_execution_services(execution.clone())
                    .new_invocation(),
            ),
        )
        .unwrap();
        assert_eq!(filtered.output.profile.output_rows, 2);
        let filtered_reader = cdf_package::PackageReader::open(filtered_package.path()).unwrap();
        let mut labels = Vec::new();
        let mut filtered_segments = Vec::new();
        filtered
            .output
            .for_each_identity_segment(&mut |segment| {
                filtered_segments.push(segment);
                Ok(())
            })
            .unwrap();
        let filtered_segment_ids = filtered_segments
            .into_iter()
            .map(|segment| segment.segment_id)
            .collect::<BTreeSet<_>>();
        for segment in filtered_reader
            .verified_canonical_segment_stream(execution.memory(), 64 * 1024 * 1024)
            .unwrap()
        {
            let segment = segment.unwrap();
            assert!(filtered_segment_ids.contains(&segment.entry.segment_id));
            for batch in segment.batches {
                assert_eq!(
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|field| field.name().as_str())
                        .collect::<Vec<_>>(),
                    ["label", "_cdf_variant", "_cdf_package_row_ord"]
                );
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                labels.extend(
                    (0..values.len())
                        .map(|row| (!values.is_null(row)).then(|| values.value(row).to_owned())),
                );
            }
        }
        assert_eq!(labels, [None, Some("eight".to_owned())]);

        let cancellation = RunCancellation::default();
        *metadata_cancellation.lock().unwrap() = Some(cancellation.clone());
        let cancelled_package = tempfile::tempdir().unwrap();
        let cancelled_options = EngineExecutionConfig::default()
            .with_execution_services(execution.clone())
            .new_invocation()
            .with_cancellation(cancellation.clone());
        let cancellation_error = futures_executor::block_on(
            cdf_engine::execute_to_package_with_segment_positions_and_pre_finalize(
                &engine_plan,
                resource.as_ref(),
                cancelled_package.path(),
                &|_, _| Ok(()),
                cancelled_options,
            ),
        )
        .unwrap_err();
        assert!(cancellation.is_cancelled());
        assert!(metadata_cancellation.lock().unwrap().is_none());
        assert!(
            cancellation_error.message.contains("cancel"),
            "{cancellation_error}"
        );
        let cancelled_manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(cancelled_package.path().join("manifest.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(cancelled_manifest["lifecycle"]["status"], "extracting");

        let mut many_jobs_request = compile_request(root.path());
        many_jobs_request
            .source_options
            .insert("maximum_concurrency".to_owned(), serde_json::json!(16));
        let mut many_jobs_plan = driver.compile(many_jobs_request).unwrap();
        many_jobs_plan.schema = one_job_plan.schema.clone();
        let many_jobs_resource = driver.resolve(&many_jobs_plan, &context).unwrap();
        let many_jobs_scan = many_jobs_resource.negotiate(&request).unwrap();
        assert_eq!(
            many_jobs_scan.external_task_set().unwrap().content_sha256,
            reference.content_sha256
        );
        assert_eq!(
            artifact_hash(&many_jobs_scan).unwrap(),
            artifact_hash(&one_job_scan).unwrap()
        );

        let generation_context = driver.catalog_context(&one_job_plan, &context).unwrap();
        let generation_source = driver.physical_plan(&one_job_plan).unwrap().source;
        let mut generation_reader = resource.planned_partition_reader(&reference).unwrap();
        let generation_partition = generation_reader.next_partition(0).unwrap().unwrap();
        let generation_task = generation_partition
            .retention()
            .and_then(PayloadRetention::downcast_ref::<IcebergExecutableTask>)
            .unwrap()
            .clone();
        drop(
            prepare_task_scan(
                &generation_context,
                &generation_source,
                generation_task.clone(),
                RunCancellation::default(),
            )
            .unwrap(),
        );
        let old_data_path = table.join("data/old.parquet");
        let mut replacement = fs::read(&old_data_path).unwrap();
        let replacement_size = replacement.len();
        replacement[0] ^= 1;
        fs::write(&old_data_path, replacement).unwrap();
        assert_eq!(
            usize::try_from(fs::metadata(&old_data_path).unwrap().len()).unwrap(),
            replacement_size
        );
        let generation_error = match prepare_task_scan(
            &generation_context,
            &generation_source,
            generation_task,
            RunCancellation::default(),
        ) {
            Ok(_) => panic!("same-sized Iceberg replacement escaped attempt attestation"),
            Err(error) => error,
        };
        assert_eq!(generation_error.kind, cdf_kernel::ErrorKind::Data);
        assert!(
            generation_error
                .message
                .contains("generation changed between attempts"),
            "{generation_error}"
        );
    }

    #[test]
    fn append_snapshot_resume_selects_only_new_files_and_rejects_nonappend_history() {
        let root = tempfile::tempdir().unwrap();
        let (execution, resource, scan, committed) = planned_append_resource(root.path(), "append");
        assert_eq!(scan.external_task_set().unwrap().task_count, 2);
        let full_scan = scan.clone();
        let scan = resource
            .rebind_scan_for_resume(
                scan,
                &SourcePosition::TableSnapshot(Box::new(committed.clone())),
            )
            .unwrap();
        assert_eq!(scan.estimated_rows, Some(5));
        assert_eq!(scan.planned_source_bytes.unwrap().get(), 1);
        let reference = scan.external_task_set().cloned().unwrap();
        assert_eq!(reference.task_count, 1);
        let store = ExternalTaskStore::new(
            root.path().join(".cdf"),
            ContentStoreNamespace::new(PLANNING_ARTIFACT_NAMESPACE).unwrap(),
        )
        .unwrap();
        let mut reader = store
            .reader(
                reference,
                crate::ICEBERG_TASK_SET_TYPE,
                crate::DEFAULT_MAXIMUM_TASK_BYTES,
                crate::DEFAULT_MAXIMUM_TASK_AUTHORITY_BYTES,
                execution.memory(),
            )
            .unwrap();
        let task: IcebergScanTask =
            serde_json::from_slice(reader.next_record().unwrap().unwrap().payload.payload())
                .unwrap();
        assert_eq!(task.canonical_ordinal, 0);
        assert_eq!(task.data_file.added_snapshot_id, 101);
        assert!(task.data_file.path.ends_with("appended.parquet"));
        assert!(reader.next_record().unwrap().is_none());

        let driver = filesystem_driver();
        let mut historical_request = compile_request(root.path());
        historical_request.resource_options.insert(
            "selector".to_owned(),
            serde_json::json!({"kind": "snapshot", "snapshot_id": 100}),
        );
        let mut historical_plan = driver.compile(historical_request).unwrap();
        let historical_context = SourceResolutionContext::new(
            root.path(),
            Arc::new(NoopSecretProvider),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let session = driver
            .discovery_session(&historical_plan, &historical_context)
            .unwrap();
        let candidate = session.candidates().unwrap().remove(0);
        historical_plan.schema = session
            .observe(
                &candidate,
                &SourceDiscoveryRequest::new(64 * 1024 * 1024, 1).unwrap(),
            )
            .unwrap()
            .schema
            .as_ref()
            .clone();
        let historical = driver
            .resolve(&historical_plan, &historical_context)
            .unwrap();
        let historical_scan = historical
            .negotiate(&ScanRequest {
                resource_id: historical_plan.descriptor.resource_id.clone(),
                projection: None,
                filters: Vec::new(),
                limit: None,
                order_by: Vec::new(),
                scope: ScopeKey::Resource,
            })
            .unwrap();
        assert_eq!(historical_scan.external_task_set().unwrap().task_count, 1);
        let historical_store = ExternalTaskStore::new(
            root.path().join(".cdf"),
            ContentStoreNamespace::new(PLANNING_ARTIFACT_NAMESPACE).unwrap(),
        )
        .unwrap();
        let historical_reader = historical_store
            .reader(
                historical_scan.external_task_set().cloned().unwrap(),
                crate::ICEBERG_TASK_SET_TYPE,
                crate::DEFAULT_MAXIMUM_TASK_BYTES,
                crate::DEFAULT_MAXIMUM_TASK_AUTHORITY_BYTES,
                execution.memory(),
            )
            .unwrap();
        let historical_authority: IcebergTaskSetAuthority =
            serde_json::from_slice(historical_reader.authority().payload()).unwrap();
        assert_eq!(historical_authority.snapshot.unwrap().snapshot_id, 100);

        let mut divergent_position = committed.clone();
        divergent_position.snapshot_id = 99;
        let divergent = resource
            .rebind_scan_for_resume(
                full_scan.clone(),
                &SourcePosition::TableSnapshot(Box::new(divergent_position)),
            )
            .unwrap_err();
        assert!(divergent.message.contains("is not an ancestor"));
        let mut missing_position = committed;
        missing_position.snapshot_id = 98;
        let missing = resource
            .rebind_scan_for_resume(
                full_scan,
                &SourcePosition::TableSnapshot(Box::new(missing_position)),
            )
            .unwrap_err();
        assert!(missing.message.contains("history no longer contains"));

        let rejected = tempfile::tempdir().unwrap();
        let (_execution, resource, scan, committed) =
            planned_append_resource(rejected.path(), "overwrite");
        let error = resource
            .rebind_scan_for_resume(scan, &SourcePosition::TableSnapshot(Box::new(committed)))
            .unwrap_err();
        assert!(error.message.contains("snapshot 101 operation `overwrite`"));
        assert!(error.message.contains("mode = `snapshot`"));
        assert!(error.message.contains("changelog-capable disposition"));
    }

    #[test]
    fn v1_parquet_snapshot_discovers_plans_and_executes() {
        let root = tempfile::tempdir().unwrap();
        let table = root.path().join("analytics/events");
        let execution = execution_services();
        write_v1_table_fixture(&execution, &table);
        let driver = filesystem_driver();
        let mut plan = driver.compile(compile_request(root.path())).unwrap();
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
        assert_eq!(observation.schema.fields().len(), 1);
        plan.schema = observation.schema.as_ref().clone();

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
        let reference = scan.external_task_set().unwrap();
        assert_eq!(reference.task_count, 1);
        let store = ExternalTaskStore::new(
            root.path().join(".cdf"),
            ContentStoreNamespace::new(PLANNING_ARTIFACT_NAMESPACE).unwrap(),
        )
        .unwrap();
        let reader = store
            .reader(
                reference.clone(),
                crate::ICEBERG_TASK_SET_TYPE,
                crate::DEFAULT_MAXIMUM_TASK_BYTES,
                crate::DEFAULT_MAXIMUM_TASK_AUTHORITY_BYTES,
                execution.memory(),
            )
            .unwrap();
        let authority: crate::IcebergTaskSetAuthority =
            serde_json::from_slice(reader.authority().payload()).unwrap();
        assert_eq!(authority.table_format_version, 1);

        let mut planned = resource.planned_partition_reader(reference).unwrap();
        let executable = planned.next_partition(0).unwrap().unwrap();
        let rows = futures_executor::block_on(async {
            let mut stream = resource.open_executable(executable).await?;
            let mut rows = 0_usize;
            while let Some(batch) = stream.try_next().await? {
                rows += batch
                    .record_batch()
                    .ok_or_else(|| CdfError::internal("Iceberg v1 test expected Arrow data"))?
                    .num_rows();
            }
            stream.completion().await?;
            Ok::<_, CdfError>(rows)
        })
        .unwrap();
        assert_eq!(rows, 3);
        assert!(planned.next_partition(1).unwrap().is_none());
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
        let driver = rest_driver(transport);
        let plan = driver.compile(rest_compile_request(root.path())).unwrap();
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
        assert_eq!(scan.external_task_set().unwrap().task_count, 0);
        assert_eq!(observed_requests.lock().unwrap().len(), 2);
        assert!(observed_requests.lock().unwrap()[0].contains("warehouse=primary"));
        assert!(
            observed_requests.lock().unwrap()[1]
                .contains("/v1/prod/namespaces/analytics/tables/events")
        );
    }

    #[test]
    fn rest_catalog_nonempty_snapshot_executes_canonical_local_objects() {
        let root = tempfile::tempdir().unwrap();
        let table = root.path().join("analytics/events");
        let execution = execution_services();
        write_nonempty_table_fixture(&execution, &table);
        let metadata_location = table.join("metadata/v1.metadata.json");
        let metadata: serde_json::Value =
            serde_json::from_slice(&fs::read(&metadata_location).unwrap()).unwrap();
        let transport = SequenceHttpTransport::new([
            serde_json::to_vec(&serde_json::json!({
                "defaults": {"prefix": "prod"},
                "overrides": {}
            }))
            .unwrap(),
            serde_json::to_vec(&serde_json::json!({
                "metadata-location": metadata_location.display().to_string(),
                "metadata": metadata
            }))
            .unwrap(),
        ]);
        let observed_requests = Arc::clone(&transport.requests);
        let driver = rest_driver(transport);
        let mut plan = driver.compile(rest_compile_request(root.path())).unwrap();
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
        plan.schema = observation.schema.as_ref().clone();

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
        let reference = scan.external_task_set().unwrap();
        assert_eq!(reference.task_count, 2);
        let mut planned = resource.planned_partition_reader(reference).unwrap();
        let rows = futures_executor::block_on(async {
            let mut rows = 0_usize;
            for ordinal in 0..reference.task_count {
                let executable = planned.next_partition(ordinal).unwrap().unwrap();
                let mut stream = resource.open_executable(executable).await?;
                while let Some(batch) = stream.try_next().await? {
                    rows += batch
                        .record_batch()
                        .ok_or_else(|| CdfError::internal("Iceberg REST test expected Arrow data"))?
                        .num_rows();
                }
                stream.completion().await?;
            }
            Ok::<_, CdfError>(rows)
        })
        .unwrap();

        assert_eq!(rows, 5);
        assert_eq!(observed_requests.lock().unwrap().len(), 2);
        assert!(
            planned
                .next_partition(reference.task_count)
                .unwrap()
                .is_none()
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
