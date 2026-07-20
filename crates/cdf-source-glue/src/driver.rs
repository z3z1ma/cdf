use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use arrow_schema::{Schema, SchemaRef};
use cdf_http::{SecretProvider, SecretUri};
use cdf_kernel::{
    BackpressureSupport, BatchStream, CapabilitySupport, CdfError, EffectiveSchemaCatalogEntry,
    EffectiveSchemaRuntime, EstimateSupport, ExecutablePartition, FilterCapabilities,
    IncrementalShape, PartitionAttestation, PartitionAttestationAttempt, PartitionOpenAttempt,
    PartitionPlan, PartitionStreamPayload, PartitioningCapabilities, PlannedPartitionReader,
    PlannedTaskSetReference, PushdownFidelity, QueryableResource, ReplaySupport,
    ResourceCapabilities, ResourceDescriptor, ResourceStream, Result, ScanPlan, ScanRequest,
    ScopeKind, SourcePosition, TypePolicyAllowances,
};
use cdf_object_access::{FileTransport, FileTransportControl, FileTransportResource};
use cdf_runtime::{
    ByteTransformRegistry, CompiledSourcePlan, ExecutionServices, FormatRegistry,
    PreparedSourcePayload, PreparedSourcePayloadKey, SourceAddPlanner, SourceAddProposal,
    SourceAddRequest, SourceAttestationStrength, SourceBatchMemoryContract, SourceCompileRequest,
    SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest, SourceDiscoverySession,
    SourceDriver, SourceDriverDescriptor, SourceEvidenceLocation, SourceExecutionCapabilities,
    SourceExecutorClass, SourceHealthRequest, SourceHealthResult, SourceHealthSink,
    SourceHealthStatus, SourceResolutionContext, SourceRetryGranularity, artifact_hash,
};
use cdf_task_store::ExternalTaskStore;
use futures_channel::oneshot;
use serde::{Deserialize, Serialize};

use crate::{
    GlueCatalogClient, GlueGetTableRequest, GlueGetUnfilteredTableRequest, GlueResourceOptions,
    GlueSourceOptions, GlueTable, GlueTableClass, LakeFormationClient, classify_table,
    execution::{execute_object, prepare_object},
    glue_arrow_schema, glue_option_schema, glue_source_descriptor,
    lake_formation::LakeFormationRuntime,
    planner::{GluePlanningContext, plan_glue_scan},
    task_reader::{GlueExecutableTask, GluePlannedPartitionReader},
};

const PLANNING_ARTIFACT_NAMESPACE: &str = "planner-artifacts";
static GLUE_QUERY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

type RuntimeFactory = dyn Fn(
        Arc<dyn SecretProvider + Send + Sync>,
        ExecutionServices,
        cdf_runtime::SourceEgressScope,
    ) -> Result<GlueRuntimeDependencies>
    + Send
    + Sync
    + 'static;

#[derive(Clone)]
pub struct GlueRuntimeDependencies {
    pub object_access: Arc<dyn FileTransport>,
    pub catalog: Arc<dyn GlueCatalogClient>,
    pub lake_formation: Arc<dyn LakeFormationClient>,
    pub formats: Arc<FormatRegistry>,
    pub transforms: Arc<ByteTransformRegistry>,
}

impl std::fmt::Debug for GlueRuntimeDependencies {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GlueRuntimeDependencies")
            .finish_non_exhaustive()
    }
}

impl GlueRuntimeDependencies {
    pub fn new(
        object_access: Arc<dyn FileTransport>,
        catalog: Arc<dyn GlueCatalogClient>,
        lake_formation: Arc<dyn LakeFormationClient>,
        formats: Arc<FormatRegistry>,
        transforms: Arc<ByteTransformRegistry>,
    ) -> Self {
        Self {
            object_access,
            catalog,
            lake_formation,
            formats,
            transforms,
        }
    }
}

#[derive(Clone)]
pub struct GlueSourceDriver {
    descriptor: SourceDriverDescriptor,
    option_schema: serde_json::Value,
    runtime_factory: Arc<RuntimeFactory>,
}

impl std::fmt::Debug for GlueSourceDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GlueSourceDriver")
            .field("descriptor", &self.descriptor)
            .finish_non_exhaustive()
    }
}

impl GlueSourceDriver {
    pub fn new<F>(runtime_factory: F) -> Result<Self>
    where
        F: Fn(
                Arc<dyn SecretProvider + Send + Sync>,
                ExecutionServices,
                cdf_runtime::SourceEgressScope,
            ) -> Result<GlueRuntimeDependencies>
            + Send
            + Sync
            + 'static,
    {
        Ok(Self {
            descriptor: glue_source_descriptor()?,
            option_schema: glue_option_schema(),
            runtime_factory: Arc::new(runtime_factory),
        })
    }

    fn physical_plan(&self, plan: &CompiledSourcePlan) -> Result<GluePhysicalPlan> {
        let physical: GluePhysicalPlan = serde_json::from_value(plan.physical_plan.clone())
            .map_err(|error| CdfError::contract(format!("invalid Glue source plan: {error}")))?;
        physical.validate()?;
        Ok(physical)
    }

    fn dependencies(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<GlueRuntimeDependencies> {
        (self.runtime_factory)(
            Arc::clone(context.secret_provider()),
            context.execution().clone(),
            context.egress_scope(&plan.driver.driver_id),
        )
    }

    fn load_table(
        &self,
        plan: &CompiledSourcePlan,
        physical: &GluePhysicalPlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<(LoadedGlueTable, GlueRuntimeDependencies)> {
        let dependencies = self.dependencies(plan, context)?;
        let loaded = self.load_table_with_dependencies(plan, physical, context, &dependencies)?;
        Ok((loaded, dependencies))
    }

    fn load_table_with_dependencies(
        &self,
        plan: &CompiledSourcePlan,
        physical: &GluePhysicalPlan,
        context: &SourceResolutionContext<'_>,
        dependencies: &GlueRuntimeDependencies,
    ) -> Result<LoadedGlueTable> {
        let request = table_request(&physical.source, &physical.resource, context.cancellation())?;
        let catalog = Arc::clone(&dependencies.catalog);
        let mut response = context
            .execution()
            .run_io(async move { catalog.get_table(request).await })?;
        let lake_formation = if response.table.is_registered_with_lake_formation {
            if physical.source.object_credentials.is_some() {
                return Err(CdfError::auth(
                    "Lake Formation governed Glue tables cannot use object_credentials; CDF must vend scoped credentials from Lake Formation",
                ));
            }
            let catalog_id = response
                .table
                .catalog_id
                .clone()
                .or_else(|| physical.source.catalog_id.clone())
                .ok_or_else(|| {
                    CdfError::data(
                        "Lake Formation governed Glue table omitted its catalog/account id",
                    )
                })?;
            let requested_columns = requested_data_columns(&plan.schema, &response.table)?;
            let all_columns_requested = plan.schema.fields().is_empty();
            let query_start_unix_seconds = context.execution().unix_now().as_secs();
            let request = GlueGetUnfilteredTableRequest {
                region: physical.source.region.clone(),
                catalog_id,
                database: physical.resource.database.clone(),
                table: physical.resource.table.clone(),
                requested_columns,
                all_columns_requested,
                query_id: glue_query_id(plan, context.execution())?,
                query_start_unix_seconds,
                endpoint: physical.source.endpoint.clone(),
                credentials: physical
                    .source
                    .credentials
                    .as_ref()
                    .map(|value| SecretUri::new(value.clone()))
                    .transpose()?,
                maximum_response_bytes: physical.source.maximum_response_bytes,
                cancellation: context.cancellation(),
            };
            let catalog = Arc::clone(&dependencies.catalog);
            let governed = context
                .execution()
                .run_io(async move { catalog.get_unfiltered_table(request).await })?;
            let authorization = governed.lake_formation.clone().ok_or_else(|| {
                CdfError::internal(
                    "Glue governed metadata response omitted Lake Formation authorization",
                )
            })?;
            let table_s3_path = governed
                .table
                .storage_descriptor
                .as_ref()
                .and_then(|descriptor| descriptor.location.clone())
                .ok_or_else(|| {
                    CdfError::data("Lake Formation governed Glue table omitted S3 Location")
                })?;
            response.bytes_read = response.bytes_read.saturating_add(governed.bytes_read);
            response.table = governed.table;
            Some(LakeFormationRuntime::new(
                Arc::clone(&dependencies.lake_formation),
                &physical.source,
                authorization,
                table_s3_path,
                !response.table.partition_keys.is_empty(),
                context.execution().clone(),
                context.cancellation(),
            )?)
        } else {
            None
        };
        LoadedGlueTable::new(
            response.table,
            response.bytes_read,
            &physical.resource,
            context.execution().memory(),
            lake_formation,
        )
    }
}

impl SourceDriver for GlueSourceDriver {
    fn descriptor(&self) -> &SourceDriverDescriptor {
        &self.descriptor
    }

    fn option_schema(&self) -> &serde_json::Value {
        &self.option_schema
    }

    fn validate_portable_plan(&self, plan: &CompiledSourcePlan) -> Result<()> {
        plan.validate()?;
        self.physical_plan(plan).map(drop)
    }

    fn add_planner(&self) -> Option<&dyn SourceAddPlanner> {
        Some(self)
    }

    fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        let source: GlueSourceOptions = decode_options("Glue source", request.source_options)?;
        let resource: GlueResourceOptions =
            decode_options("Glue resource", request.resource_options)?;
        source.validate()?;
        resource.validate()?;
        let physical = GluePhysicalPlan { source, resource };
        let encoded = serde_json::to_value(&physical)
            .map_err(|error| CdfError::internal(format!("serialize Glue plan: {error}")))?;
        CompiledSourcePlan::new(
            self.descriptor.clone(),
            glue_resource_capabilities(),
            glue_execution_capabilities(&physical.source)?,
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
        let candidate = table.discovery_candidate(&physical.source, &physical.resource)?;
        let retained = table.retained_bytes()?;
        context.prepared_payloads().install(
            prepared_table_key(plan)?,
            PreparedSourcePayload::new(
                table.clone(),
                cdf_kernel::PayloadRetention::new(Arc::new(table.clone()), retained)?,
            ),
        )?;
        Ok(Box::new(GlueDiscoverySession { table, candidate }))
    }

    fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        let physical = self.physical_plan(plan)?;
        let dependencies = self.dependencies(plan, context)?;
        let (table, prepared_retention) = match context
            .prepared_payloads()
            .take(&prepared_table_key(plan)?)?
        {
            Some(payload) => {
                let (table, retention) =
                    payload.into_typed::<LoadedGlueTable>("Glue table metadata")?;
                (table, Some(retention))
            }
            None => (
                self.load_table_with_dependencies(plan, &physical, context, &dependencies)?,
                None,
            ),
        };
        let task_store = ExternalTaskStore::new(
            context.project_root().join(".cdf"),
            cdf_kernel::ContentStoreNamespace::new(PLANNING_ARTIFACT_NAMESPACE)?,
        )?;
        Ok(Arc::new(GlueResource {
            descriptor: plan.descriptor.clone(),
            schema: Arc::new(plan.schema.clone()),
            physical_schema: Arc::new(table.schema.clone()),
            data_schema: Arc::new(table.data_schema.clone()),
            partition_schema: Arc::new(table.partition_schema.clone()),
            capabilities: plan.resource_capabilities.clone(),
            type_policy_allowances: plan.type_policy_allowances,
            effective_schema_runtime: plan.effective_schema_runtime.clone(),
            baseline_observation_schema_catalog: plan.baseline_observation_schema_catalog.clone(),
            compiled_source_plan_hash: artifact_hash(plan)?,
            source: physical.source,
            resource: physical.resource,
            table,
            dependencies,
            execution: context.execution().clone(),
            egress: context.egress_scope(&plan.driver.driver_id),
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
                probe_id: "glue-inventory".to_owned(),
                status: SourceHealthStatus::Skipped,
                message: "no Glue external-table resources are compiled".to_owned(),
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
                        message: if table.lake_formation.is_some() {
                            "Glue/Lake Formation authorized metadata and format mapping are readable; credential vending and object access are verified by plan/run"
                        } else {
                            "Glue catalog table and format mapping are readable"
                        }
                        .to_owned(),
                        details: serde_json::json!({
                            "resource_id": plan.descriptor.resource_id.as_str(),
                            "database": physical.resource.database,
                            "table": physical.resource.table,
                            "format": table.format_id,
                            "partition_columns": table.partition_schema.fields().len(),
                            "lake_formation": table.lake_formation.is_some(),
                            "metadata_bytes": table.bytes_read,
                        }),
                    }
                }
                Err(error) => SourceHealthResult::failed(
                    plan.descriptor.resource_id.as_str(),
                    "Glue catalog/table metadata probe failed",
                    &plan.descriptor.resource_id,
                    &error,
                ),
            };
            output.emit(result)?;
        }
        Ok(())
    }
}

impl SourceAddPlanner for GlueSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        request.validate()?;
        let Some(rest) = request.location.strip_prefix("glue://") else {
            return Ok(None);
        };
        let uri = url::Url::parse(&request.location).map_err(|_| {
            CdfError::contract("Glue add URI must be glue://<region>/<database>/<table>")
        })?;
        let region = uri.host_str().ok_or_else(|| {
            CdfError::contract("Glue add URI must name its AWS region as the host")
        })?;
        let components = uri
            .path_segments()
            .map(|parts| parts.filter(|part| !part.is_empty()).collect::<Vec<_>>())
            .unwrap_or_default();
        let [database, table] = components.as_slice() else {
            return Err(CdfError::contract(
                "Glue add URI must be glue://<region>/<database>/<table>",
            ));
        };
        if rest.contains('@') || uri.password().is_some() {
            return Err(CdfError::contract(
                "Glue add URI cannot contain credentials",
            ));
        }
        let mut source_options = BTreeMap::from([("region".to_owned(), serde_json::json!(region))]);
        for key in [
            "catalog_id",
            "endpoint",
            "object_region",
            "lake_formation_endpoint",
            "credentials",
            "object_credentials",
        ] {
            if let Some(value) = request.options.get(key) {
                source_options.insert(key.to_owned(), serde_json::json!(value));
            }
        }
        if let Some(value) = request.options.get("egress_allowlist") {
            let hosts = if value.starts_with('[') {
                serde_json::from_str::<Vec<String>>(value).map_err(|error| {
                    CdfError::contract(format!("Glue egress allowlist is invalid JSON: {error}"))
                })?
            } else {
                vec![value.clone()]
            };
            source_options.insert("egress_allowlist".to_owned(), serde_json::json!(hosts));
        }
        let allowed = [
            "catalog_id",
            "endpoint",
            "object_region",
            "lake_formation_endpoint",
            "credentials",
            "object_credentials",
            "egress_allowlist",
            "partition_expression",
            "format",
        ];
        let unknown = request
            .options
            .keys()
            .filter(|key| !allowed.contains(&key.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Err(CdfError::contract(format!(
                "Glue cdf add received unsupported options: {}",
                unknown.join(", ")
            )));
        }
        let mut resource_options = BTreeMap::from([
            ("database".to_owned(), serde_json::json!(database)),
            ("table".to_owned(), serde_json::json!(table)),
        ]);
        for key in ["partition_expression", "format"] {
            if let Some(value) = request.options.get(key) {
                resource_options.insert(key.to_owned(), serde_json::json!(value));
            }
        }
        Ok(Some(SourceAddProposal {
            source_kind: "glue".to_owned(),
            source_options,
            resource_options,
            cursor: None,
            display_location: SourceEvidenceLocation::from_operational(&request.location)?,
            display_selection: format!("{database}.{table}"),
            private_files: Vec::new(),
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GluePhysicalPlan {
    source: GlueSourceOptions,
    resource: GlueResourceOptions,
}

impl GluePhysicalPlan {
    fn validate(&self) -> Result<()> {
        self.source.validate()?;
        self.resource.validate()
    }
}

#[derive(Clone, Debug)]
struct LoadedGlueTable {
    table: GlueTable,
    schema: Schema,
    data_schema: Schema,
    partition_schema: Schema,
    generation: String,
    format_id: String,
    bytes_read: u64,
    lake_formation: Option<LakeFormationRuntime>,
    _memory: cdf_memory::MemoryLease,
}

impl LoadedGlueTable {
    fn new(
        table: GlueTable,
        bytes_read: u64,
        resource: &GlueResourceOptions,
        memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        lake_formation: Option<LakeFormationRuntime>,
    ) -> Result<Self> {
        if table.is_registered_with_lake_formation != lake_formation.is_some() {
            return Err(CdfError::internal(
                "Glue table Lake Formation registration and runtime authority disagree",
            ));
        }
        let mapping = match classify_table(&table, resource.format.as_deref())? {
            GlueTableClass::Conventional(mapping) => mapping,
            class => return Err(classification_error(class)),
        };
        let descriptor = table
            .storage_descriptor
            .as_ref()
            .ok_or_else(|| CdfError::data("AWS Glue table omitted its StorageDescriptor"))?;
        let data_schema = glue_arrow_schema(&descriptor.columns, &[])?;
        let partition_schema = glue_arrow_schema(&[], &table.partition_keys)?;
        let schema = glue_arrow_schema(&descriptor.columns, &table.partition_keys)?;
        let generation = crate::model::table_generation(&table)?;
        let retained_model_bytes = u64::try_from(
            serde_json::to_vec(&table)
                .map_err(|error| {
                    CdfError::internal(format!("encode retained Glue table: {error}"))
                })?
                .len(),
        )
        .unwrap_or(u64::MAX)
        .checked_mul(4)
        .and_then(|value| value.checked_add(4096))
        .ok_or_else(|| CdfError::data("retained Glue table byte estimate overflowed"))?;
        let memory = cdf_memory::reserve_blocking(
            memory,
            &cdf_memory::ReservationRequest::new(
                cdf_memory::ConsumerKey::new(
                    "glue-table-metadata",
                    cdf_memory::MemoryClass::Control,
                )?,
                retained_model_bytes,
            )?,
        )?;
        Ok(Self {
            table,
            schema,
            data_schema,
            partition_schema,
            generation,
            format_id: mapping.format_id,
            bytes_read,
            lake_formation,
            _memory: memory,
        })
    }

    fn retained_bytes(&self) -> Result<u64> {
        Ok(self._memory.bytes())
    }

    fn discovery_candidate(
        &self,
        source: &GlueSourceOptions,
        resource: &GlueResourceOptions,
    ) -> Result<SourceDiscoveryCandidate> {
        let catalog = source.catalog_id.as_deref().unwrap_or("aws");
        SourceDiscoveryCandidate::new(
            format!(
                "glue://{}/{}/{}/{}",
                source.region, catalog, resource.database, resource.table
            ),
            Some(self.bytes_read),
            None,
            BTreeMap::from([
                ("catalog_generation".to_owned(), self.generation.clone()),
                ("format".to_owned(), self.format_id.clone()),
            ]),
        )
    }
}

struct GlueDiscoverySession {
    table: LoadedGlueTable,
    candidate: SourceDiscoveryCandidate,
}

impl SourceDiscoverySession for GlueDiscoverySession {
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
                "Glue discovery candidate does not match the loaded catalog table",
            ));
        }
        if self.table.bytes_read > request.maximum_bytes {
            return Err(CdfError::data(format!(
                "Glue schema metadata used {} bytes but discovery permits {}; increase the discovery byte budget",
                self.table.bytes_read, request.maximum_bytes
            )));
        }
        cdf_runtime::SourceSchemaObservation::new(
            candidate,
            self.table.schema.clone(),
            candidate.identity.clone(),
            self.table.bytes_read,
            0,
        )
    }
}

struct GlueResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    physical_schema: SchemaRef,
    data_schema: SchemaRef,
    partition_schema: SchemaRef,
    capabilities: ResourceCapabilities,
    type_policy_allowances: TypePolicyAllowances,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
    compiled_source_plan_hash: String,
    source: GlueSourceOptions,
    resource: GlueResourceOptions,
    table: LoadedGlueTable,
    dependencies: GlueRuntimeDependencies,
    execution: ExecutionServices,
    egress: cdf_runtime::SourceEgressScope,
    task_store: ExternalTaskStore,
    cancellation: cdf_runtime::RunCancellation,
    _prepared_retention: Option<cdf_kernel::PayloadRetention>,
}

impl ResourceStream for GlueResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn compiled_source_plan_hash(&self) -> Option<&str> {
        Some(&self.compiled_source_plan_hash)
    }

    fn validate_runtime_dependencies(&self) -> Result<()> {
        self.source.validate()?;
        self.resource.validate()?;
        self.table.schema.fields().iter().try_for_each(|field| {
            self.physical_schema
                .field_with_name(field.name())
                .map(|_| ())
                .map_err(|_| {
                    CdfError::internal("Glue retained schema authority changed during resolution")
                })
        })
    }

    fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        Err(CdfError::contract(
            "Glue uses external canonical object tasks and must be planned through negotiate",
        ))
    }

    fn planned_partition_reader(
        &self,
        reference: &PlannedTaskSetReference,
    ) -> Result<Box<dyn PlannedPartitionReader>> {
        Ok(Box::new(GluePlannedPartitionReader::open(
            &self.task_store,
            reference.clone(),
            &self.source,
            self.execution.memory(),
        )?))
    }

    fn rebind_scan_for_resume(
        &self,
        scan: &mut ScanPlan,
        committed_frontier: &SourcePosition,
    ) -> Result<()> {
        let SourcePosition::FileManifest(committed) = committed_frontier else {
            return Err(CdfError::data(format!(
                "Glue external table cannot resume from a {} position",
                committed_frontier.kind().as_str()
            )));
        };
        let Some(reference) = scan.planned_task_set.clone() else {
            return Ok(());
        };
        let committed = committed
            .files
            .iter()
            .map(|file| (file.path.as_str(), file))
            .collect::<BTreeMap<_, _>>();
        let mut reader = self.task_store.reader(
            reference,
            crate::GLUE_TASK_SET_TYPE,
            self.source.maximum_task_bytes,
            self.source.maximum_task_authority_bytes,
            self.execution.memory(),
        )?;
        let authority: crate::GlueTaskAuthority =
            serde_json::from_slice(reader.authority().payload()).map_err(|error| {
                CdfError::data(format!("decode Glue resume authority: {error}"))
            })?;
        authority.validate()?;
        let mut writer = self.task_store.writer(
            crate::GLUE_TASK_SET_TYPE,
            cdf_task_store::TaskSetLimits {
                maximum_task_bytes: self.source.maximum_task_bytes,
                maximum_authority_bytes: self.source.maximum_task_authority_bytes,
                writer_buffer_bytes: self.source.task_writer_buffer_bytes,
            },
            self.execution.memory(),
            self.execution.spill().as_ref(),
        )?;
        let mut ordinal = 0_u64;
        let mut estimated_bytes = 0_u64;
        while let Some(record) = reader.next_record()? {
            let mut task: crate::GlueObjectTask = serde_json::from_slice(record.payload.payload())
                .map_err(|error| CdfError::data(format!("decode Glue resume task: {error}")))?;
            task.validate_against(&authority)?;
            if committed
                .get(task.file.path.as_str())
                .is_some_and(|file| *file == &task.file)
            {
                continue;
            }
            task.canonical_ordinal = ordinal;
            writer.push_with(ordinal, |output| task.encode_to(output))?;
            estimated_bytes = estimated_bytes.saturating_add(task.file.size_bytes);
            ordinal = ordinal
                .checked_add(1)
                .ok_or_else(|| CdfError::data("Glue resume task ordinal overflowed"))?;
        }
        if ordinal == 0 {
            scan.planned_task_set = None;
            scan.partitions.clear();
            scan.estimated_bytes = Some(0);
            return Ok(());
        }
        let artifact = writer.finalize(|output| authority.encode_to(output))?;
        scan.planned_task_set = Some(artifact.reference);
        scan.estimated_bytes = Some(estimated_bytes);
        Ok(())
    }

    fn open(&self, _partition: PartitionPlan) -> PartitionOpenAttempt<'_> {
        PartitionOpenAttempt::materialized(Box::pin(async {
            Err(CdfError::contract(
                "Glue executes retained external object tasks; open an executable partition",
            ))
        }))
    }

    fn open_executable(&self, partition: ExecutablePartition) -> PartitionOpenAttempt<'_> {
        let retained = partition
            .retention()
            .and_then(cdf_kernel::PayloadRetention::downcast_ref::<GlueExecutableTask>)
            .cloned();
        let Some(executable) = retained else {
            return PartitionOpenAttempt::materialized(Box::pin(async {
                Err(CdfError::contract(
                    "Glue executable partition omitted its retained canonical task",
                ))
            }));
        };
        let plan = partition.into_plan();
        let prepared = prepare_object(
            &executable.task,
            executable.authority(),
            &self.source,
            self.table.lake_formation.as_ref(),
            &self.data_schema,
            &self.partition_schema,
            &self.physical_schema,
            &self.dependencies.object_access,
            &self.dependencies.formats,
            &self.dependencies.transforms,
            &self.egress,
            self.execution.memory(),
            self.cancellation.clone(),
        );
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                return PartitionOpenAttempt::materialized(Box::pin(async move { Err(error) }));
            }
        };
        let execution = self.execution.clone();
        let source_options = self.source.clone();
        let resource_id = self.descriptor.resource_id.clone();
        let partition_id = plan.partition_id.clone();
        let memory = self.execution.memory();
        let maximum_items = usize::from(self.source.stream_buffer_batches);
        let (completion_sender, completion_receiver) = oneshot::channel();
        let decode_cpu = prepared.format.descriptor().decode_cpu.clone();
        let stream = match execution.spawn_cpu_stream(
            &format!("glue-open-{}", plan.partition_id.as_str()),
            decode_cpu,
            maximum_items,
            move |sender, cancellation| async move {
                let completion = execute_object(
                    prepared,
                    source_options,
                    resource_id,
                    partition_id,
                    memory,
                    cancellation,
                    sender,
                )
                .await?;
                let _ = completion_sender.send(completion);
                Ok(())
            },
        ) {
            Ok(stream) => stream,
            Err(error) => {
                return PartitionOpenAttempt::materialized(Box::pin(async move { Err(error) }));
            }
        };
        let termination = stream.termination();
        let opening = Box::pin(async move {
            let completion = Box::pin(async move {
                completion_receiver.await.map_err(|_| {
                    CdfError::internal(
                        "Glue source reached EOF without publishing completion evidence",
                    )
                })
            });
            Ok(PartitionStreamPayload::new(
                Box::pin(stream) as BatchStream,
                completion,
            ))
        });
        PartitionOpenAttempt::with_termination(opening, termination)
    }

    fn attest_executable(&self, partition: ExecutablePartition) -> PartitionAttestationAttempt<'_> {
        let retained = partition
            .retention()
            .and_then(cdf_kernel::PayloadRetention::downcast_ref::<GlueExecutableTask>)
            .cloned();
        let object_access = Arc::clone(&self.dependencies.object_access);
        let egress = self.egress.clone();
        let source = self.source.clone();
        let lake_formation = self.table.lake_formation.clone();
        let cancellation = self.cancellation.clone();
        let physical_hash = cdf_kernel::canonical_arrow_schema_hash(self.physical_schema.as_ref());
        PartitionAttestationAttempt::materialized(Box::pin(async move {
            let executable = retained.ok_or_else(|| {
                CdfError::contract("Glue attestation omitted its retained object task")
            })?;
            let logical = object_resource(
                &executable.task.file.path,
                &source,
                lake_formation.as_ref(),
                &executable.task.partition_values,
            )?;
            let observed = object_access.metadata(
                &egress,
                &logical,
                &FileTransportControl::new(cancellation, None),
            )?;
            if observed.identity().file_position_evidence()? != executable.task.file {
                return Err(CdfError::data(
                    "Glue object generation changed before retry/commit attestation",
                ));
            }
            Ok(Some(PartitionAttestation::new(
                SourcePosition::FileManifest(cdf_kernel::FileManifest {
                    version: cdf_kernel::SOURCE_POSITION_VERSION,
                    files: vec![executable.task.file],
                }),
                Some(physical_hash?),
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

impl QueryableResource for GlueResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        plan_glue_scan(
            &self.descriptor,
            &self.source,
            &self.resource,
            &self.table.table,
            &self.table.generation,
            request,
            GluePlanningContext {
                catalog: Arc::clone(&self.dependencies.catalog),
                object_access: Arc::clone(&self.dependencies.object_access),
                execution: self.execution.clone(),
                egress: self.egress.clone(),
                task_store: self.task_store.clone(),
                cancellation: self.cancellation.clone(),
                lake_formation: self.table.lake_formation.clone(),
            },
        )
    }
}

fn table_request(
    source: &GlueSourceOptions,
    resource: &GlueResourceOptions,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<GlueGetTableRequest> {
    Ok(GlueGetTableRequest {
        region: source.region.clone(),
        catalog_id: source.catalog_id.clone(),
        database: resource.database.clone(),
        table: resource.table.clone(),
        endpoint: source.endpoint.clone(),
        credentials: source
            .credentials
            .as_ref()
            .map(|value| SecretUri::new(value.clone()))
            .transpose()?,
        maximum_response_bytes: source.maximum_response_bytes,
        cancellation,
    })
}

fn requested_data_columns(schema: &Schema, table: &GlueTable) -> Result<Vec<String>> {
    if schema.fields().is_empty() {
        return Ok(Vec::new());
    }
    let available = table
        .storage_descriptor
        .as_ref()
        .ok_or_else(|| CdfError::data("AWS Glue table omitted its StorageDescriptor"))?
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut requested = Vec::new();
    for field in schema.fields() {
        let source_name = cdf_kernel::source_name(field.as_ref()).unwrap_or_else(|| field.name());
        if available.contains(source_name) && !requested.iter().any(|name| name == source_name) {
            requested.push(source_name.to_owned());
        }
    }
    if requested.is_empty() && !available.is_empty() {
        return Err(CdfError::contract(
            "compiled Glue schema contains no table data columns for Lake Formation audit context",
        ));
    }
    Ok(requested)
}

fn glue_query_id(plan: &CompiledSourcePlan, execution: &ExecutionServices) -> Result<String> {
    let sequence = GLUE_QUERY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    artifact_hash(&serde_json::json!({
        "kind": "cdf_glue_query_v1",
        "plan": artifact_hash(plan)?,
        "started_unix_nanos": execution.unix_now().as_nanos().to_string(),
        "process_sequence": sequence,
    }))
}

fn prepared_table_key(plan: &CompiledSourcePlan) -> Result<PreparedSourcePayloadKey> {
    PreparedSourcePayloadKey::new(
        plan.descriptor.resource_id.clone(),
        plan.driver.driver_id.clone(),
        artifact_hash(&serde_json::json!({
            "kind": "glue_table_metadata_v1",
            "source_discovery_binding": plan.discovery_binding_hash()?,
        }))?,
    )
}

fn decode_options<T: for<'de> Deserialize<'de>>(
    label: &str,
    options: BTreeMap<String, serde_json::Value>,
) -> Result<T> {
    serde_json::from_value(serde_json::Value::Object(options.into_iter().collect()))
        .map_err(|error| CdfError::contract(format!("{label} options are invalid: {error}")))
}

fn classification_error(class: GlueTableClass) -> CdfError {
    match class {
        GlueTableClass::Iceberg => CdfError::contract(
            "Glue table is Iceberg; configure source kind `iceberg` with catalog.kind = `glue`",
        ),
        GlueTableClass::Delta => CdfError::contract(
            "Glue table is Delta; use the Delta source or query it through Athena/Trino",
        ),
        GlueTableClass::Hudi => CdfError::contract(
            "Glue table is Hudi; use the Hudi source or query it through Athena/Trino",
        ),
        GlueTableClass::View => {
            CdfError::contract("Glue object is a view; execute it through Athena/Trino")
        }
        GlueTableClass::Federated => CdfError::contract(
            "Glue object is federated/JDBC-backed; use the owning database source or Athena/Trino",
        ),
        GlueTableClass::Stream => {
            CdfError::contract("Glue object describes a stream; use the owning streaming source")
        }
        GlueTableClass::UnsupportedSerde { serde } => CdfError::contract(format!(
            "Glue table uses unsupported SerDe `{serde}`; configure an exact registered format override or use Athena/Trino"
        )),
        GlueTableClass::Conventional(_) => {
            CdfError::internal("conventional Glue table was rejected")
        }
    }
}

fn glue_resource_capabilities() -> ResourceCapabilities {
    ResourceCapabilities {
        projection: CapabilitySupport::Unsupported,
        filters: FilterCapabilities {
            default_fidelity: PushdownFidelity::Exact,
            supported_operators: vec![
                "=".to_owned(),
                "!=".to_owned(),
                ">".to_owned(),
                ">=".to_owned(),
                "<".to_owned(),
                "<=".to_owned(),
            ],
        },
        limits: CapabilitySupport::Unsupported,
        ordering: CapabilitySupport::Unsupported,
        partitioning: PartitioningCapabilities {
            parallel_partitions: true,
            supported_scopes: vec![ScopeKind::File],
        },
        incremental: IncrementalShape::File,
        replay: ReplaySupport::ExactRecordedBatches,
        idempotent_reads: true,
        backpressure: BackpressureSupport::Pausable,
        estimates: EstimateSupport::Bytes,
    }
}

fn glue_execution_capabilities(source: &GlueSourceOptions) -> Result<SourceExecutionCapabilities> {
    Ok(SourceExecutionCapabilities {
        minimum_poll_bytes: 8 * 1024,
        maximum_poll_bytes: 32 * 1024 * 1024,
        minimum_decode_bytes: 8 * 1024,
        maximum_decode_bytes: source.execution_working_set_bytes()?,
        maximum_concurrency: source.maximum_concurrency,
        useful_concurrency: source.maximum_concurrency,
        executor_class: SourceExecutorClass::Cpu,
        blocking_lane: None,
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
        attestation: SourceAttestationStrength::ImmutableContent,
        rate_limit: None,
        quota_authority: None,
        canonical_order: true,
        bounded: true,
        batch_memory: SourceBatchMemoryContract::Preaccounted,
        telemetry_version: "v1".to_owned(),
    })
}

fn object_resource(
    path: &str,
    source: &GlueSourceOptions,
    lake_formation: Option<&LakeFormationRuntime>,
    partition_values: &[Option<String>],
) -> Result<FileTransportResource> {
    let mut resource = FileTransportResource::remote_url(path.to_owned()).with_egress_allowlist(
        if source.egress_allowlist.is_empty() {
            cdf_http::EgressAllowlist::allow_any()
        } else {
            cdf_http::EgressAllowlist::from_hosts(source.egress_allowlist.clone())
        },
    );
    if let Some(lake_formation) = lake_formation {
        resource = resource
            .with_runtime_aws_credentials(lake_formation.binding(path, partition_values)?)?;
    } else if let Some(reference) = &source.object_credentials {
        resource = resource.with_credentials(SecretUri::new(reference.clone())?);
    }
    Ok(resource)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Mutex,
            atomic::{AtomicBool, Ordering},
        },
    };

    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Field};
    use cdf_engine::StandaloneExecutionHost;
    use cdf_http::{SecretUri, SecretValue};
    use cdf_kernel::{
        FileManifest, ResourceId, SchemaSource, ScopeKey, TrustLevel, WriteDisposition,
    };
    use cdf_object_access::{
        AccountedFileIdentity, FILE_IDENTITY_MEMORY_ENVELOPE_BYTES, FileIdentityMetadata,
        FileIdentityStream, FileMetadataObservation,
    };
    use cdf_runtime::{MemoryByteSource, SourceCompileContext};
    use futures_util::{TryStreamExt, stream};
    use parquet::arrow::ArrowWriter;

    use super::*;
    use crate::GlueStorageDescriptor;

    #[derive(Clone)]
    struct StaticCatalog {
        table: GlueTable,
        pages: Arc<Mutex<VecDeque<crate::GluePartitionPage>>>,
        table_reads: Arc<Mutex<u64>>,
        governed: bool,
    }

    #[derive(Debug)]
    struct DenyLakeFormation;

    impl LakeFormationClient for DenyLakeFormation {
        fn vend_credentials(
            &self,
            _request: crate::LakeFormationCredentialRequest,
        ) -> cdf_kernel::BoxFuture<'_, Result<crate::LakeFormationCredentialResponse>> {
            Box::pin(async {
                Err(CdfError::internal(
                    "non-governed Glue test requested Lake Formation credentials",
                ))
            })
        }
    }

    impl GlueCatalogClient for StaticCatalog {
        fn get_table(
            &self,
            _request: GlueGetTableRequest,
        ) -> cdf_kernel::BoxFuture<'_, Result<crate::GlueTableResponse>> {
            *self.table_reads.lock().unwrap() += 1;
            let table = self.table.clone();
            Box::pin(async move {
                Ok(crate::GlueTableResponse {
                    table,
                    lake_formation: None,
                    bytes_read: 1024,
                })
            })
        }

        fn get_partitions(
            &self,
            _request: crate::GlueGetPartitionsRequest,
        ) -> cdf_kernel::BoxFuture<'_, Result<crate::GluePartitionPage>> {
            let page = self.pages.lock().unwrap().pop_front();
            Box::pin(async move {
                page.ok_or_else(|| CdfError::internal("unexpected Glue partition page request"))
            })
        }

        fn get_unfiltered_table(
            &self,
            request: crate::GlueGetUnfilteredTableRequest,
        ) -> cdf_kernel::BoxFuture<'_, Result<crate::GlueTableResponse>> {
            let governed = self.governed;
            let table = self.table.clone();
            Box::pin(async move {
                if !governed {
                    return Err(CdfError::internal(
                        "non-governed test requested unfiltered Glue table metadata",
                    ));
                }
                let columns = table
                    .storage_descriptor
                    .as_ref()
                    .unwrap()
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect::<Vec<_>>();
                Ok(crate::GlueTableResponse {
                    table,
                    lake_formation: Some(crate::GlueLakeFormationAuthorization {
                        query_id: request.query_id,
                        query_start_unix_seconds: request.query_start_unix_seconds,
                        query_authorization_id: "authorization-1".to_owned(),
                        resource_arn: "arn:aws:glue:us-west-2:123456789012:table/analytics/events"
                            .to_owned(),
                        authorized_columns: columns,
                    }),
                    bytes_read: 2048,
                })
            })
        }

        fn get_unfiltered_partitions(
            &self,
            _request: crate::GlueGetUnfilteredPartitionsRequest,
        ) -> cdf_kernel::BoxFuture<'_, Result<crate::GluePartitionPage>> {
            let page = self.pages.lock().unwrap().pop_front();
            Box::pin(async move {
                page.ok_or_else(|| {
                    CdfError::internal("unexpected governed Glue partition page request")
                })
            })
        }
    }

    #[derive(Clone)]
    struct StaticObjectAccess {
        identity: FileIdentityMetadata,
        source: Arc<dyn cdf_runtime::ByteSource>,
        memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        runtime_authority_seen: Arc<AtomicBool>,
    }

    impl FileTransport for StaticObjectAccess {
        fn metadata(
            &self,
            _egress: &cdf_runtime::SourceEgressScope,
            resource: &FileTransportResource,
            _control: &FileTransportControl,
        ) -> Result<FileMetadataObservation> {
            self.runtime_authority_seen
                .store(resource.uses_runtime_aws_credentials(), Ordering::Relaxed);
            Ok(FileMetadataObservation::direct(
                resource,
                self.identity.clone(),
            ))
        }

        fn list(
            &self,
            _egress: &cdf_runtime::SourceEgressScope,
            resource: &FileTransportResource,
            maximum_results: usize,
            _control: &FileTransportControl,
        ) -> Result<FileIdentityStream> {
            self.runtime_authority_seen
                .store(resource.uses_runtime_aws_credentials(), Ordering::Relaxed);
            if maximum_results == 0 {
                return Err(CdfError::contract(
                    "test object listing requires a nonzero result bound",
                ));
            }
            let lease = cdf_memory::reserve_blocking(
                Arc::clone(&self.memory),
                &cdf_memory::ReservationRequest::new(
                    cdf_memory::ConsumerKey::new(
                        "glue-test-object-identity",
                        cdf_memory::MemoryClass::Discovery,
                    )?,
                    FILE_IDENTITY_MEMORY_ENVELOPE_BYTES,
                )?,
            )?;
            let identity = AccountedFileIdentity::new(self.identity.clone(), lease)?;
            Ok(FileIdentityStream::materialized(stream::iter([Ok(
                identity,
            )])))
        }

        fn open_byte_source(
            &self,
            _egress: &cdf_runtime::SourceEgressScope,
            _resource: &FileTransportResource,
            expected: &FileIdentityMetadata,
            _memory: Arc<dyn cdf_memory::MemoryCoordinator>,
        ) -> Result<Arc<dyn cdf_runtime::ByteSource>> {
            if expected != &self.identity {
                return Err(CdfError::data(
                    "test object generation differs from the planned identity",
                ));
            }
            Ok(Arc::clone(&self.source))
        }
    }

    struct NoopSecrets;

    impl SecretProvider for NoopSecrets {
        fn resolve(&self, _uri: &SecretUri) -> Result<SecretValue> {
            Err(CdfError::auth("Glue test does not resolve secrets"))
        }
    }

    fn table_fixture() -> GlueTable {
        let descriptor = GlueStorageDescriptor {
            columns: vec![
                crate::GlueColumn {
                    name: "id".to_owned(),
                    type_name: "bigint".to_owned(),
                    comment: None,
                    parameters: BTreeMap::new(),
                },
                crate::GlueColumn {
                    name: "label".to_owned(),
                    type_name: "string".to_owned(),
                    comment: None,
                    parameters: BTreeMap::new(),
                },
            ],
            location: Some("s3://fixture/events/".to_owned()),
            input_format: Some(
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat".to_owned(),
            ),
            output_format: None,
            compressed: Some(false),
            serde_info: Some(crate::GlueSerdeInfo {
                serialization_library: Some(
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe".to_owned(),
                ),
                parameters: BTreeMap::new(),
            }),
            parameters: BTreeMap::new(),
        };
        GlueTable {
            name: "events".to_owned(),
            database_name: Some("analytics".to_owned()),
            catalog_id: Some("123456789012".to_owned()),
            version_id: Some("7".to_owned()),
            update_time: None,
            table_type: Some("EXTERNAL_TABLE".to_owned()),
            parameters: BTreeMap::new(),
            partition_keys: vec![crate::GlueColumn {
                name: "day".to_owned(),
                type_name: "string".to_owned(),
                comment: None,
                parameters: BTreeMap::new(),
            }],
            storage_descriptor: Some(descriptor),
            view_original_text: None,
            view_expanded_text: None,
            target_table: None,
            is_registered_with_lake_formation: false,
        }
    }

    fn parquet_fixture() -> (SchemaRef, Vec<u8>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
            ],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(Vec::new(), Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        (schema, writer.into_inner().unwrap())
    }

    fn compile_request(project_root: &std::path::Path) -> SourceCompileRequest {
        SourceCompileRequest {
            source_kind: "glue".to_owned(),
            context: SourceCompileContext {
                source_name: "catalog".to_owned(),
                project_root: Some(project_root.to_path_buf()),
                cursor_pushdown: None,
            },
            source_options: BTreeMap::from([
                ("region".to_owned(), serde_json::json!("us-west-2")),
                ("egress_allowlist".to_owned(), serde_json::json!([])),
            ]),
            resource_options: BTreeMap::from([
                ("database".to_owned(), serde_json::json!("analytics")),
                ("table".to_owned(), serde_json::json!("events")),
            ]),
            descriptor: ResourceDescriptor {
                resource_id: ResourceId::new("catalog.events").unwrap(),
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
            type_policy_allowances: TypePolicyAllowances::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        }
    }

    #[test]
    fn conventional_table_discovers_plans_executes_and_resumes_without_source_branches() {
        let root = tempfile::tempdir().unwrap();
        let (_host, execution) =
            StandaloneExecutionHost::default_services_with_spill(256 << 20, 256 << 20).unwrap();
        let (_data_schema, parquet) = parquet_fixture();
        let object_path = "s3://fixture/events/day=2026-07-20/part-000.parquet";
        let identity = FileIdentityMetadata {
            location: object_path.to_owned(),
            size_bytes: Some(u64::try_from(parquet.len()).unwrap()),
            checksum: None,
            etag: Some("fixture-generation-1".to_owned()),
            version: None,
            modified: Some("2026-07-20T00:00:00Z".to_owned()),
            exact_ranges: true,
        };
        let source: Arc<dyn cdf_runtime::ByteSource> = Arc::new(
            futures_executor::block_on(MemoryByteSource::from_bytes(
                object_path,
                parquet,
                execution.memory(),
            ))
            .unwrap(),
        );
        let object_access: Arc<dyn FileTransport> = Arc::new(StaticObjectAccess {
            identity: identity.clone(),
            source,
            memory: execution.memory(),
            runtime_authority_seen: Arc::new(AtomicBool::new(false)),
        });
        let table = table_fixture();
        let table_reads = Arc::new(Mutex::new(0));
        let catalog: Arc<dyn GlueCatalogClient> = Arc::new(StaticCatalog {
            table: table.clone(),
            pages: Arc::new(Mutex::new(VecDeque::from([crate::GluePartitionPage {
                partitions: vec![crate::GluePartition {
                    values: vec!["2026-07-20".to_owned()],
                    storage_descriptor: Some(GlueStorageDescriptor {
                        location: Some("s3://fixture/events/day=2026-07-20/".to_owned()),
                        ..GlueStorageDescriptor::default()
                    }),
                    parameters: BTreeMap::new(),
                }],
                next_token: None,
                bytes_read: 512,
            }]))),
            table_reads: Arc::clone(&table_reads),
            governed: false,
        });
        let mut formats = FormatRegistry::default();
        formats
            .register(Arc::new(
                cdf_format_parquet::ParquetFormatDriver::new().unwrap(),
            ))
            .unwrap();
        let formats = Arc::new(formats);
        let transforms = Arc::new(ByteTransformRegistry::default());
        let driver = GlueSourceDriver::new(move |_secrets, _execution, _egress| {
            Ok(GlueRuntimeDependencies::new(
                Arc::clone(&object_access),
                Arc::clone(&catalog),
                Arc::new(DenyLakeFormation),
                Arc::clone(&formats),
                Arc::clone(&transforms),
            ))
        })
        .unwrap();
        let mut plan = driver.compile(compile_request(root.path())).unwrap();
        let context = SourceResolutionContext::new(
            root.path(),
            Arc::new(NoopSecrets),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let session = driver.discovery_session(&plan, &context).unwrap();
        let candidate = session.candidates().unwrap().remove(0);
        let observation = session
            .observe(
                &candidate,
                &SourceDiscoveryRequest::new(16 << 20, 1).unwrap(),
            )
            .unwrap();
        assert_eq!(observation.schema.fields().len(), 3);
        plan.schema = observation.schema.as_ref().clone();
        let resource = driver.resolve(&plan, &context).unwrap();
        let request = ScanRequest {
            resource_id: plan.descriptor.resource_id.clone(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        };
        let scan = resource.negotiate(&request).unwrap();
        let reference = scan.planned_task_set.as_ref().unwrap();
        assert_eq!(reference.task_count, 1);
        assert_eq!(*table_reads.lock().unwrap(), 2);

        let mut reader = resource.planned_partition_reader(reference).unwrap();
        let executable = reader.next_partition(0).unwrap().unwrap();
        let rows = futures_executor::block_on(async {
            let mut stream = resource.open_executable(executable).await?;
            let mut rows = 0;
            while let Some(batch) = stream.try_next().await? {
                let record = batch
                    .record_batch()
                    .ok_or_else(|| CdfError::internal("Glue test expected Arrow data"))?;
                assert_eq!(record.schema().field(2).name(), "day");
                let day = record
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                assert!(day.iter().all(|value| value == Some("2026-07-20")));
                rows += record.num_rows();
            }
            stream.completion().await?;
            Ok::<_, CdfError>(rows)
        })
        .unwrap();
        assert_eq!(rows, 2);
        assert!(reader.next_partition(1).unwrap().is_none());

        let mut resumed = scan;
        resource
            .rebind_scan_for_resume(
                &mut resumed,
                &SourcePosition::FileManifest(FileManifest {
                    version: cdf_kernel::SOURCE_POSITION_VERSION,
                    files: vec![identity.file_position_evidence().unwrap()],
                }),
            )
            .unwrap();
        assert!(resumed.planned_task_set.is_none());
        assert_eq!(resumed.estimated_bytes, Some(0));
    }

    #[test]
    fn governed_table_uses_authorized_metadata_and_runtime_partition_binding() {
        let root = tempfile::tempdir().unwrap();
        let (_host, execution) =
            StandaloneExecutionHost::default_services_with_spill(256 << 20, 256 << 20).unwrap();
        let (_data_schema, parquet) = parquet_fixture();
        let object_path = "s3://fixture/events/day=2026-07-20/part-000.parquet";
        let identity = FileIdentityMetadata {
            location: object_path.to_owned(),
            size_bytes: Some(u64::try_from(parquet.len()).unwrap()),
            checksum: None,
            etag: Some("fixture-generation-1".to_owned()),
            version: None,
            modified: Some("2026-07-20T00:00:00Z".to_owned()),
            exact_ranges: true,
        };
        let source: Arc<dyn cdf_runtime::ByteSource> = Arc::new(
            futures_executor::block_on(MemoryByteSource::from_bytes(
                object_path,
                parquet,
                execution.memory(),
            ))
            .unwrap(),
        );
        let runtime_authority_seen = Arc::new(AtomicBool::new(false));
        let object_access: Arc<dyn FileTransport> = Arc::new(StaticObjectAccess {
            identity,
            source,
            memory: execution.memory(),
            runtime_authority_seen: Arc::clone(&runtime_authority_seen),
        });
        let mut table = table_fixture();
        table.is_registered_with_lake_formation = true;
        let catalog: Arc<dyn GlueCatalogClient> = Arc::new(StaticCatalog {
            table,
            pages: Arc::new(Mutex::new(VecDeque::from([crate::GluePartitionPage {
                partitions: vec![crate::GluePartition {
                    values: vec!["2026-07-20".to_owned()],
                    storage_descriptor: Some(GlueStorageDescriptor {
                        location: Some("s3://fixture/events/day=2026-07-20/".to_owned()),
                        ..GlueStorageDescriptor::default()
                    }),
                    parameters: BTreeMap::new(),
                }],
                next_token: None,
                bytes_read: 512,
            }]))),
            table_reads: Arc::new(Mutex::new(0)),
            governed: true,
        });
        let mut formats = FormatRegistry::default();
        formats
            .register(Arc::new(
                cdf_format_parquet::ParquetFormatDriver::new().unwrap(),
            ))
            .unwrap();
        let formats = Arc::new(formats);
        let transforms = Arc::new(ByteTransformRegistry::default());
        let driver = GlueSourceDriver::new(move |_secrets, _execution, _egress| {
            Ok(GlueRuntimeDependencies::new(
                Arc::clone(&object_access),
                Arc::clone(&catalog),
                Arc::new(DenyLakeFormation),
                Arc::clone(&formats),
                Arc::clone(&transforms),
            ))
        })
        .unwrap();
        let mut plan = driver.compile(compile_request(root.path())).unwrap();
        let context = SourceResolutionContext::new(
            root.path(),
            Arc::new(NoopSecrets),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let discovery = driver.discovery_session(&plan, &context).unwrap();
        let candidate = discovery.candidates().unwrap().remove(0);
        plan.schema = discovery
            .observe(
                &candidate,
                &SourceDiscoveryRequest::new(16 << 20, 1).unwrap(),
            )
            .unwrap()
            .schema
            .as_ref()
            .clone();
        let resource = driver.resolve(&plan, &context).unwrap();
        let scan = resource
            .negotiate(&ScanRequest {
                resource_id: plan.descriptor.resource_id,
                projection: None,
                filters: Vec::new(),
                limit: None,
                order_by: Vec::new(),
                scope: ScopeKey::Resource,
            })
            .unwrap();
        assert_eq!(scan.planned_task_set.unwrap().task_count, 1);
        assert!(runtime_authority_seen.load(Ordering::Relaxed));
    }

    #[test]
    fn row_format_table_uses_the_registered_streaming_decoder() {
        let root = tempfile::tempdir().unwrap();
        let (_host, execution) =
            StandaloneExecutionHost::default_services_with_spill(256 << 20, 256 << 20).unwrap();
        let payload = br#"{"id":1,"label":"one"}
{"id":2,"label":"two"}
"#
        .to_vec();
        let object_path = "s3://fixture/events/part-000.ndjson";
        let identity = FileIdentityMetadata {
            location: object_path.to_owned(),
            size_bytes: Some(u64::try_from(payload.len()).unwrap()),
            checksum: None,
            etag: Some("fixture-generation-1".to_owned()),
            version: None,
            modified: Some("2026-07-20T00:00:00Z".to_owned()),
            exact_ranges: true,
        };
        let source: Arc<dyn cdf_runtime::ByteSource> = Arc::new(
            futures_executor::block_on(MemoryByteSource::from_bytes(
                object_path,
                payload,
                execution.memory(),
            ))
            .unwrap(),
        );
        let object_access: Arc<dyn FileTransport> = Arc::new(StaticObjectAccess {
            identity,
            source,
            memory: execution.memory(),
            runtime_authority_seen: Arc::new(AtomicBool::new(false)),
        });
        let mut table = table_fixture();
        table.partition_keys.clear();
        table.parameters = BTreeMap::from([("classification".to_owned(), "json".to_owned())]);
        let descriptor = table.storage_descriptor.as_mut().unwrap();
        descriptor.location = Some("s3://fixture/events/".to_owned());
        descriptor.input_format = Some("org.apache.hadoop.mapred.TextInputFormat".to_owned());
        descriptor.serde_info = Some(crate::GlueSerdeInfo {
            serialization_library: Some("org.openx.data.jsonserde.JsonSerDe".to_owned()),
            parameters: BTreeMap::new(),
        });
        let catalog: Arc<dyn GlueCatalogClient> = Arc::new(StaticCatalog {
            table,
            pages: Arc::new(Mutex::new(VecDeque::new())),
            table_reads: Arc::new(Mutex::new(0)),
            governed: false,
        });
        let mut formats = FormatRegistry::default();
        formats
            .register(Arc::new(
                cdf_format_json::NdjsonFormatDriver::new().unwrap(),
            ))
            .unwrap();
        let formats = Arc::new(formats);
        let transforms = Arc::new(ByteTransformRegistry::default());
        let driver = GlueSourceDriver::new(move |_secrets, _execution, _egress| {
            Ok(GlueRuntimeDependencies::new(
                Arc::clone(&object_access),
                Arc::clone(&catalog),
                Arc::new(DenyLakeFormation),
                Arc::clone(&formats),
                Arc::clone(&transforms),
            ))
        })
        .unwrap();
        let mut plan = driver.compile(compile_request(root.path())).unwrap();
        let context = SourceResolutionContext::new(
            root.path(),
            Arc::new(NoopSecrets),
            &execution,
            Arc::new(cdf_http::EgressAllowlist::allow_any()),
        );
        let discovery = driver.discovery_session(&plan, &context).unwrap();
        let candidate = discovery.candidates().unwrap().remove(0);
        plan.schema = discovery
            .observe(
                &candidate,
                &SourceDiscoveryRequest::new(16 << 20, 1).unwrap(),
            )
            .unwrap()
            .schema
            .as_ref()
            .clone();
        let resource = driver.resolve(&plan, &context).unwrap();
        let scan = resource
            .negotiate(&ScanRequest {
                resource_id: plan.descriptor.resource_id,
                projection: None,
                filters: Vec::new(),
                limit: None,
                order_by: Vec::new(),
                scope: ScopeKey::Resource,
            })
            .unwrap();
        let mut reader = resource
            .planned_partition_reader(scan.planned_task_set.as_ref().unwrap())
            .unwrap();
        let executable = reader.next_partition(0).unwrap().unwrap();
        let rows = futures_executor::block_on(async {
            let mut stream = resource.open_executable(executable).await?;
            let mut rows = 0;
            while let Some(batch) = stream.try_next().await? {
                rows += batch
                    .record_batch()
                    .ok_or_else(|| CdfError::internal("Glue test expected Arrow data"))?
                    .num_rows();
            }
            stream.completion().await?;
            Ok::<_, CdfError>(rows)
        })
        .unwrap();
        assert_eq!(rows, 2);
    }
}
