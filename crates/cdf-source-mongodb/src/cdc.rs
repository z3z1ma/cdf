use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{Schema, SchemaRef};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use cdf_kernel::{
    Batch, BatchId, BatchStream, CdcMetadata, CdcOperation, CdcSettlementBoundary,
    CdcSettlementMarker, CdcSettlementUnitKind, CdfError, CompiledScanIntent,
    CompiledSourcePlanHash, DeliveryGuarantee, EffectiveSchemaCatalogEntry, EffectiveSchemaRuntime,
    PartitionAuthority, PartitionId, PartitionPlan, PayloadRetention, PlanId,
    PreContractPhysicalReconciliation, PreContractResidualCandidate, QueryableResource,
    ResourceCapabilities, ResourceDescriptor, ResourceStream, Result, RoutePlan, RouteScalar,
    ScanPlan, ScanRequest, SourcePosition, bind_partition_schema_observation, source_name,
};
use cdf_memory::{ConsumerKey, MemoryClass, ReservationRequest, reserve};
use cdf_runtime::{ExecutionServices, SourceEgressScope, TaskStreamSender};
use mongodb::{
    change_stream::{
        ChangeStream,
        event::{ChangeStreamEvent, OperationType, ResumeToken},
    },
    options::FullDocumentType,
};
use sha2::{Digest, Sha256};

use crate::{
    driver::{
        MongoDbBootstrap, MongoDbPhysicalPlan, MongoDbRepresentation, MongoDbRuntimeConfig,
        MongoDbWatch, compile_globs, compiled_database_inventory, mongodb_cdc_capabilities,
        mongodb_change_stream_scope, read_collection_metadata, validate_server_version,
    },
    error::classify_mongodb_error,
    execution::{
        MONGODB_MAXIMUM_DECODE_BYTES, MONGODB_MAXIMUM_OUTPUT_BATCH_BYTES, MongoDbClientHandle,
        connect_mongodb,
    },
    identifier::MongoDbIdentifier,
    resource::{current_physical_schema, validate_compiled_schema_evidence},
    schema::{attach_expected_physical_types, decode_batch_with_physical_schema},
};

const MONGODB_CDC_PARTITION: &str = "mongodb-cdc";

#[derive(Clone)]
pub(crate) struct MongoDbCdcResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    decoder_schema: Option<SchemaRef>,
    physical_schema: Option<SchemaRef>,
    typed_routes: BTreeMap<String, MongoDbTypedRouteSchema>,
    observation_ids: BTreeMap<String, String>,
    database: MongoDbIdentifier,
    collection: Option<MongoDbIdentifier>,
    watch: MongoDbWatch,
    representation: MongoDbRepresentation,
    admitted_collections: Vec<String>,
    include_collections: Vec<glob::Pattern>,
    exclude_collections: Vec<glob::Pattern>,
    change_pipeline: Vec<mongodb::bson::Document>,
    change_batch_rows: u32,
    change_max_await_ms: u64,
    change_comment: Option<String>,
    read_concern: Option<mongodb::options::ReadConcern>,
    selection_criteria: Option<mongodb::options::SelectionCriteria>,
    scope: cdf_kernel::MongoChangeStreamScope,
    runtime: MongoDbRuntimeConfig,
    stream_buffer_batches: usize,
    client: Arc<tokio::sync::OnceCell<MongoDbClientHandle>>,
    capabilities: ResourceCapabilities,
    execution: ExecutionServices,
    egress: SourceEgressScope,
    type_policy_allowances: cdf_kernel::TypePolicyAllowances,
    source_materializations: Vec<cdf_kernel::SourceMaterializationRule>,
    compiled_source_plan_hash: CompiledSourcePlanHash,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
}

#[derive(Clone)]
struct MongoDbTypedRouteSchema {
    effective_schema: SchemaRef,
    physical_schema: SchemaRef,
    document_effective_schema: SchemaRef,
    document_physical_schema: SchemaRef,
    decoder_schema: SchemaRef,
}

struct MongoDbCdcEventBatch {
    record_batch: RecordBatch,
    observed_schema: Option<SchemaRef>,
    residual_candidates: Vec<PreContractResidualCandidate>,
    physical_reconciliations: Vec<PreContractPhysicalReconciliation>,
    pre_contract_evidence_bytes: u64,
    residuals_complete: bool,
}

impl MongoDbCdcEventBatch {
    fn exact(record_batch: RecordBatch, observed_schema: Option<SchemaRef>) -> Self {
        Self {
            record_batch,
            observed_schema,
            residual_candidates: Vec::new(),
            physical_reconciliations: Vec::new(),
            pre_contract_evidence_bytes: 0,
            residuals_complete: false,
        }
    }
}

impl MongoDbCdcResource {
    pub(crate) fn from_compiled_plan(
        compiled: &cdf_runtime::CompiledSourcePlan,
        physical: MongoDbPhysicalPlan,
        runtime: MongoDbRuntimeConfig,
        egress: SourceEgressScope,
        execution: ExecutionServices,
    ) -> Result<Self> {
        if physical.bootstrap == Some(MongoDbBootstrap::Snapshot) {
            return Err(CdfError::contract(
                "MongoDB CDC snapshot bootstrap requires the gapless snapshot handoff runtime, which is not available in this build",
            ));
        }
        let watch = physical
            .watch
            .ok_or_else(|| CdfError::contract("MongoDB CDC omitted its watch scope"))?;
        let representation = physical
            .representation
            .ok_or_else(|| CdfError::contract("MongoDB CDC omitted its representation"))?;
        physical
            .bootstrap
            .ok_or_else(|| CdfError::contract("MongoDB CDC omitted its bootstrap"))?;
        validate_compiled_schema_evidence(compiled)?;
        let schema = Arc::new(compiled.schema.clone());
        let (mut physical_schema, decoder_schema) = if watch == MongoDbWatch::Collection {
            let observed = current_physical_schema(compiled)?;
            let decoder = attach_expected_physical_types(schema.as_ref(), observed.as_ref())?;
            (Some(observed), Some(decoder))
        } else {
            (None, None)
        };
        let admitted_collections = match watch {
            MongoDbWatch::Collection => vec![
                physical
                    .collection
                    .as_ref()
                    .ok_or_else(|| CdfError::contract("MongoDB collection CDC omitted collection"))?
                    .as_str()
                    .to_owned(),
            ],
            MongoDbWatch::Database => {
                let inventory = compiled_database_inventory(
                    &physical.database,
                    compiled.effective_schema_runtime.as_ref(),
                )?;
                if inventory.is_empty() {
                    return Err(CdfError::data(
                        "MongoDB database CDC has no compiled collection inventory; run discovery and compile again",
                    ));
                }
                inventory
            }
        };
        if watch == MongoDbWatch::Database && representation == MongoDbRepresentation::Envelope {
            let envelope_hash = cdf_kernel::canonical_arrow_schema_hash(
                crate::driver::mongodb_envelope_schema().as_ref(),
            )?;
            let runtime = compiled.effective_schema_runtime.as_ref().ok_or_else(|| {
                CdfError::data(
                    "MongoDB database CDC envelope omitted compiled discovery evidence; run discovery and compile again",
                )
            })?;
            if runtime
                .evidence
                .observations()
                .iter()
                .any(|observation| observation.physical_schema_hash != envelope_hash)
            {
                return Err(CdfError::data(
                    "MongoDB database CDC envelope discovery contains a non-envelope physical schema",
                ));
            }
            physical_schema = Some(
                runtime
                    .physical_schema(&envelope_hash)
                    .cloned()
                    .ok_or_else(|| {
                        CdfError::data(
                            "MongoDB database CDC envelope schema is absent from its compiled physical catalog",
                        )
                    })?,
            );
        }
        let observation_ids = database_observation_ids(
            watch,
            &physical.database,
            &admitted_collections,
            compiled.effective_schema_runtime.as_ref(),
        )?;
        let typed_routes = if watch == MongoDbWatch::Database
            && representation == MongoDbRepresentation::Typed
        {
            typed_database_routes(
                &admitted_collections,
                compiled.effective_schema_runtime.as_ref().ok_or_else(|| {
                    CdfError::data(
                        "typed MongoDB database CDC omitted compiled schema evidence; run discovery and compile again",
                    )
                })?,
            )?
        } else {
            BTreeMap::new()
        };
        Ok(Self {
            descriptor: compiled.descriptor.clone(),
            schema,
            decoder_schema,
            physical_schema,
            typed_routes,
            observation_ids,
            database: physical.database.clone(),
            collection: physical.collection.clone(),
            watch,
            representation,
            admitted_collections,
            include_collections: compile_globs(&physical.include_collections)?,
            exclude_collections: compile_globs(&physical.exclude_collections)?,
            change_pipeline: physical.change_pipeline.clone(),
            change_batch_rows: physical.change_batch_rows,
            change_max_await_ms: physical.change_max_await_ms,
            change_comment: physical.change_comment.clone(),
            read_concern: physical.native.change_stream_read_concern(),
            selection_criteria: physical.native.change_stream_selection_criteria(),
            scope: mongodb_change_stream_scope(&physical)?,
            runtime,
            stream_buffer_batches: physical.stream_buffer_batches,
            client: Arc::new(tokio::sync::OnceCell::new()),
            capabilities: mongodb_cdc_capabilities(&compiled.descriptor),
            execution,
            egress,
            type_policy_allowances: compiled.type_policy_allowances,
            source_materializations: compiled.source_materializations.clone(),
            compiled_source_plan_hash: compiled.compiled_source_plan_hash()?,
            effective_schema_runtime: compiled.effective_schema_runtime.clone(),
            baseline_observation_schema_catalog: compiled
                .baseline_observation_schema_catalog
                .clone(),
        })
    }

    fn validate_scan_request(&self, request: &ScanRequest) -> Result<()> {
        if request.resource_id != self.descriptor.resource_id {
            return Err(CdfError::contract(
                "MongoDB CDC scan selected another resource",
            ));
        }
        if !request.filters.is_empty() || request.limit.is_some() || !request.order_by.is_empty() {
            return Err(CdfError::contract(
                "MongoDB CDC does not accept run-time filters, limits, or ordering; use change_pipeline for source-side event filtering",
            ));
        }
        Ok(())
    }

    fn admits_collection(&self, collection: &str) -> bool {
        !collection.starts_with("system.")
            && (self.include_collections.is_empty()
                || self
                    .include_collections
                    .iter()
                    .any(|pattern| pattern.matches(collection)))
            && !self
                .exclude_collections
                .iter()
                .any(|pattern| pattern.matches(collection))
    }

    fn typed_route(&self, collection: &str) -> Result<&MongoDbTypedRouteSchema> {
        self.typed_routes.get(collection).ok_or_else(|| {
            CdfError::data(format!(
                "MongoDB change event selected typed collection `{}.{collection}` without compiled routed schema authority; run discovery and compile before resuming",
                self.database
            ))
        })
    }

    fn partition(&self, request: &ScanRequest) -> Result<PartitionPlan> {
        self.validate_scan_request(request)?;
        let mut partition = PartitionPlan {
            partition_id: PartitionId::new(MONGODB_CDC_PARTITION)?,
            scope: self.descriptor.state_scope.clone(),
            planned_position: None,
            start_position: None,
            scan_intent: CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::from([
                ("kind".to_owned(), MONGODB_CDC_PARTITION.to_owned()),
                ("database".to_owned(), self.database.as_str().to_owned()),
            ]),
        };
        if self.watch == MongoDbWatch::Collection
            && let Some(runtime) = &self.effective_schema_runtime
        {
            let observation_id = format!(
                "{}.{}",
                self.database,
                self.collection.as_ref().ok_or_else(|| {
                    CdfError::internal("MongoDB collection CDC lost its collection")
                })?
            );
            bind_partition_schema_observation(&mut partition, runtime, &observation_id)?;
        }
        Ok(partition)
    }

    fn open_owned(self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'static> {
        let execution = self.execution.clone();
        let memory = execution.memory();
        let task = match execution.spawn_io_stream(
            "mongodb-change-stream",
            self.stream_buffer_batches,
            move |sender, cancellation| async move {
                execute_change_stream(self, partition, memory, sender, cancellation).await
            },
        ) {
            Ok(task) => task,
            Err(error) => {
                return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move {
                    Err(error)
                }));
            }
        };
        let termination = task.termination();
        cdf_kernel::PartitionOpenAttempt::with_termination(
            Box::pin(async move {
                Ok(cdf_kernel::PartitionStreamPayload::new(
                    Box::pin(task) as BatchStream,
                    Box::pin(async { Ok(cdf_kernel::PartitionCompletion::default()) }),
                ))
            }),
            termination,
        )
    }
}

fn database_observation_ids(
    watch: MongoDbWatch,
    database: &MongoDbIdentifier,
    admitted_collections: &[String],
    runtime: Option<&EffectiveSchemaRuntime>,
) -> Result<BTreeMap<String, String>> {
    if watch == MongoDbWatch::Collection {
        return Ok(BTreeMap::new());
    }
    let runtime = runtime.ok_or_else(|| {
        CdfError::data(
            "MongoDB database CDC omitted compiled discovery evidence; run discovery and compile again",
        )
    })?;
    let mut observations = BTreeMap::new();
    for observation in runtime.evidence.observations() {
        let route = observation.route.as_ref().ok_or_else(|| {
            CdfError::data(format!(
                "MongoDB database CDC observation {:?} omitted its collection route",
                observation.observation_id
            ))
        })?;
        if route.field != "source_collection" {
            return Err(CdfError::data(format!(
                "MongoDB database CDC observation {:?} routes by {:?}, expected `source_collection`",
                observation.observation_id, route.field
            )));
        }
        let collection = route.value.canonical_value.clone();
        if observation.observation_id != format!("{database}.{collection}")
            || observations
                .insert(collection.clone(), observation.observation_id.clone())
                .is_some()
        {
            return Err(CdfError::data(
                "MongoDB database CDC discovery carries duplicate or inconsistent collection observation identity",
            ));
        }
    }
    if observations.keys().map(String::as_str).collect::<Vec<_>>()
        != admitted_collections
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    {
        return Err(CdfError::data(
            "MongoDB database CDC collection inventory does not exactly match routed schema observations; run discovery and compile again",
        ));
    }
    Ok(observations)
}

fn typed_database_routes(
    admitted_collections: &[String],
    runtime: &EffectiveSchemaRuntime,
) -> Result<BTreeMap<String, MongoDbTypedRouteSchema>> {
    let mut routes = BTreeMap::new();
    for observation in runtime.evidence.observations() {
        let route = observation.route.as_ref().ok_or_else(|| {
            CdfError::data(format!(
                "typed MongoDB database observation {:?} omitted its collection route",
                observation.observation_id
            ))
        })?;
        if route.field != "source_collection" {
            return Err(CdfError::data(
                "typed MongoDB database CDC requires `source_collection` routed schema authority",
            ));
        }
        let collection = route.value.canonical_value.clone();
        let physical_schema = runtime
            .physical_schema(&observation.physical_schema_hash)
            .cloned()
            .ok_or_else(|| {
                CdfError::data(format!(
                    "typed MongoDB collection `{collection}` omitted its physical schema"
                ))
            })?;
        let effective_schema = runtime
            .physical_schema(&observation.effective_schema_hash)
            .cloned()
            .ok_or_else(|| {
                CdfError::data(format!(
                    "typed MongoDB collection `{collection}` omitted its effective schema"
                ))
            })?;
        validate_collection_route_field(physical_schema.as_ref(), &collection)?;
        validate_collection_route_field(effective_schema.as_ref(), &collection)?;
        let document_physical_schema = without_collection_route(physical_schema.as_ref())?;
        let document_effective_schema = without_collection_route(effective_schema.as_ref())?;
        let decoder_schema = attach_expected_physical_types(
            document_effective_schema.as_ref(),
            document_physical_schema.as_ref(),
        )?;
        let item = MongoDbTypedRouteSchema {
            effective_schema,
            physical_schema,
            document_effective_schema,
            document_physical_schema,
            decoder_schema,
        };
        if routes.insert(collection, item).is_some() {
            return Err(CdfError::data(
                "typed MongoDB database CDC contains duplicate collection route authority",
            ));
        }
    }
    if routes.keys().map(String::as_str).collect::<Vec<_>>()
        != admitted_collections
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    {
        return Err(CdfError::data(
            "typed MongoDB database CDC schemas do not exactly cover the admitted collection inventory",
        ));
    }
    Ok(routes)
}

fn validate_collection_route_field(schema: &Schema, collection: &str) -> Result<()> {
    let field = schema.field_with_name("source_collection").map_err(|_| {
        CdfError::data(format!(
            "typed MongoDB collection `{collection}` omitted its protected `source_collection` field"
        ))
    })?;
    if field.is_nullable()
        || field.data_type() != &arrow_schema::DataType::Utf8
        || schema
            .fields()
            .last()
            .is_none_or(|field| field.name() != "source_collection")
    {
        return Err(CdfError::data(format!(
            "typed MongoDB collection `{collection}` must append non-null UTF-8 `source_collection` authority"
        )));
    }
    Ok(())
}

fn without_collection_route(schema: &Schema) -> Result<SchemaRef> {
    let indices = schema
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(index, field)| (field.name() != "source_collection").then_some(index))
        .collect::<Vec<_>>();
    if indices.len().checked_add(1) != Some(schema.fields().len()) {
        return Err(CdfError::data(
            "typed MongoDB routed schema must contain exactly one `source_collection` field",
        ));
    }
    Ok(Arc::new(schema.project(&indices).map_err(CdfError::from)?))
}

impl fmt::Debug for MongoDbCdcResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MongoDbCdcResource")
            .field("descriptor", &self.descriptor)
            .field("database", &self.database)
            .field("collection", &self.collection)
            .field("watch", &self.watch)
            .field("representation", &self.representation)
            .field(
                "admitted_collection_count",
                &self.admitted_collections.len(),
            )
            .finish_non_exhaustive()
    }
}

impl ResourceStream for MongoDbCdcResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn required_route_field(&self) -> Option<&str> {
        (self.watch == MongoDbWatch::Database).then_some("source_collection")
    }

    fn routed_output_schemas(&self, route: &RoutePlan) -> Result<Vec<(RouteScalar, SchemaRef)>> {
        if self.watch != MongoDbWatch::Database {
            return Ok(Vec::new());
        }
        if route.field != "source_collection" {
            return Err(CdfError::contract(
                "MongoDB database CDC must route by `source_collection`",
            ));
        }
        if self.representation == MongoDbRepresentation::Typed {
            return self
                .effective_schema_runtime
                .as_ref()
                .ok_or_else(|| {
                    CdfError::data(
                        "typed MongoDB database CDC omitted routed schema authority; discover and compile again",
                    )
                })?
                .routed_observation_schemas(route);
        }
        self.admitted_collections
            .iter()
            .map(|collection| {
                let values = StringArray::from(vec![collection.as_str()]);
                Ok((
                    RouteScalar::from_array(&values, 0)?,
                    Arc::clone(&self.schema),
                ))
            })
            .collect()
    }

    fn compiled_source_plan_hash(&self) -> Option<&CompiledSourcePlanHash> {
        Some(&self.compiled_source_plan_hash)
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.effective_schema_runtime.as_ref()
    }

    fn baseline_observation_schema_catalog(&self) -> &[EffectiveSchemaCatalogEntry] {
        &self.baseline_observation_schema_catalog
    }

    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.type_policy_allowances
    }

    fn source_materializations(&self) -> &[cdf_kernel::SourceMaterializationRule] {
        &self.source_materializations
    }

    fn validate_runtime_dependencies(&self) -> Result<()> {
        self.egress.authorize(&self.runtime.endpoint)
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        Ok(vec![self.partition(request)?])
    }

    fn rebind_scan_for_resume(
        &self,
        mut scan: ScanPlan,
        committed_frontier: &SourcePosition,
    ) -> Result<ScanPlan> {
        validate_resume_position(committed_frontier, &self.scope)?;
        let partitions = scan.inline_partitions_mut().ok_or_else(|| {
            CdfError::contract("MongoDB CDC resume requires one inline partition")
        })?;
        let [partition] = partitions.as_mut_slice() else {
            return Err(CdfError::contract(
                "MongoDB CDC resume requires exactly one partition",
            ));
        };
        partition.start_position = Some(committed_frontier.clone());
        Ok(scan)
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.clone().open_owned(partition)
    }
}

impl QueryableResource for MongoDbCdcResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        Ok(ScanPlan::from_partition_authority(
            PlanId::new(format!("mongodb-cdc-{}", self.descriptor.resource_id))?,
            request.clone(),
            PartitionAuthority::Inline(vec![self.partition(request)?]),
            Vec::new(),
            Vec::new(),
            None,
            None,
            DeliveryGuarantee::EffectivelyOncePerPosition,
        ))
    }
}

async fn execute_change_stream(
    resource: MongoDbCdcResource,
    partition: PartitionPlan,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    mut sender: TaskStreamSender<Batch>,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<()> {
    if partition.partition_id.as_str() != MONGODB_CDC_PARTITION {
        return Err(CdfError::contract("MongoDB CDC partition identity changed"));
    }
    let resume = partition
        .start_position
        .as_ref()
        .map(|position| decode_resume_position(position, &resource.scope))
        .transpose()?;
    let handle = resource
        .client
        .get_or_try_init(|| {
            connect_mongodb(
                &resource.runtime,
                Arc::clone(&memory),
                &resource.egress,
                &cancellation,
            )
        })
        .await?;
    preflight_change_stream(&resource, handle, &cancellation).await?;
    let mut stream = open_change_stream(&resource, handle, resume, &cancellation).await?;
    let mut ordinal = 0_u64;
    if partition.start_position.is_none()
        && let Some(token) = stream.resume_token()
    {
        let position = encode_resume_position(
            &token,
            &resource.scope,
            cdf_kernel::MongoResumeTokenSource::PostBatch,
        )?;
        send_control(
            &resource,
            &partition,
            &memory,
            &mut sender,
            &mut ordinal,
            CdcSettlementBoundary::Begin,
            &position,
        )
        .await?;
        send_control(
            &resource,
            &partition,
            &memory,
            &mut sender,
            &mut ordinal,
            CdcSettlementBoundary::Terminal,
            &position,
        )
        .await?;
    }
    loop {
        cancellation.check()?;
        let event = cancellation
            .await_or_cancel(async {
                stream
                    .next_if_any()
                    .await
                    .map_err(|error| classify_mongodb_error("read MongoDB change stream", error))
            })
            .await?;
        let Some(event) = event else {
            continue;
        };
        publish_event(
            &resource,
            &partition,
            event,
            &memory,
            &mut sender,
            &mut ordinal,
        )
        .await?;
    }
}

async fn preflight_change_stream(
    resource: &MongoDbCdcResource,
    handle: &MongoDbClientHandle,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<()> {
    let database = handle.client.database(resource.database.as_str());
    if resource.watch == MongoDbWatch::Database {
        let mut current_inventory = cancellation
            .await_or_cancel(async {
                database.list_collection_names().await.map_err(|error| {
                    classify_mongodb_error("list MongoDB database collections", error)
                })
            })
            .await?
            .into_iter()
            .filter(|name| resource.admits_collection(name))
            .collect::<Vec<_>>();
        current_inventory.sort();
        current_inventory.dedup();
        if current_inventory != resource.admitted_collections {
            return Err(CdfError::data(
                "MongoDB database collection inventory differs from its compiled authority; run discovery and compile before resuming",
            ));
        }
    }
    let build_info = cancellation
        .await_or_cancel(async {
            database
                .run_command(mongodb::bson::doc! {"buildInfo": 1_i32})
                .await
                .map_err(|error| classify_mongodb_error("read MongoDB server version", error))
        })
        .await?;
    validate_server_version(&build_info)?;
    let hello = cancellation
        .await_or_cancel(async {
            database
                .run_command(mongodb::bson::doc! {"hello": 1_i32})
                .await
                .map_err(|error| classify_mongodb_error("read MongoDB topology", error))
        })
        .await?;
    if hello.get_str("setName").is_err() && hello.get_str("msg").ok() != Some("isdbgrid") {
        return Err(CdfError::contract(
            "MongoDB CDC requires a replica set or sharded cluster topology",
        ));
    }
    for collection in &resource.admitted_collections {
        let collection = MongoDbIdentifier::new(collection)?;
        let metadata = read_collection_metadata(&database, &collection, cancellation).await?;
        if !metadata.change_stream_pre_and_post_images() {
            return Err(CdfError::contract(format!(
                "MongoDB CDC requires changeStreamPreAndPostImages.enabled=true for collection `{collection}`"
            )));
        }
    }
    Ok(())
}

async fn open_change_stream(
    resource: &MongoDbCdcResource,
    handle: &MongoDbClientHandle,
    resume: Option<ResumeToken>,
    cancellation: &cdf_runtime::RunCancellation,
) -> Result<ChangeStream<ChangeStreamEvent<mongodb::bson::Document>>> {
    let database = handle.client.database(resource.database.as_str());
    let pipeline = runtime_change_pipeline(resource);
    let batch_size = resource.change_batch_rows;
    let max_await = Duration::from_millis(resource.change_max_await_ms);
    let comment = resource
        .change_comment
        .clone()
        .map(mongodb::bson::Bson::String);
    let read_concern = resource.read_concern.clone();
    let selection_criteria = resource.selection_criteria.clone();
    cancellation
        .await_or_cancel(async {
            let result = match resource.watch {
                MongoDbWatch::Collection => {
                    let collection = database.collection::<mongodb::bson::Document>(
                        resource
                            .collection
                            .as_ref()
                            .ok_or_else(|| CdfError::internal("collection watch lost collection"))?
                            .as_str(),
                    );
                    let watch = collection
                        .watch()
                        .pipeline(pipeline)
                        .full_document(FullDocumentType::Required)
                        .batch_size(batch_size)
                        .max_await_time(max_await)
                        .show_expanded_events(false);
                    let watch = match comment {
                        Some(value) => watch.comment(value),
                        None => watch,
                    };
                    let watch = match read_concern {
                        Some(value) => watch.read_concern(value),
                        None => watch,
                    };
                    let watch = match selection_criteria {
                        Some(value) => watch.selection_criteria(value),
                        None => watch,
                    };
                    watch.resume_after(resume).await
                }
                MongoDbWatch::Database => {
                    let watch = database
                        .watch()
                        .pipeline(pipeline)
                        .full_document(FullDocumentType::Required)
                        .batch_size(batch_size)
                        .max_await_time(max_await)
                        .show_expanded_events(true);
                    let watch = match comment {
                        Some(value) => watch.comment(value),
                        None => watch,
                    };
                    let watch = match read_concern {
                        Some(value) => watch.read_concern(value),
                        None => watch,
                    };
                    let watch = match selection_criteria {
                        Some(value) => watch.selection_criteria(value),
                        None => watch,
                    };
                    watch.resume_after(resume).await
                }
            };
            result.map_err(|error| classify_mongodb_error("open MongoDB change stream", error))
        })
        .await
}

fn runtime_change_pipeline(resource: &MongoDbCdcResource) -> Vec<mongodb::bson::Document> {
    if resource.watch == MongoDbWatch::Collection {
        return resource.change_pipeline.clone();
    }
    let admitted = resource
        .admitted_collections
        .iter()
        .cloned()
        .map(mongodb::bson::Bson::String)
        .collect::<Vec<_>>();
    let mut pipeline = vec![mongodb::bson::doc! {
        "$match": {
            "$or": [
                {
                    "operationType": {"$in": ["insert", "update", "replace", "delete"]},
                    "ns.coll": {"$in": admitted},
                },
                {"operationType": {"$nin": ["insert", "update", "replace", "delete"]}},
            ]
        }
    }];
    pipeline.extend(resource.change_pipeline.iter().cloned());
    pipeline
}

async fn publish_event(
    resource: &MongoDbCdcResource,
    partition: &PartitionPlan,
    event: ChangeStreamEvent<mongodb::bson::Document>,
    memory: &Arc<dyn cdf_memory::MemoryCoordinator>,
    sender: &mut TaskStreamSender<Batch>,
    ordinal: &mut u64,
) -> Result<()> {
    let position = encode_resume_position(
        &event.id,
        &resource.scope,
        cdf_kernel::MongoResumeTokenSource::Event,
    )?;
    let namespace = event
        .ns
        .as_ref()
        .ok_or_else(|| CdfError::data("MongoDB change event omitted its namespace"))?;
    let collection = namespace
        .coll
        .as_deref()
        .ok_or_else(|| CdfError::data("MongoDB change event omitted its collection namespace"))?;
    if namespace.db != resource.database.as_str()
        || resource
            .admitted_collections
            .binary_search_by(|name| name.as_str().cmp(collection))
            .is_err()
    {
        return Err(CdfError::data(format!(
            "MongoDB change event selected uncompiled collection `{}.{collection}`; run discovery and compile before resuming",
            namespace.db
        )));
    }
    let operation = match event.operation_type {
        OperationType::Insert => CdcOperation::Insert,
        OperationType::Update | OperationType::Replace => CdcOperation::Update,
        OperationType::Delete => CdcOperation::Delete,
        other => {
            return Err(CdfError::data(format!(
                "MongoDB change stream emitted unsupported operation {other:?}; no resume token was advanced"
            )));
        }
    };
    send_control(
        resource,
        partition,
        memory,
        sender,
        ordinal,
        CdcSettlementBoundary::Begin,
        &position,
    )
    .await?;
    let decoded = match (resource.representation, operation) {
        (MongoDbRepresentation::Envelope, CdcOperation::Delete) => MongoDbCdcEventBatch::exact(
            envelope_delete_batch(resource, collection, event.document_key.as_ref())?,
            None,
        ),
        (MongoDbRepresentation::Envelope, _) => MongoDbCdcEventBatch::exact(
            envelope_upsert_batch(
                resource,
                collection,
                event.document_key.as_ref(),
                event.full_document.as_ref(),
            )?,
            resource.physical_schema.clone(),
        ),
        (MongoDbRepresentation::Typed, CdcOperation::Delete) => MongoDbCdcEventBatch::exact(
            typed_delete_batch(resource, collection, event.document_key.as_ref())?,
            None,
        ),
        (MongoDbRepresentation::Typed, _) => {
            typed_upsert_batch(resource, collection, event.full_document.as_ref())?
        }
    };
    let record_batch = decoded.record_batch;
    let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
    let lease = reserve(
        Arc::clone(memory),
        ReservationRequest::new(
            ConsumerKey::new("mongodb-cdc-event", MemoryClass::Decode)?,
            MONGODB_MAXIMUM_DECODE_BYTES,
        )?,
    )
    .await?;
    *ordinal = ordinal.saturating_add(1);
    let observed_schema = decoded
        .observed_schema
        .unwrap_or_else(|| record_batch.schema());
    let mut batch = Batch::from_record_batch(
        event_batch_id(resource, *ordinal, "data")?,
        resource.descriptor.resource_id.clone(),
        partition.partition_id.clone(),
        cdf_kernel::canonical_arrow_schema_hash(observed_schema.as_ref())?,
        record_batch,
    )?;
    if operation != CdcOperation::Delete {
        batch
            .header
            .mark_materialized_output(observed_schema.as_ref())?;
    }
    batch
        .header
        .extend_residual_candidates(decoded.residual_candidates);
    batch
        .header
        .extend_physical_reconciliations(decoded.physical_reconciliations);
    if decoded.residuals_complete {
        batch.header.mark_materialized_residuals_complete();
    }
    if let Some(observation_id) = resource.observation_ids.get(collection) {
        batch
            .header
            .bind_schema_observation(observation_id.clone())?;
    }
    batch.header.source_position = Some(position.clone());
    batch.header.cdc = Some(CdcMetadata {
        operation,
        position: position.clone(),
    });
    let evidence_bytes = decoded
        .pre_contract_evidence_bytes
        .max(batch.header.pre_contract_evidence_retained_bytes()?);
    let retained_total = retained_bytes.checked_add(evidence_bytes).ok_or_else(|| {
        CdfError::internal("MongoDB CDC event retained-memory accounting overflow")
    })?;
    if retained_bytes == 0 || retained_total > MONGODB_MAXIMUM_OUTPUT_BATCH_BYTES {
        return Err(CdfError::data(format!(
            "MongoDB CDC Arrow event and drift evidence retain {retained_total} bytes outside the compiled 1..={MONGODB_MAXIMUM_OUTPUT_BATCH_BYTES}-byte bound"
        )));
    }
    lease.reconcile(retained_total)?;
    let batch = batch.with_retention(PayloadRetention::new(Arc::new(lease), retained_total)?)?;
    sender.send(batch).await?;
    send_control(
        resource,
        partition,
        memory,
        sender,
        ordinal,
        CdcSettlementBoundary::Terminal,
        &position,
    )
    .await
}

async fn send_control(
    resource: &MongoDbCdcResource,
    partition: &PartitionPlan,
    memory: &Arc<dyn cdf_memory::MemoryCoordinator>,
    sender: &mut TaskStreamSender<Batch>,
    ordinal: &mut u64,
    boundary: CdcSettlementBoundary,
    position: &SourcePosition,
) -> Result<()> {
    *ordinal = ordinal.saturating_add(1);
    let record_batch = RecordBatch::new_empty(Arc::clone(&resource.schema));
    let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
    let physical_schema = resource
        .physical_schema
        .clone()
        .unwrap_or_else(|| Arc::clone(&resource.schema));
    let mut batch = Batch::from_record_batch(
        event_batch_id(resource, *ordinal, "settlement")?,
        resource.descriptor.resource_id.clone(),
        partition.partition_id.clone(),
        cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref())?,
        record_batch,
    )?;
    if resource.physical_schema.is_some() {
        batch
            .header
            .mark_materialized_output(physical_schema.as_ref())?;
    }
    batch.header.byte_count = 0;
    batch.header.source_position = Some(position.clone());
    batch.header.cdc_settlement = Some(CdcSettlementMarker {
        unit_kind: CdcSettlementUnitKind::EventPrefix,
        boundary,
        position: position.clone(),
    });
    if retained_bytes != 0 {
        let lease = reserve(
            Arc::clone(memory),
            ReservationRequest::new(
                ConsumerKey::new("mongodb-cdc-settlement", MemoryClass::Control)?,
                retained_bytes,
            )?,
        )
        .await?;
        batch = batch.with_retention(PayloadRetention::new(Arc::new(lease), retained_bytes)?)?;
    }
    sender.send(batch).await
}

fn envelope_upsert_batch(
    resource: &MongoDbCdcResource,
    collection: &str,
    document_key: Option<&mongodb::bson::Document>,
    document: Option<&mongodb::bson::Document>,
) -> Result<RecordBatch> {
    let key = canonical_extjson(
        document_key.ok_or_else(|| CdfError::data("MongoDB change event omitted documentKey"))?,
    )?;
    let document = canonical_extjson(
        document
            .ok_or_else(|| CdfError::data("MongoDB CDC event omitted required fullDocument"))?,
    )?;
    RecordBatch::try_new(
        Arc::clone(&resource.schema),
        vec![
            Arc::new(StringArray::from(vec![resource.database.as_str()])) as ArrayRef,
            Arc::new(StringArray::from(vec![collection])) as ArrayRef,
            Arc::new(StringArray::from(vec![key])) as ArrayRef,
            Arc::new(StringArray::from(vec![document])) as ArrayRef,
        ],
    )
    .map_err(CdfError::from)
}

fn envelope_delete_batch(
    resource: &MongoDbCdcResource,
    collection: &str,
    document_key: Option<&mongodb::bson::Document>,
) -> Result<RecordBatch> {
    envelope_delete_record_batch(&resource.schema, collection, document_key)
}

fn envelope_delete_record_batch(
    output_schema: &SchemaRef,
    collection: &str,
    document_key: Option<&mongodb::bson::Document>,
) -> Result<RecordBatch> {
    let key = canonical_extjson(
        document_key.ok_or_else(|| CdfError::data("MongoDB delete event omitted documentKey"))?,
    )?;
    let schema = Arc::new(Schema::new(vec![
        output_schema.field_with_name("document_key")?.clone(),
        output_schema.field_with_name("source_collection")?.clone(),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![key])) as ArrayRef,
            Arc::new(StringArray::from(vec![collection])) as ArrayRef,
        ],
    )
    .map_err(CdfError::from)
}

fn typed_upsert_batch(
    resource: &MongoDbCdcResource,
    collection: &str,
    document: Option<&mongodb::bson::Document>,
) -> Result<MongoDbCdcEventBatch> {
    let document = document
        .ok_or_else(|| CdfError::data("MongoDB CDC event omitted required fullDocument"))?;
    let raw = mongodb::bson::serialize_to_raw_document_buf(document).map_err(|error| {
        CdfError::data(format!(
            "MongoDB CDC fullDocument could not be decoded: {error}"
        ))
    })?;
    let route = (resource.watch == MongoDbWatch::Database)
        .then(|| resource.typed_route(collection))
        .transpose()?;
    let decoder = route
        .map(|route| &route.decoder_schema)
        .or(resource.decoder_schema.as_ref())
        .ok_or_else(|| CdfError::internal("typed CDC omitted decoder schema"))?;
    let physical = route
        .map(|route| &route.document_physical_schema)
        .or(resource.physical_schema.as_ref())
        .ok_or_else(|| CdfError::internal("typed CDC omitted physical schema"))?;
    let effective = route
        .map(|route| &route.document_effective_schema)
        .unwrap_or(&resource.schema);
    let decoded = decode_batch_with_physical_schema(
        Arc::clone(decoder),
        Arc::clone(decoder),
        Arc::clone(effective),
        Arc::clone(physical),
        &[raw.as_ref()],
        0,
    )?;
    let observed_schema = route
        .map(|route| Arc::clone(&route.physical_schema))
        .or_else(|| resource.physical_schema.clone());
    let record_batch = match route {
        Some(route) => append_collection_route(
            decoded.record_batch,
            Arc::clone(&route.effective_schema),
            collection,
        )?,
        None => decoded.record_batch,
    };
    Ok(MongoDbCdcEventBatch {
        record_batch,
        observed_schema,
        residual_candidates: decoded.residual_candidates,
        physical_reconciliations: decoded.physical_reconciliations,
        pre_contract_evidence_bytes: decoded.pre_contract_evidence_bytes,
        residuals_complete: true,
    })
}

fn typed_delete_batch(
    resource: &MongoDbCdcResource,
    collection: &str,
    document_key: Option<&mongodb::bson::Document>,
) -> Result<RecordBatch> {
    let document_key =
        document_key.ok_or_else(|| CdfError::data("MongoDB delete event omitted documentKey"))?;
    let route = (resource.watch == MongoDbWatch::Database)
        .then(|| resource.typed_route(collection))
        .transpose()?;
    let effective = route
        .map(|route| &route.document_effective_schema)
        .unwrap_or(&resource.schema);
    let physical_authority = route
        .map(|route| &route.document_physical_schema)
        .or(resource.physical_schema.as_ref())
        .ok_or_else(|| CdfError::internal("typed CDC omitted physical schema"))?;
    let key_schema = project_schema(effective, &resource.descriptor.merge_key)?;
    let physical = project_physical_schema(
        effective,
        physical_authority,
        &resource.descriptor.merge_key,
    )?;
    let decoder = attach_expected_physical_types(key_schema.as_ref(), physical.as_ref())?;
    let raw = mongodb::bson::serialize_to_raw_document_buf(document_key).map_err(|error| {
        CdfError::data(format!(
            "MongoDB CDC documentKey could not be decoded: {error}"
        ))
    })?;
    let decoded = decode_batch_with_physical_schema(
        Arc::clone(&decoder),
        decoder,
        Arc::clone(&key_schema),
        physical,
        &[raw.as_ref()],
        0,
    )?;
    if !decoded.residual_candidates.is_empty()
        || !decoded.physical_reconciliations.is_empty()
        || decoded
            .record_batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(CdfError::data(
            "MongoDB CDC documentKey does not exactly satisfy the compiled CDC_APPLY key",
        ));
    }
    let decoded = RecordBatch::try_new(key_schema, decoded.record_batch.columns().to_vec())
        .map_err(CdfError::from)?;
    match route {
        Some(route) => {
            let output_fields = resource
                .descriptor
                .merge_key
                .iter()
                .map(|field| route.effective_schema.field_with_name(field).cloned())
                .chain(std::iter::once(
                    route
                        .effective_schema
                        .field_with_name("source_collection")
                        .cloned(),
                ))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(CdfError::from)?;
            append_collection_route(decoded, Arc::new(Schema::new(output_fields)), collection)
        }
        None => Ok(decoded),
    }
}

fn append_collection_route(
    batch: RecordBatch,
    output_schema: SchemaRef,
    collection: &str,
) -> Result<RecordBatch> {
    if output_schema.fields().len() != batch.num_columns().saturating_add(1)
        || output_schema
            .fields()
            .last()
            .is_none_or(|field| field.name() != "source_collection")
    {
        return Err(CdfError::internal(
            "typed MongoDB routed output schema does not append its collection authority",
        ));
    }
    let mut columns = batch.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![collection])) as ArrayRef);
    RecordBatch::try_new(output_schema, columns).map_err(CdfError::from)
}

fn project_schema(schema: &SchemaRef, fields: &[String]) -> Result<SchemaRef> {
    let projected = fields
        .iter()
        .map(|name| {
            schema
                .field_with_name(name)
                .cloned()
                .map_err(|_| CdfError::contract(format!("MongoDB CDC key `{name}` is absent")))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(projected)))
}

fn project_physical_schema(
    effective_schema: &SchemaRef,
    physical_schema: &SchemaRef,
    fields: &[String],
) -> Result<SchemaRef> {
    let projected = fields
        .iter()
        .map(|name| {
            let effective = effective_schema
                .field_with_name(name)
                .map_err(|_| CdfError::contract(format!("MongoDB CDC key `{name}` is absent")))?;
            let source = source_name(effective).unwrap_or_else(|| effective.name());
            physical_schema
                .fields()
                .iter()
                .find(|field| source_name(field).unwrap_or_else(|| field.name()) == source)
                .cloned()
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "MongoDB CDC physical schema omitted key source field `{source}`"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(projected)))
}

fn canonical_extjson(document: &mongodb::bson::Document) -> Result<String> {
    serde_json::to_string(&mongodb::bson::Bson::Document(document.clone()).into_canonical_extjson())
        .map_err(|error| CdfError::data(format!("encode MongoDB Canonical Extended JSON: {error}")))
}

fn event_batch_id(resource: &MongoDbCdcResource, ordinal: u64, suffix: &str) -> Result<BatchId> {
    BatchId::new(format!(
        "{}-mongodb-cdc-{ordinal:012}-{suffix}",
        resource
            .descriptor
            .resource_id
            .as_str()
            .chars()
            .map(|character| if character.is_ascii_alphanumeric() {
                character
            } else {
                '-'
            })
            .collect::<String>()
    ))
}

fn encode_resume_position(
    token: &ResumeToken,
    scope: &cdf_kernel::MongoChangeStreamScope,
    source: cdf_kernel::MongoResumeTokenSource,
) -> Result<SourcePosition> {
    let bytes = mongodb::bson::serialize_to_vec(token).map_err(|error| {
        CdfError::data(format!("encode MongoDB resume token authority: {error}"))
    })?;
    let position =
        SourcePosition::resume_token(cdf_kernel::ResumeTokenPosition::MongoChangeStream(
            cdf_kernel::MongoChangeStreamResumeToken {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                scope: scope.clone(),
                token_bson_base64: STANDARD.encode(&bytes),
                token_sha256: format!("sha256:{:x}", Sha256::digest(&bytes)),
                resume_mode: cdf_kernel::MongoResumeMode::ResumeAfter,
                token_source: source,
            },
        ));
    position.validate()?;
    Ok(position)
}

fn validate_resume_position(
    position: &SourcePosition,
    scope: &cdf_kernel::MongoChangeStreamScope,
) -> Result<()> {
    let SourcePosition::ResumeToken(position) = position else {
        return Err(CdfError::contract(
            "MongoDB CDC can resume only from a MongoDB change-stream token",
        ));
    };
    let cdf_kernel::ResumeTokenPosition::MongoChangeStream(position) = position.as_ref();
    if &position.scope != scope {
        return Err(CdfError::data(
            "MongoDB resume token scope differs from the compiled source, watch, pipeline, or options authority",
        ));
    }
    Ok(())
}

fn decode_resume_position(
    position: &SourcePosition,
    scope: &cdf_kernel::MongoChangeStreamScope,
) -> Result<ResumeToken> {
    validate_resume_position(position, scope)?;
    let SourcePosition::ResumeToken(position) = position else {
        unreachable!("validated resume token")
    };
    let cdf_kernel::ResumeTokenPosition::MongoChangeStream(position) = position.as_ref();
    let bytes = STANDARD
        .decode(&position.token_bson_base64)
        .map_err(|error| {
            CdfError::data(format!("decode MongoDB resume token authority: {error}"))
        })?;
    mongodb::bson::deserialize_from_slice(&bytes)
        .map_err(|error| CdfError::data(format!("decode MongoDB resume token BSON: {error}")))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field};

    use super::*;

    fn scope() -> cdf_kernel::MongoChangeStreamScope {
        cdf_kernel::MongoChangeStreamScope {
            source_binding: "mongo".to_owned(),
            watch_level: cdf_kernel::MongoWatchLevel::Database,
            database: Some("analytics".to_owned()),
            collection: None,
            pipeline_sha256: format!("sha256:{}", "a".repeat(64)),
            options_sha256: format!("sha256:{}", "b".repeat(64)),
        }
    }

    #[test]
    fn resume_token_round_trip_preserves_exact_bson_and_scope() {
        let token: ResumeToken =
            mongodb::bson::deserialize_from_document(mongodb::bson::doc! {"_data": "826789abcdef"})
                .unwrap();
        let position =
            encode_resume_position(&token, &scope(), cdf_kernel::MongoResumeTokenSource::Event)
                .unwrap();
        let decoded = decode_resume_position(&position, &scope()).unwrap();
        assert_eq!(decoded, token);
        assert!(
            decode_resume_position(
                &position,
                &cdf_kernel::MongoChangeStreamScope {
                    source_binding: "other".to_owned(),
                    ..scope()
                }
            )
            .is_err()
        );
    }

    #[test]
    fn envelope_json_is_canonical_and_delete_keeps_route_authority() {
        let document = mongodb::bson::doc! {
            "_id": mongodb::bson::oid::ObjectId::parse_str("507f1f77bcf86cd799439011").unwrap(),
            "amount": mongodb::bson::Decimal128::from_str("12.30").unwrap(),
        };
        let encoded = canonical_extjson(&document).unwrap();
        assert!(encoded.contains("$oid"));
        assert!(encoded.contains("$numberDecimal"));

        let delete = envelope_delete_record_batch(
            &crate::driver::mongodb_envelope_schema(),
            "orders",
            Some(&mongodb::bson::doc! {"_id": 1_i32}),
        )
        .unwrap();
        assert_eq!(delete.schema().fields().len(), 2);
        assert_eq!(delete.schema().field(0).name(), "document_key");
        assert_eq!(delete.schema().field(1).name(), "source_collection");
    }

    #[test]
    fn typed_delete_maps_logical_key_to_its_mongodb_source_field() {
        let effective = Arc::new(Schema::new(vec![cdf_kernel::with_source_name(
            Field::new("id", DataType::Utf8, false),
            "_id",
        )]));
        let physical = Arc::new(Schema::new(vec![cdf_kernel::with_physical_type(
            cdf_kernel::with_source_name(Field::new("_id", DataType::Utf8, false), "_id"),
            "bson:string",
        )]));
        let keys = vec!["id".to_owned()];
        let key_schema = project_schema(&effective, &keys).unwrap();
        let physical_key = project_physical_schema(&effective, &physical, &keys).unwrap();
        let decoder =
            attach_expected_physical_types(key_schema.as_ref(), physical_key.as_ref()).unwrap();
        let raw =
            mongodb::bson::serialize_to_raw_document_buf(&mongodb::bson::doc! {"_id": "order-1"})
                .unwrap();
        let decoded = decode_batch_with_physical_schema(
            Arc::clone(&decoder),
            decoder,
            key_schema,
            physical_key,
            &[raw.as_ref()],
            0,
        )
        .unwrap();

        assert!(decoded.residual_candidates.is_empty());
        assert!(decoded.physical_reconciliations.is_empty());
        assert_eq!(decoded.record_batch.schema().field(0).name(), "id");
        assert_eq!(
            decoded
                .record_batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "order-1"
        );
    }
}
