use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{Schema, SchemaRef};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use cdf_kernel::{
    Batch, BatchId, BatchStream, CdcMetadata, CdcOperation, CdcSettlementBoundary,
    CdcSettlementMarker, CdcSettlementUnitKind, CdfError, CompiledScanIntent,
    CompiledSourcePlanHash, DeliveryGuarantee, EffectiveSchemaCatalogEntry, EffectiveSchemaRuntime,
    PartitionAuthority, PartitionId, PartitionPlan, PayloadRetention, PlanId, QueryableResource,
    ResourceCapabilities, ResourceDescriptor, ResourceStream, Result, RoutePlan, RouteScalar,
    ScanPlan, ScanRequest, SourcePosition, bind_partition_schema_observation,
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
        if watch == MongoDbWatch::Database && representation == MongoDbRepresentation::Typed {
            return Err(CdfError::contract(
                "typed MongoDB database CDC requires heterogeneous routed output plans",
            ));
        }
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
        Ok(Self {
            descriptor: compiled.descriptor.clone(),
            schema,
            decoder_schema,
            physical_schema,
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
        if let Some(runtime) = &self.effective_schema_runtime {
            let observation_id = match self.watch {
                MongoDbWatch::Collection => format!(
                    "{}.{}",
                    self.database,
                    self.collection.as_ref().ok_or_else(|| {
                        CdfError::internal("MongoDB collection CDC lost its collection")
                    })?
                ),
                MongoDbWatch::Database => runtime
                    .evidence
                    .observations()
                    .first()
                    .ok_or_else(|| {
                        CdfError::data("MongoDB database CDC discovery evidence is empty")
                    })?
                    .observation_id
                    .clone(),
            };
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
    let record_batch = match (resource.representation, operation) {
        (MongoDbRepresentation::Envelope, CdcOperation::Delete) => {
            envelope_delete_batch(resource, collection, event.document_key.as_ref())?
        }
        (MongoDbRepresentation::Envelope, _) => envelope_upsert_batch(
            resource,
            collection,
            event.document_key.as_ref(),
            event.full_document.as_ref(),
        )?,
        (MongoDbRepresentation::Typed, CdcOperation::Delete) => {
            typed_delete_batch(resource, event.document_key.as_ref())?
        }
        (MongoDbRepresentation::Typed, _) => {
            typed_upsert_batch(resource, event.full_document.as_ref())?
        }
    };
    let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
    if retained_bytes == 0 || retained_bytes > MONGODB_MAXIMUM_OUTPUT_BATCH_BYTES {
        return Err(CdfError::data(format!(
            "MongoDB CDC Arrow event retains {retained_bytes} bytes outside the compiled output bound"
        )));
    }
    let lease = reserve(
        Arc::clone(memory),
        ReservationRequest::new(
            ConsumerKey::new("mongodb-cdc-event", MemoryClass::Decode)?,
            MONGODB_MAXIMUM_DECODE_BYTES,
        )?,
    )
    .await?;
    lease.reconcile(retained_bytes)?;
    *ordinal = ordinal.saturating_add(1);
    let observed_schema = match operation {
        CdcOperation::Delete => record_batch.schema(),
        CdcOperation::Insert | CdcOperation::Update => Arc::clone(
            resource
                .physical_schema
                .as_ref()
                .ok_or_else(|| CdfError::internal("MongoDB CDC upsert omitted physical schema"))?,
        ),
    };
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
    batch.header.source_position = Some(position.clone());
    batch.header.cdc = Some(CdcMetadata {
        operation,
        position: position.clone(),
    });
    let batch = batch.with_retention(PayloadRetention::new(Arc::new(lease), retained_bytes)?)?;
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
        .as_ref()
        .ok_or_else(|| CdfError::internal("MongoDB CDC settlement omitted physical schema"))?;
    let mut batch = Batch::from_record_batch(
        event_batch_id(resource, *ordinal, "settlement")?,
        resource.descriptor.resource_id.clone(),
        partition.partition_id.clone(),
        cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref())?,
        record_batch,
    )?;
    batch
        .header
        .mark_materialized_output(physical_schema.as_ref())?;
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
    document: Option<&mongodb::bson::Document>,
) -> Result<RecordBatch> {
    let document = document
        .ok_or_else(|| CdfError::data("MongoDB CDC event omitted required fullDocument"))?;
    let raw = mongodb::bson::serialize_to_raw_document_buf(document).map_err(|error| {
        CdfError::data(format!(
            "MongoDB CDC fullDocument could not be decoded: {error}"
        ))
    })?;
    let decoder = resource
        .decoder_schema
        .as_ref()
        .ok_or_else(|| CdfError::internal("typed CDC omitted decoder schema"))?;
    let physical = resource
        .physical_schema
        .as_ref()
        .ok_or_else(|| CdfError::internal("typed CDC omitted physical schema"))?;
    let decoded = decode_batch_with_physical_schema(
        Arc::clone(decoder),
        Arc::clone(decoder),
        Arc::clone(&resource.schema),
        Arc::clone(physical),
        &[raw.as_ref()],
        0,
    )?;
    if !decoded.residual_candidates.is_empty() || !decoded.physical_reconciliations.is_empty() {
        return Err(CdfError::data(
            "MongoDB CDC fullDocument differs from its compiled schema authority; discover and compile before resuming",
        ));
    }
    Ok(decoded.record_batch)
}

fn typed_delete_batch(
    resource: &MongoDbCdcResource,
    document_key: Option<&mongodb::bson::Document>,
) -> Result<RecordBatch> {
    let document_key =
        document_key.ok_or_else(|| CdfError::data("MongoDB delete event omitted documentKey"))?;
    let key_schema = project_schema(&resource.schema, &resource.descriptor.merge_key)?;
    let physical = project_schema(
        resource
            .physical_schema
            .as_ref()
            .ok_or_else(|| CdfError::internal("typed CDC omitted physical schema"))?,
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
    RecordBatch::try_new(key_schema, decoded.record_batch.columns().to_vec())
        .map_err(CdfError::from)
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
}
