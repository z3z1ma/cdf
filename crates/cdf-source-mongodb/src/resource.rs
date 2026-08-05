use std::{collections::BTreeMap, fmt, sync::Arc};

use arrow_schema::{DataType, SchemaRef, TimeUnit};
use cdf_kernel::{
    BackpressureSupport, BatchStream, CapabilitySupport, CdfError, CompiledScanIntent,
    CompiledSourcePlanHash, DeliveryGuarantee, EffectiveSchemaCatalogEntry, EffectiveSchemaRuntime,
    EstimateSupport, FilterCapabilities, IncrementalShape, PartitionAuthority, PartitionId,
    PartitionPlan, PartitioningCapabilities, PlanId, PushdownFidelity, PushedPredicate,
    QueryableResource, ReplaySupport, ResourceCapabilities, ResourceDescriptor, ResourceStream,
    Result, ScanPlan, ScanPredicate, ScanRequest, ScopeKind, source_name,
};
use cdf_runtime::{ExecutionServices, SourceEgressScope};

use crate::{
    driver::MongoDbRuntimeConfig,
    execution::{MongoDbClientHandle, MongoDbExecutionInput, execute_mongodb_collection},
    identifier::MongoDbIdentifier,
    query::{MONGODB_SOURCE_KIND, predicate_fidelity, scan_from_partition},
    schema::validate_mongodb_schema,
};

#[derive(Clone)]
pub(crate) struct MongoDbCollectionResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    endpoint: String,
    database: MongoDbIdentifier,
    collection: MongoDbIdentifier,
    batch_rows: u32,
    stream_buffer_batches: usize,
    runtime: MongoDbRuntimeConfig,
    client: Arc<tokio::sync::OnceCell<MongoDbClientHandle>>,
    capabilities: ResourceCapabilities,
    execution: Option<ExecutionServices>,
    egress: SourceEgressScope,
    type_policy_allowances: cdf_kernel::TypePolicyAllowances,
    compiled_source_plan_hash: Option<CompiledSourcePlanHash>,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
}

impl MongoDbCollectionResource {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_compiled_plan(
        compiled: &cdf_runtime::CompiledSourcePlan,
        endpoint: String,
        database: MongoDbIdentifier,
        collection: MongoDbIdentifier,
        batch_rows: u32,
        stream_buffer_batches: usize,
        runtime: MongoDbRuntimeConfig,
        egress: SourceEgressScope,
        execution: ExecutionServices,
    ) -> Result<Self> {
        let schema = Arc::new(compiled.schema.clone());
        validate_resource_shape(&compiled.descriptor, &schema, &collection)?;
        validate_compiled_schema_evidence(compiled)?;
        if !(1..=100_000).contains(&batch_rows) || !(1..=16).contains(&stream_buffer_batches) {
            return Err(CdfError::contract(
                "MongoDB batch_rows must be 1..=100000 and stream_buffer_batches must be 1..=16",
            ));
        }
        Ok(Self {
            descriptor: compiled.descriptor.clone(),
            schema,
            endpoint,
            database,
            collection,
            batch_rows,
            stream_buffer_batches,
            runtime,
            client: Arc::new(tokio::sync::OnceCell::new()),
            capabilities: mongodb_collection_capabilities(&compiled.descriptor),
            execution: Some(execution),
            egress,
            type_policy_allowances: compiled.type_policy_allowances,
            compiled_source_plan_hash: Some(compiled.compiled_source_plan_hash()?),
            effective_schema_runtime: compiled.effective_schema_runtime.clone(),
            baseline_observation_schema_catalog: compiled
                .baseline_observation_schema_catalog
                .clone(),
        })
    }

    fn runtime_schema_observation_id(&self) -> String {
        format!("runtime:{}.{}", self.database, self.collection)
    }

    fn open_owned(self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'static> {
        let Some(execution) = self.execution else {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
                Err(CdfError::contract(
                    "MongoDB source execution requires injected execution services",
                ))
            }));
        };
        if let Err(error) = validate_resource_shape(
            &self.descriptor,
            &self.schema,
            &self.collection,
        )
        .and_then(|()| {
            scan_from_partition(&self.descriptor, &self.schema, &self.collection, &partition)
                .map(|_| ())
        }) {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(
                async move { Err(error) },
            ));
        }
        let memory = execution.memory();
        let task = match execution.spawn_io_stream(
            "mongodb-source-open",
            self.stream_buffer_batches,
            move |sender, cancellation| async move {
                execute_mongodb_collection(
                    MongoDbExecutionInput {
                        runtime: self.runtime,
                        client: self.client,
                        descriptor: self.descriptor,
                        schema: self.schema,
                        database: self.database,
                        collection: self.collection,
                        batch_rows: self.batch_rows,
                        partition,
                        memory,
                        egress: self.egress,
                    },
                    sender,
                    cancellation,
                )
                .await
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

impl fmt::Debug for MongoDbCollectionResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MongoDbCollectionResource")
            .field("descriptor", &self.descriptor)
            .field("schema", &self.schema)
            .field("endpoint", &self.endpoint)
            .field("database", &self.database)
            .field("collection", &self.collection)
            .field("batch_rows", &self.batch_rows)
            .field("stream_buffer_batches", &self.stream_buffer_batches)
            .field("managed_execution", &self.execution.is_some())
            .finish_non_exhaustive()
    }
}

impl ResourceStream for MongoDbCollectionResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn compiled_source_plan_hash(&self) -> Option<&CompiledSourcePlanHash> {
        self.compiled_source_plan_hash.as_ref()
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

    fn validate_runtime_dependencies(&self) -> Result<()> {
        if self.execution.is_none() {
            return Err(CdfError::contract(
                "MongoDB source execution requires injected execution services",
            ));
        }
        self.egress.authorize(&self.endpoint)
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        let mut partition =
            plan_mongodb_partition(&self.descriptor, &self.schema, &self.collection, request)?;
        partition.scan_intent = CompiledScanIntent::full_scan();
        if self.effective_schema_runtime.is_some() {
            cdf_kernel::bind_partition_schema_candidate(
                &mut partition,
                &self.runtime_schema_observation_id(),
            )?;
        }
        Ok(vec![partition])
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.clone().open_owned(partition)
    }
}

impl QueryableResource for MongoDbCollectionResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        let mut scan =
            negotiate_mongodb_scan(&self.descriptor, &self.schema, &self.collection, request)?;
        if self.effective_schema_runtime.is_some() {
            let partition = scan
                .inline_partitions_mut()
                .and_then(|partitions| partitions.first_mut())
                .ok_or_else(|| {
                    CdfError::internal("MongoDB negotiation omitted its inline partition")
                })?;
            cdf_kernel::bind_partition_schema_candidate(
                partition,
                &self.runtime_schema_observation_id(),
            )?;
        }
        Ok(scan)
    }
}

pub(crate) fn validate_compiled_schema_evidence(
    compiled: &cdf_runtime::CompiledSourcePlan,
) -> Result<()> {
    if compiled.effective_schema_runtime.is_none()
        && compiled.baseline_observation_schema_catalog.is_empty()
    {
        return Err(CdfError::data(
            "MongoDB execution requires sampled physical schema evidence; run schema discovery and pin the resource before resolving it",
        ));
    }
    Ok(())
}

pub(crate) fn validate_resource_shape(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    collection: &MongoDbIdentifier,
) -> Result<()> {
    let _ = collection;
    validate_mongodb_schema(schema.as_ref())?;
    if let Some(cursor) = &descriptor.cursor {
        let field = schema.field_with_name(&cursor.field).map_err(|_| {
            CdfError::contract(format!(
                "MongoDB cursor field `{}` is absent from the pinned schema",
                cursor.field
            ))
        })?;
        if field.is_nullable()
            || !matches!(
                field.data_type(),
                DataType::Int32
                    | DataType::Int64
                    | DataType::Date32
                    | DataType::Timestamp(TimeUnit::Millisecond, _)
            )
        {
            return Err(CdfError::contract(
                "MongoDB cursor must be a non-null Int32, Int64, Date32, or millisecond Timestamp field",
            ));
        }
        if source_name(field).unwrap_or_else(|| field.name()) == "_id" {
            return Err(CdfError::contract(
                "MongoDB ObjectId/_id cursor positions are not supported; configure a separate ordered numeric/date/timestamp cursor",
            ));
        }
        let id = schema
            .fields()
            .iter()
            .map(AsRef::as_ref)
            .find(|field| source_name(field).unwrap_or_else(|| field.name()) == "_id")
            .ok_or_else(|| {
                CdfError::contract("MongoDB cursor resources require the `_id` stable key")
            })?;
        if id.is_nullable() {
            return Err(CdfError::contract(
                "MongoDB `_id` stable key must be non-null",
            ));
        }
    }
    Ok(())
}

pub(crate) fn mongodb_collection_capabilities(
    descriptor: &ResourceDescriptor,
) -> ResourceCapabilities {
    ResourceCapabilities {
        projection: CapabilitySupport::Supported,
        filters: FilterCapabilities {
            default_fidelity: PushdownFidelity::Exact,
            supported_operators: vec![
                "=".to_owned(),
                ">".to_owned(),
                ">=".to_owned(),
                "<".to_owned(),
                "<=".to_owned(),
            ],
        },
        limits: if descriptor.cursor.is_some() {
            CapabilitySupport::Unsupported
        } else {
            CapabilitySupport::Supported
        },
        ordering: CapabilitySupport::Supported,
        partitioning: match descriptor.state_scope.kind() {
            ScopeKind::Resource => PartitioningCapabilities::default(),
            kind => PartitioningCapabilities {
                parallel_partitions: false,
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
        estimates: EstimateSupport::Rows,
    }
}

fn classify_predicates(
    schema: &SchemaRef,
    predicates: &[ScanPredicate],
) -> (Vec<PushedPredicate>, Vec<ScanPredicate>) {
    let mut pushed = Vec::new();
    let mut unsupported = Vec::new();
    for predicate in predicates {
        match predicate_fidelity(schema, &predicate.canonical_expression) {
            PushdownFidelity::Exact => pushed.push(PushedPredicate {
                predicate: predicate.clone(),
                fidelity: PushdownFidelity::Exact,
            }),
            _ => unsupported.push(predicate.clone()),
        }
    }
    (pushed, unsupported)
}

fn negotiate_mongodb_scan(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    collection: &MongoDbIdentifier,
    request: &ScanRequest,
) -> Result<ScanPlan> {
    validate_request(descriptor, schema, collection, request)?;
    let (pushed, unsupported) = classify_predicates(schema, &request.filters);
    Ok(ScanPlan::from_partition_authority(
        PlanId::new(format!("mongodb-scan-{}", descriptor.resource_id))?,
        request.clone(),
        PartitionAuthority::Inline(vec![plan_mongodb_partition(
            descriptor, schema, collection, request,
        )?]),
        pushed,
        unsupported,
        None,
        None,
        delivery_guarantee(descriptor),
    ))
}

fn plan_mongodb_partition(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    collection: &MongoDbIdentifier,
    request: &ScanRequest,
) -> Result<PartitionPlan> {
    validate_request(descriptor, schema, collection, request)?;
    let (predicates, _) = classify_predicates(schema, &request.filters);
    let scan_intent = CompiledScanIntent {
        version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
        projection: request.projection.clone(),
        predicates,
        limit: descriptor
            .cursor
            .is_none()
            .then_some(request.limit)
            .flatten(),
        order_by: request.order_by.clone(),
    };
    scan_intent.validate()?;
    let partition = PartitionPlan {
        partition_id: PartitionId::new(MONGODB_SOURCE_KIND)?,
        scope: descriptor.state_scope.clone(),
        planned_position: None,
        start_position: None,
        scan_intent,
        retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
        metadata: BTreeMap::from([
            ("kind".to_owned(), MONGODB_SOURCE_KIND.to_owned()),
            ("collection".to_owned(), collection.as_str().to_owned()),
            ("resource_id".to_owned(), descriptor.resource_id.to_string()),
        ]),
    };
    scan_from_partition(descriptor, schema, collection, &partition)?;
    Ok(partition)
}

fn validate_request(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    collection: &MongoDbIdentifier,
    request: &ScanRequest,
) -> Result<()> {
    if request.resource_id != descriptor.resource_id {
        return Err(CdfError::contract(format!(
            "scan resource `{}` does not match MongoDB resource `{}`",
            request.resource_id, descriptor.resource_id
        )));
    }
    validate_resource_shape(descriptor, schema, collection)
}

fn delivery_guarantee(descriptor: &ResourceDescriptor) -> DeliveryGuarantee {
    match descriptor.write_disposition {
        cdf_kernel::WriteDisposition::Merge if !descriptor.primary_key.is_empty() => {
            DeliveryGuarantee::EffectivelyOncePerKey
        }
        cdf_kernel::WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        _ => DeliveryGuarantee::AtLeastOnceDuplicateRisk,
    }
}
