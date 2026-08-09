use std::{collections::BTreeMap, fmt, sync::Arc};

use arrow_schema::{DataType, SchemaRef, TimeUnit};
use cdf_kernel::{
    BackpressureSupport, BatchStream, CapabilitySupport, CdfError, CompiledScanIntent,
    CompiledSourcePlanHash, DeliveryGuarantee, EffectiveSchemaCatalogEntry, EffectiveSchemaRuntime,
    EstimateSupport, FilterCapabilities, ForeignState, IncrementalShape,
    PLAN_PHYSICAL_SCHEMA_HASH_KEY, PartitionAttestation, PartitionAttestationAttempt,
    PartitionAuthority, PartitionId, PartitionPlan, PartitioningCapabilities, PlanId,
    PushdownFidelity, PushedPredicate, QueryableResource, ReplaySupport, ResourceCapabilities,
    ResourceDescriptor, ResourceStream, Result, ScanPlan, ScanPredicate, ScanRequest, SchemaHash,
    ScopeKind, SourcePosition, source_name,
};
use cdf_runtime::{ExecutionServices, SourceEgressScope, artifact_hash};

use crate::{
    driver::{MongoDbRuntimeConfig, read_collection_metadata},
    execution::{
        MONGODB_FULL_SCAN_COMPLETION_PROTOCOL, MongoDbClientHandle, MongoDbExecutionInput,
        execute_mongodb_collection,
    },
    identifier::MongoDbIdentifier,
    query::{MONGODB_SOURCE_KIND, predicate_fidelity, scan_from_partition},
    schema::{attach_expected_physical_types, validate_mongodb_schema},
};

pub(crate) const MONGODB_COLLECTION_GENERATION_PROTOCOL: &str = "mongodb.collection_generation.v1";
const MONGODB_COLLECTION_GENERATION_SCHEMA_KEY: &str = "cdf:mongodb_collection_generation_sha256";

#[derive(Clone)]
pub(crate) struct MongoDbCollectionResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    decoder_schema: SchemaRef,
    physical_schema: SchemaRef,
    endpoint: String,
    database: MongoDbIdentifier,
    collection: MongoDbIdentifier,
    collection_generation: SourcePosition,
    batch_rows: u32,
    stream_buffer_batches: usize,
    runtime: MongoDbRuntimeConfig,
    client: Arc<tokio::sync::OnceCell<MongoDbClientHandle>>,
    capabilities: ResourceCapabilities,
    execution: Option<ExecutionServices>,
    egress: SourceEgressScope,
    type_policy_allowances: cdf_kernel::TypePolicyAllowances,
    source_materializations: Vec<cdf_kernel::SourceMaterializationRule>,
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
        let observed_schema = current_physical_schema(compiled)?;
        let collection_generation = collection_generation_from_schema(
            &compiled.descriptor,
            observed_schema.as_ref(),
            &database,
            &collection,
        )?;
        let decoder_schema = attach_expected_physical_types(&schema, observed_schema.as_ref())?;
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
            decoder_schema,
            physical_schema: observed_schema,
            endpoint,
            database,
            collection,
            collection_generation,
            batch_rows,
            stream_buffer_batches,
            runtime,
            client: Arc::new(tokio::sync::OnceCell::new()),
            capabilities: mongodb_collection_capabilities(&compiled.descriptor),
            execution: Some(execution),
            egress,
            type_policy_allowances: compiled.type_policy_allowances,
            source_materializations: compiled.source_materializations.clone(),
            compiled_source_plan_hash: Some(compiled.compiled_source_plan_hash()?),
            effective_schema_runtime: compiled.effective_schema_runtime.clone(),
            baseline_observation_schema_catalog: compiled
                .baseline_observation_schema_catalog
                .clone(),
        })
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
                        decoder_schema: self.decoder_schema,
                        physical_schema: self.physical_schema,
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

fn current_physical_schema(compiled: &cdf_runtime::CompiledSourcePlan) -> Result<SchemaRef> {
    if let Some(runtime) = compiled.effective_schema_runtime.as_ref() {
        let [observation] = runtime.evidence.observations() else {
            return Err(CdfError::data(
                "MongoDB execution requires exactly one current collection schema observation",
            ));
        };
        return runtime
            .physical_schema(&observation.physical_schema_hash)
            .cloned()
            .ok_or_else(|| {
                CdfError::data(
                    "MongoDB current collection schema observation is absent from its physical catalog",
                )
            });
    }
    let [observation] = compiled.baseline_observation_schema_catalog.as_slice() else {
        return Err(CdfError::data(
            "MongoDB execution requires exactly one pinned collection schema observation",
        ));
    };
    Ok(Arc::clone(&observation.schema))
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

    fn source_materializations(&self) -> &[cdf_kernel::SourceMaterializationRule] {
        &self.source_materializations
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
        let mut partition = plan_mongodb_partition(
            &self.descriptor,
            &self.schema,
            &self.collection,
            &self.collection_generation,
            request,
        )?;
        partition.scan_intent = CompiledScanIntent::full_scan();
        if self.effective_schema_runtime.is_some() {
            cdf_kernel::bind_partition_schema_candidate(&mut partition, "runtime.mongodb")?;
        }
        Ok(vec![partition])
    }

    fn rebind_scan_for_resume(
        &self,
        mut scan: ScanPlan,
        committed_frontier: &SourcePosition,
    ) -> Result<ScanPlan> {
        let partitions = scan.inline_partitions_mut().ok_or_else(|| {
            CdfError::contract("MongoDB resume binding requires one inline partition")
        })?;
        let [partition] = partitions.as_mut_slice() else {
            return Err(CdfError::contract(format!(
                "MongoDB resume binding requires one partition, found {}",
                partitions.len()
            )));
        };
        rebind_mongodb_partition_for_resume(&self.descriptor, partition, committed_frontier)?;
        Ok(scan)
    }

    fn attest_partition(&self, partition: PartitionPlan) -> PartitionAttestationAttempt<'_> {
        if partition.planned_position.as_ref() != Some(&self.collection_generation) {
            return PartitionAttestationAttempt::materialized(Box::pin(async {
                Err(CdfError::contract(
                    "MongoDB partition collection generation differs from its compiled source authority",
                ))
            }));
        }
        if let Err(error) =
            scan_from_partition(&self.descriptor, &self.schema, &self.collection, &partition)
        {
            return PartitionAttestationAttempt::materialized(Box::pin(async move { Err(error) }));
        }
        let physical_schema_hash = match partition
            .metadata
            .get(PLAN_PHYSICAL_SCHEMA_HASH_KEY)
            .map(|value| SchemaHash::new(value.clone()))
            .transpose()
        {
            Ok(hash) => hash,
            Err(error) => {
                return PartitionAttestationAttempt::materialized(Box::pin(
                    async move { Err(error) },
                ));
            }
        };
        let Some(execution) = self.execution.as_ref() else {
            return PartitionAttestationAttempt::materialized(Box::pin(async {
                Err(CdfError::contract(
                    "MongoDB source attestation requires injected execution services",
                ))
            }));
        };
        let runtime = self.runtime.clone();
        let client = Arc::clone(&self.client);
        let descriptor = self.descriptor.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        let memory = execution.memory();
        let egress = self.egress.clone();
        let cancellation = execution.run_cancellation();
        let attestation = execution.run_io(async move {
            let handle = client
                .get_or_try_init(|| {
                    crate::execution::connect_mongodb(&runtime, memory, &egress, &cancellation)
                })
                .await?;
            let metadata = read_collection_metadata(
                &handle.client.database(database.as_str()),
                &collection,
                &cancellation,
            )
            .await?;
            let position = mongodb_collection_generation_position(
                &descriptor,
                &database,
                &collection,
                metadata.collection_generation_sha256(),
            )?;
            Ok(Some(PartitionAttestation::new(
                position,
                physical_schema_hash,
            )))
        });
        PartitionAttestationAttempt::materialized(Box::pin(async move { attestation }))
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
        let mut scan = negotiate_mongodb_scan(
            &self.descriptor,
            &self.schema,
            &self.collection,
            &self.collection_generation,
            request,
        )?;
        if self.effective_schema_runtime.is_some() {
            let partition = scan
                .inline_partitions_mut()
                .and_then(|partitions| partitions.first_mut())
                .ok_or_else(|| {
                    CdfError::internal("MongoDB negotiation omitted its inline partition")
                })?;
            cdf_kernel::bind_partition_schema_candidate(partition, "runtime.mongodb")?;
        }
        Ok(scan)
    }
}

fn collection_generation_from_schema(
    descriptor: &ResourceDescriptor,
    schema: &arrow_schema::Schema,
    database: &MongoDbIdentifier,
    collection: &MongoDbIdentifier,
) -> Result<SourcePosition> {
    let generation = schema
        .metadata()
        .get(MONGODB_COLLECTION_GENERATION_SCHEMA_KEY)
        .ok_or_else(|| {
            CdfError::data(
                "MongoDB compiled schema omitted collection-generation identity; compile the resource again",
            )
        })?;
    mongodb_collection_generation_position(descriptor, database, collection, generation)
}

pub(crate) fn mongodb_collection_generation_position(
    descriptor: &ResourceDescriptor,
    database: &MongoDbIdentifier,
    collection: &MongoDbIdentifier,
    generation_sha256: &str,
) -> Result<SourcePosition> {
    let Some(generation_hex) = generation_sha256.strip_prefix("sha256:") else {
        return Err(CdfError::data(
            "MongoDB collection-generation identity must use sha256:<64 lowercase hex>",
        ));
    };
    if generation_hex.len() != 64
        || !generation_hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CdfError::data(
            "MongoDB collection-generation identity must use sha256:<64 lowercase hex>",
        ));
    }
    let authority = (
        MONGODB_COLLECTION_GENERATION_PROTOCOL,
        descriptor.resource_id.as_str(),
        database.as_str(),
        collection.as_str(),
        generation_sha256,
    );
    let opaque_blob = serde_json::to_vec(&authority).map_err(|error| {
        CdfError::internal(format!(
            "serialize MongoDB collection-generation authority: {error}"
        ))
    })?;
    let position = SourcePosition::ForeignState(ForeignState {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        protocol: MONGODB_COLLECTION_GENERATION_PROTOCOL.to_owned(),
        blob_sha256: artifact_hash(&authority)?,
        opaque_blob,
    });
    position.validate()?;
    Ok(position)
}

pub(crate) fn rebind_mongodb_partition_for_resume(
    descriptor: &ResourceDescriptor,
    partition: &mut PartitionPlan,
    committed_frontier: &SourcePosition,
) -> Result<()> {
    committed_frontier.validate()?;
    let Some(cursor) = &descriptor.cursor else {
        let SourcePosition::ForeignState(state) = committed_frontier else {
            return Err(CdfError::contract(format!(
                "MongoDB full replacement for resource `{}` expected its full-scan completion checkpoint, found {}",
                descriptor.resource_id,
                committed_frontier.kind().as_str()
            )));
        };
        if state.protocol != MONGODB_FULL_SCAN_COMPLETION_PROTOCOL {
            return Err(CdfError::contract(format!(
                "MongoDB full replacement for resource `{}` cannot use foreign-state protocol {:?}",
                descriptor.resource_id, state.protocol
            )));
        }
        partition.start_position = None;
        return Ok(());
    };
    let SourcePosition::Cursor(position) = committed_frontier else {
        return Err(CdfError::contract(format!(
            "MongoDB cursor resource `{}` cannot resume from {} checkpoint authority",
            descriptor.resource_id,
            committed_frontier.kind().as_str()
        )));
    };
    if position.field != cursor.field {
        return Err(CdfError::contract(format!(
            "MongoDB cursor resource `{}` expected checkpoint field {:?}, found {:?}",
            descriptor.resource_id, cursor.field, position.field
        )));
    }
    partition.start_position = Some(committed_frontier.clone());
    Ok(())
}

pub(crate) fn validate_compiled_schema_evidence(
    compiled: &cdf_runtime::CompiledSourcePlan,
) -> Result<()> {
    if compiled.effective_schema_runtime.is_none()
        && compiled.baseline_observation_schema_catalog.is_empty()
    {
        return Err(CdfError::data(
            "MongoDB execution requires sampled physical schema evidence established during resource preparation",
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
    collection_generation: &SourcePosition,
    request: &ScanRequest,
) -> Result<ScanPlan> {
    validate_request(descriptor, schema, collection, request)?;
    let (pushed, unsupported) = classify_predicates(schema, &request.filters);
    Ok(ScanPlan::from_partition_authority(
        PlanId::new(format!("mongodb-scan-{}", descriptor.resource_id))?,
        request.clone(),
        PartitionAuthority::Inline(vec![plan_mongodb_partition(
            descriptor,
            schema,
            collection,
            collection_generation,
            request,
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
    collection_generation: &SourcePosition,
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
        planned_position: Some(collection_generation.clone()),
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
