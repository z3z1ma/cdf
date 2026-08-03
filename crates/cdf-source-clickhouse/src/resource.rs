use std::{collections::BTreeMap, fmt, sync::Arc};

use arrow_schema::SchemaRef;
use cdf_kernel::{
    BackpressureSupport, BatchStream, CapabilitySupport, CdfError, CompiledScanIntent,
    CompiledSourcePlanHash, DeliveryGuarantee, EffectiveSchemaCatalogEntry, EffectiveSchemaRuntime,
    EstimateSupport, FilterCapabilities, IncrementalShape, PartitionAuthority, PartitionId,
    PartitionPlan, PartitioningCapabilities, PlanId, PushdownFidelity, PushedPredicate,
    QueryableResource, ReplaySupport, ResourceCapabilities, ResourceDescriptor, ResourceStream,
    Result, ScanPlan, ScanPredicate, ScanRequest, ScopeKind,
};
use cdf_runtime::{ExecutionServices, RunCancellation, SourceEgressScope};

use crate::{
    client::ClickHouseConnection,
    execution::{ClickHouseExecutionInput, execute_clickhouse_table},
    identifier::ClickHouseIdentifier,
    query::{
        CLICKHOUSE_SOURCE_KIND, CLICKHOUSE_SQL_DIALECT, predicate_fidelity, scan_from_partition,
    },
    types::validate_resource_shape,
};

type ConnectionResolver =
    Arc<dyn Fn(RunCancellation) -> Result<ClickHouseConnection> + Send + Sync + 'static>;

#[derive(Clone)]
pub(crate) struct ClickHouseTableResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    endpoint: String,
    database: ClickHouseIdentifier,
    table: ClickHouseIdentifier,
    stable_key: Option<ClickHouseIdentifier>,
    stream_buffer_batches: usize,
    resolve_connection: ConnectionResolver,
    capabilities: ResourceCapabilities,
    execution: Option<ExecutionServices>,
    egress: SourceEgressScope,
    type_policy_allowances: cdf_kernel::TypePolicyAllowances,
    compiled_source_plan_hash: Option<CompiledSourcePlanHash>,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
}

impl ClickHouseTableResource {
    fn schema_observation_id(&self) -> String {
        format!("{}.{}", self.database.as_str(), self.table.as_str())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_compiled_plan<F>(
        compiled: &cdf_runtime::CompiledSourcePlan,
        endpoint: String,
        database: ClickHouseIdentifier,
        table: ClickHouseIdentifier,
        stable_key: Option<ClickHouseIdentifier>,
        stream_buffer_batches: usize,
        egress: SourceEgressScope,
        execution: ExecutionServices,
        resolve_connection: F,
    ) -> Result<Self>
    where
        F: Fn(RunCancellation) -> Result<ClickHouseConnection> + Send + Sync + 'static,
    {
        let schema = Arc::new(compiled.schema.clone());
        validate_resource_shape(&compiled.descriptor, &schema, &table, stable_key.as_ref())?;
        validate_compiled_schema_evidence(compiled)?;
        if !(1..=64).contains(&stream_buffer_batches) {
            return Err(CdfError::contract(
                "ClickHouse stream_buffer_batches must be between 1 and 64",
            ));
        }
        Ok(Self {
            descriptor: compiled.descriptor.clone(),
            schema,
            endpoint,
            database,
            table,
            stable_key,
            stream_buffer_batches,
            resolve_connection: Arc::new(resolve_connection),
            capabilities: clickhouse_table_capabilities(&compiled.descriptor),
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

    fn open_owned(self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'static> {
        let Some(execution) = self.execution else {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
                Err(CdfError::contract(
                    "ClickHouse source execution requires injected execution services",
                ))
            }));
        };
        if let Err(error) = validate_resource_shape(
            &self.descriptor,
            &self.schema,
            &self.table,
            self.stable_key.as_ref(),
        )
        .and_then(|()| {
            scan_from_partition(
                &self.descriptor,
                &self.schema,
                &self.table,
                self.stable_key.as_ref(),
                &partition,
            )
            .map(|_| ())
        }) {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(
                async move { Err(error) },
            ));
        }
        let memory = execution.memory();
        let task = match execution.spawn_io_stream(
            "clickhouse-source-open",
            self.stream_buffer_batches,
            move |sender, cancellation| async move {
                let connection = (self.resolve_connection)(cancellation.clone())?;
                execute_clickhouse_table(
                    ClickHouseExecutionInput {
                        connection,
                        descriptor: self.descriptor,
                        schema: self.schema,
                        table: self.table,
                        stable_key: self.stable_key,
                        partition,
                        memory,
                        egress: self.egress,
                        effective_schema_runtime: self.effective_schema_runtime,
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

pub(crate) fn validate_compiled_schema_evidence(
    compiled: &cdf_runtime::CompiledSourcePlan,
) -> Result<()> {
    if compiled.effective_schema_runtime.is_none() {
        return Err(CdfError::data(
            "ClickHouse execution requires catalog-backed physical schema evidence; run schema discovery before resolving the resource",
        ));
    }
    Ok(())
}

impl fmt::Debug for ClickHouseTableResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClickHouseTableResource")
            .field("descriptor", &self.descriptor)
            .field("schema", &self.schema)
            .field("endpoint", &self.endpoint)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("stable_key", &self.stable_key)
            .field("stream_buffer_batches", &self.stream_buffer_batches)
            .field("managed_execution", &self.execution.is_some())
            .finish_non_exhaustive()
    }
}

impl ResourceStream for ClickHouseTableResource {
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
                "ClickHouse source execution requires injected execution services",
            ));
        }
        self.egress.authorize(&self.endpoint)
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        let mut partition = plan_clickhouse_partition(
            &self.descriptor,
            &self.schema,
            &self.table,
            self.stable_key.as_ref(),
            request,
        )?;
        partition.scan_intent = CompiledScanIntent::full_scan();
        if let Some(runtime) = &self.effective_schema_runtime {
            let observation_id = self.schema_observation_id();
            cdf_kernel::bind_partition_schema_observation(
                &mut partition,
                runtime,
                &observation_id,
            )?;
        }
        Ok(vec![partition])
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.clone().open_owned(partition)
    }
}

impl QueryableResource for ClickHouseTableResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        let mut scan = negotiate_clickhouse_scan(
            &self.descriptor,
            &self.schema,
            &self.table,
            self.stable_key.as_ref(),
            request,
        )?;
        if let Some(runtime) = &self.effective_schema_runtime {
            let observation_id = self.schema_observation_id();
            let partition = scan
                .inline_partitions_mut()
                .and_then(|partitions| partitions.first_mut())
                .ok_or_else(|| {
                    CdfError::internal("ClickHouse negotiation omitted its single inline partition")
                })?;
            cdf_kernel::bind_partition_schema_observation(partition, runtime, &observation_id)?;
        }
        Ok(scan)
    }
}

pub(crate) fn clickhouse_table_capabilities(
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

fn negotiate_clickhouse_scan(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    table: &ClickHouseIdentifier,
    stable_key: Option<&ClickHouseIdentifier>,
    request: &ScanRequest,
) -> Result<ScanPlan> {
    validate_request(descriptor, schema, table, stable_key, request)?;
    let (pushed, unsupported) = classify_predicates(schema, &request.filters);
    Ok(ScanPlan::from_partition_authority(
        PlanId::new(format!("clickhouse-scan-{}", descriptor.resource_id))?,
        request.clone(),
        PartitionAuthority::Inline(vec![plan_clickhouse_partition(
            descriptor, schema, table, stable_key, request,
        )?]),
        pushed,
        unsupported,
        None,
        None,
        delivery_guarantee(descriptor),
    ))
}

fn plan_clickhouse_partition(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    table: &ClickHouseIdentifier,
    stable_key: Option<&ClickHouseIdentifier>,
    request: &ScanRequest,
) -> Result<PartitionPlan> {
    validate_request(descriptor, schema, table, stable_key, request)?;
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
    let mut metadata = BTreeMap::from([
        ("kind".to_owned(), CLICKHOUSE_SOURCE_KIND.to_owned()),
        ("dialect".to_owned(), CLICKHOUSE_SQL_DIALECT.to_owned()),
        ("table".to_owned(), table.as_str().to_owned()),
        ("resource_id".to_owned(), descriptor.resource_id.to_string()),
    ]);
    if let Some(stable_key) = stable_key {
        metadata.insert("stable_key".to_owned(), stable_key.as_str().to_owned());
    }
    let partition = PartitionPlan {
        partition_id: PartitionId::new(CLICKHOUSE_SOURCE_KIND)?,
        scope: descriptor.state_scope.clone(),
        planned_position: None,
        start_position: None,
        scan_intent,
        retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
        metadata,
    };
    scan_from_partition(descriptor, schema, table, stable_key, &partition)?;
    Ok(partition)
}

fn validate_request(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    table: &ClickHouseIdentifier,
    stable_key: Option<&ClickHouseIdentifier>,
    request: &ScanRequest,
) -> Result<()> {
    if request.resource_id != descriptor.resource_id {
        return Err(CdfError::contract(format!(
            "scan resource `{}` does not match ClickHouse resource `{}`",
            request.resource_id, descriptor.resource_id
        )));
    }
    validate_resource_shape(descriptor, schema, table, stable_key)
}

fn delivery_guarantee(descriptor: &ResourceDescriptor) -> DeliveryGuarantee {
    match descriptor.write_disposition {
        cdf_kernel::WriteDisposition::Merge if !descriptor.primary_key.is_empty() => {
            DeliveryGuarantee::EffectivelyOncePerKey
        }
        cdf_kernel::WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        cdf_kernel::WriteDisposition::CdcApply => DeliveryGuarantee::EffectivelyOncePerPosition,
        _ => DeliveryGuarantee::AtLeastOnceDuplicateRisk,
    }
}
