use std::{collections::BTreeMap, fmt, path::PathBuf, sync::Arc};

use arrow_schema::SchemaRef;
use cdf_kernel::{
    BackpressureSupport, BatchStream, CapabilitySupport, CdfError, CompiledScanIntent,
    CompiledSourcePlanHash, DeliveryGuarantee, EffectiveSchemaCatalogEntry, EffectiveSchemaRuntime,
    EstimateSupport, FilterCapabilities, IncrementalShape, PLAN_PHYSICAL_SCHEMA_HASH_KEY,
    PartitionAttestation, PartitionAttestationAttempt, PartitionAuthority, PartitionId,
    PartitionPlan, PartitioningCapabilities, PlanId, PushdownFidelity, PushedPredicate,
    QueryableResource, ReplaySupport, ResourceCapabilities, ResourceDescriptor, ResourceStream,
    Result, ScanPlan, ScanPredicate, ScanRequest, SchemaHash, ScopeKind,
};
use cdf_runtime::ExecutionServices;

use crate::{
    error::validate_source_file,
    identifier::SqliteIdentifier,
    native::{
        SQLITE_SOURCE_GENERATION_PROTOCOL, SqliteNativeOptions, SqliteSourceInput,
        bind_source_generation, discover_sqlite_query, source_generation_from_schema,
        sqlite_source_generation_position,
    },
};

mod execution;
mod query;
mod schema;
mod temporal;

pub(crate) use self::{
    execution::{
        SQLITE_MAXIMUM_BATCH_BYTES, SQLITE_SOURCE_BLOCKING_LANE_ID, sqlite_source_blocking_lane,
    },
    schema::{SqliteTemporalEncoding, validate_sqlite_source_resource_shape},
};
use self::{
    execution::{SqliteExecutionInput, execute_sqlite_source},
    query::{
        SQLITE_SOURCE_KIND, SQLITE_SQL_DIALECT, SqliteSourceScan, parse_supported_predicate,
        scan_from_partition, validate_requested_order,
    },
};

#[derive(Clone)]
pub(crate) struct SqliteSourceResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    database_path: PathBuf,
    input: SqliteSourceInput,
    stable_key: Option<SqliteIdentifier>,
    temporal_encodings: BTreeMap<String, SqliteTemporalEncoding>,
    options: SqliteNativeOptions,
    source_generation: cdf_kernel::SourcePosition,
    capabilities: ResourceCapabilities,
    execution: Option<ExecutionServices>,
    type_policy_allowances: cdf_kernel::TypePolicyAllowances,
    compiled_source_plan_hash: Option<CompiledSourcePlanHash>,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
}

impl SqliteSourceResource {
    fn new(
        database_path: PathBuf,
        descriptor: ResourceDescriptor,
        schema: SchemaRef,
        input: SqliteSourceInput,
        stable_key: Option<SqliteIdentifier>,
        temporal_encodings: BTreeMap<String, SqliteTemporalEncoding>,
        options: SqliteNativeOptions,
    ) -> Result<Self> {
        input.validate()?;
        options.validate()?;
        let mut authority_schema = schema.as_ref().clone();
        bind_source_generation(&mut authority_schema, &input, &options)?;
        validate_sqlite_source_resource_shape(
            &descriptor,
            &schema,
            stable_key.as_ref(),
            &temporal_encodings,
        )?;
        let source_generation = sqlite_source_generation_position(
            &descriptor,
            source_generation_from_schema(&authority_schema)?,
        )?;
        let capabilities = sqlite_source_capabilities(&descriptor);
        Ok(Self {
            descriptor,
            schema,
            database_path,
            input,
            stable_key,
            temporal_encodings,
            options,
            source_generation,
            capabilities,
            execution: None,
            type_policy_allowances: Default::default(),
            compiled_source_plan_hash: None,
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        })
    }

    pub(crate) fn from_compiled_plan(
        compiled: &cdf_runtime::CompiledSourcePlan,
        database_path: PathBuf,
        input: SqliteSourceInput,
        stable_key: Option<SqliteIdentifier>,
        temporal_encodings: BTreeMap<String, SqliteTemporalEncoding>,
        options: SqliteNativeOptions,
        execution: ExecutionServices,
    ) -> Result<Self> {
        let mut resource = Self::new(
            database_path,
            compiled.descriptor.clone(),
            Arc::new(compiled.schema.clone()),
            input,
            stable_key,
            temporal_encodings,
            options,
        )?;
        resource.compiled_source_plan_hash = Some(compiled.compiled_source_plan_hash()?);
        resource.effective_schema_runtime = compiled.effective_schema_runtime.clone();
        resource.baseline_observation_schema_catalog =
            compiled.baseline_observation_schema_catalog.clone();
        resource.type_policy_allowances = compiled.type_policy_allowances;
        resource = resource.with_execution(execution)?;
        Ok(resource)
    }

    fn with_execution(mut self, execution: ExecutionServices) -> Result<Self> {
        execution.ensure_blocking_lanes(&[sqlite_source_blocking_lane()])?;
        self.execution = Some(execution);
        Ok(self)
    }

    fn open_owned(self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'static> {
        let mut resource = self;
        let Some(execution) = resource.execution.take() else {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
                Err(CdfError::contract(
                    "SQLite source execution requires injected execution services",
                ))
            }));
        };
        open_sqlite_source(resource, partition, execution)
    }
}

fn open_sqlite_source(
    resource: SqliteSourceResource,
    partition: PartitionPlan,
    execution: ExecutionServices,
) -> cdf_kernel::PartitionOpenAttempt<'static> {
    let SqliteSourceResource {
        descriptor,
        schema,
        database_path,
        input,
        stable_key,
        temporal_encodings,
        options,
        source_generation,
        type_policy_allowances,
        effective_schema_runtime,
        ..
    } = resource;
    if let Err(error) = input
        .validate()
        .and_then(|()| options.validate())
        .and_then(|()| {
            validate_sqlite_source_resource_shape(
                &descriptor,
                &schema,
                stable_key.as_ref(),
                &temporal_encodings,
            )
        })
        .and_then(|()| {
            scan_from_partition(
                &descriptor,
                &schema,
                &input,
                stable_key.as_ref(),
                &temporal_encodings,
                Some(&source_generation),
                &partition,
            )
            .map(|_| ())
        })
    {
        return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move { Err(error) }));
    }
    if let Err(error) = execution.ensure_blocking_lanes(&[sqlite_source_blocking_lane()]) {
        return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async move { Err(error) }));
    }
    let memory = execution.memory();
    let task = match execution.spawn_blocking_stream(
        "sqlite-source-open",
        SQLITE_SOURCE_BLOCKING_LANE_ID,
        1,
        move |sender, cancellation| {
            execute_sqlite_source(
                SqliteExecutionInput {
                    database_path,
                    descriptor,
                    schema,
                    input,
                    stable_key,
                    temporal_encodings,
                    options,
                    source_generation,
                    type_policy_allowances,
                    effective_schema_runtime,
                    partition,
                    memory,
                },
                sender,
                cancellation,
            )
        },
    ) {
        Ok(task) => task,
        Err(error) => {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(
                async move { Err(error) },
            ));
        }
    };
    let termination = task.termination();
    let opening = Box::pin(async move {
        Ok(cdf_kernel::PartitionStreamPayload::new(
            Box::pin(task) as BatchStream,
            Box::pin(async { Ok(cdf_kernel::PartitionCompletion::default()) }),
        ))
    });
    cdf_kernel::PartitionOpenAttempt::with_termination(opening, termination)
}

impl fmt::Debug for SqliteSourceResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SqliteSourceResource")
            .field("descriptor", &self.descriptor)
            .field("schema", &self.schema)
            .field("database_path", &"<redacted-sqlite-database>")
            .field("input", &self.input.location_summary())
            .field("stable_key", &self.stable_key)
            .field("temporal_encodings", &self.temporal_encodings)
            .field("options", &self.options)
            .field("source_generation", &self.source_generation)
            .field("capabilities", &self.capabilities)
            .field("managed_execution", &self.execution.is_some())
            .finish()
    }
}

impl ResourceStream for SqliteSourceResource {
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
    fn validate_runtime_dependencies(&self) -> Result<()> {
        if self.execution.is_none() {
            return Err(CdfError::contract(
                "SQLite source execution requires injected execution services",
            ));
        }
        validate_source_file(&self.database_path)
    }
    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        let mut partition = plan_sqlite_source_partition(
            &self.descriptor,
            &self.schema,
            &self.input,
            self.stable_key.as_ref(),
            &self.temporal_encodings,
            Some(&self.source_generation),
            request,
        )?;
        partition.scan_intent = CompiledScanIntent::full_scan();
        if let Some(runtime) = &self.effective_schema_runtime {
            cdf_kernel::bind_partition_schema_observation(
                &mut partition,
                runtime,
                &self.input.location_summary(),
            )?;
        }
        Ok(vec![partition])
    }
    fn type_policy_allowances(&self) -> cdf_kernel::TypePolicyAllowances {
        self.type_policy_allowances
    }
    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.clone().open_owned(partition)
    }
    fn attest_partition(&self, partition: PartitionPlan) -> PartitionAttestationAttempt<'_> {
        if partition.planned_position.as_ref() != Some(&self.source_generation) {
            return PartitionAttestationAttempt::materialized(Box::pin(async {
                Err(CdfError::contract(
                    "SQLite partition generation differs from compiled source authority",
                ))
            }));
        }
        if let Err(error) = scan_from_partition(
            &self.descriptor,
            &self.schema,
            &self.input,
            self.stable_key.as_ref(),
            &self.temporal_encodings,
            Some(&self.source_generation),
            &partition,
        ) {
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
                    "SQLite source attestation requires injected execution services",
                ))
            }));
        };
        let database_path = self.database_path.clone();
        let descriptor = self.descriptor.clone();
        let input = self.input.clone();
        let options = self.options.clone();
        let cancellation = execution.run_cancellation();
        let attestation = execution.run_blocking(SQLITE_SOURCE_BLOCKING_LANE_ID, move || {
            cancellation.check()?;
            let mut schema = match &input {
                SqliteSourceInput::Table { table } => {
                    crate::catalog::discover_sqlite_table(
                        &database_path,
                        &descriptor.resource_id,
                        table,
                    )?
                    .schema
                }
                SqliteSourceInput::Query { .. } => {
                    discover_sqlite_query(
                        &database_path,
                        &descriptor.resource_id,
                        &input,
                        &options,
                        options.discovery_records,
                        options.discovery_bytes,
                    )?
                    .schema
                }
            };
            bind_source_generation(&mut schema, &input, &options)?;
            cancellation.check()?;
            let position = sqlite_source_generation_position(
                &descriptor,
                source_generation_from_schema(&schema)?,
            )?;
            Ok(Some(PartitionAttestation::new(
                position,
                physical_schema_hash,
            )))
        });
        PartitionAttestationAttempt::materialized(Box::pin(async move { attestation }))
    }
}

impl QueryableResource for SqliteSourceResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }
    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        let mut scan = negotiate_sqlite_source_scan(
            &self.descriptor,
            &self.schema,
            &self.input,
            self.stable_key.as_ref(),
            &self.temporal_encodings,
            Some(&self.source_generation),
            request,
        )?;
        if let Some(runtime) = &self.effective_schema_runtime {
            let partition = scan
                .inline_partitions_mut()
                .and_then(|partitions| partitions.first_mut())
                .ok_or_else(|| {
                    CdfError::internal(
                        "SQLite negotiation omitted its single inline partition authority",
                    )
                })?;
            cdf_kernel::bind_partition_schema_observation(
                partition,
                runtime,
                &self.input.location_summary(),
            )?;
        }
        Ok(scan)
    }
}

pub(crate) fn sqlite_source_capabilities(descriptor: &ResourceDescriptor) -> ResourceCapabilities {
    ResourceCapabilities {
        projection: CapabilitySupport::Supported,
        filters: FilterCapabilities {
            default_fidelity: PushdownFidelity::Inexact,
            supported_operators: vec![
                "=".to_owned(),
                ">".to_owned(),
                ">=".to_owned(),
                "<".to_owned(),
                "<=".to_owned(),
            ],
        },
        limits: CapabilitySupport::Unsupported,
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

#[cfg(test)]
pub(crate) fn sqlite_source_predicate_fidelity(
    schema: &SchemaRef,
    expression: &cdf_kernel::DeclarativeExpression,
) -> PushdownFidelity {
    parse_supported_predicate(schema, expression)
        .map_or(PushdownFidelity::Unsupported, |predicate| {
            predicate.fidelity
        })
}

pub(crate) fn classify_sqlite_source_predicates(
    schema: &SchemaRef,
    predicates: &[ScanPredicate],
) -> (Vec<PushedPredicate>, Vec<ScanPredicate>) {
    let mut pushed = Vec::new();
    let mut unsupported = Vec::new();
    for predicate in predicates {
        match parse_supported_predicate(schema, &predicate.canonical_expression) {
            Some(parsed) => pushed.push(PushedPredicate {
                predicate: predicate.clone(),
                fidelity: parsed.fidelity,
            }),
            None => unsupported.push(predicate.clone()),
        }
    }
    (pushed, unsupported)
}

pub(crate) fn negotiate_sqlite_source_scan(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    input: &SqliteSourceInput,
    stable_key: Option<&SqliteIdentifier>,
    temporal_encodings: &BTreeMap<String, SqliteTemporalEncoding>,
    source_generation: Option<&cdf_kernel::SourcePosition>,
    request: &ScanRequest,
) -> Result<ScanPlan> {
    if request.resource_id != descriptor.resource_id {
        return Err(CdfError::contract(format!(
            "scan request resource `{}` does not match SQLite resource `{}`",
            request.resource_id, descriptor.resource_id
        )));
    }
    input.validate()?;
    validate_sqlite_source_resource_shape(descriptor, schema, stable_key, temporal_encodings)?;
    let (pushed, unsupported) = classify_sqlite_source_predicates(schema, &request.filters);
    Ok(ScanPlan::from_partition_authority(
        PlanId::new(format!("sqlite-scan-{}", descriptor.resource_id))?,
        request.clone(),
        PartitionAuthority::Inline(vec![plan_sqlite_source_partition(
            descriptor,
            schema,
            input,
            stable_key,
            temporal_encodings,
            source_generation,
            request,
        )?]),
        pushed,
        unsupported,
        None,
        None,
        delivery_guarantee(descriptor),
    ))
}

pub(crate) fn plan_sqlite_source_partition(
    descriptor: &ResourceDescriptor,
    schema: &SchemaRef,
    input: &SqliteSourceInput,
    stable_key: Option<&SqliteIdentifier>,
    temporal_encodings: &BTreeMap<String, SqliteTemporalEncoding>,
    source_generation: Option<&cdf_kernel::SourcePosition>,
    request: &ScanRequest,
) -> Result<PartitionPlan> {
    if request.resource_id != descriptor.resource_id {
        return Err(CdfError::contract(format!(
            "scan request resource `{}` does not match SQLite resource `{}`",
            request.resource_id, descriptor.resource_id
        )));
    }
    input.validate()?;
    validate_sqlite_source_resource_shape(descriptor, schema, stable_key, temporal_encodings)?;
    validate_requested_order(descriptor, stable_key, &request.order_by)?;
    let (pushed, _) = classify_sqlite_source_predicates(schema, &request.filters);
    let scan_intent = CompiledScanIntent {
        version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
        projection: request.projection.clone(),
        predicates: pushed,
        limit: None,
        order_by: request.order_by.clone(),
    };
    scan_intent.validate()?;
    SqliteSourceScan::from_intent(descriptor, schema, stable_key, &scan_intent)?;
    let mut metadata = BTreeMap::from([
        ("kind".to_owned(), SQLITE_SOURCE_KIND.to_owned()),
        ("dialect".to_owned(), SQLITE_SQL_DIALECT.to_owned()),
        ("input".to_owned(), input.location_summary()),
        ("resource_id".to_owned(), descriptor.resource_id.to_string()),
    ]);
    if let Some(key) = stable_key {
        metadata.insert("stable_key".to_owned(), key.as_str().to_owned());
    }
    if let Some(cdf_kernel::SourcePosition::ForeignState(authority)) = source_generation {
        if authority.protocol != SQLITE_SOURCE_GENERATION_PROTOCOL {
            return Err(CdfError::contract(
                "SQLite source generation uses an unexpected source-position protocol",
            ));
        }
        metadata.insert(
            "source_generation".to_owned(),
            authority.blob_sha256.clone(),
        );
    }
    Ok(PartitionPlan {
        partition_id: PartitionId::new("sqlite")?,
        scope: descriptor.state_scope.clone(),
        planned_position: source_generation.cloned(),
        start_position: None,
        scan_intent,
        retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
        metadata,
    })
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

#[cfg(test)]
mod tests;
