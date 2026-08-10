use std::{fmt, sync::Arc};

use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array,
    builder::{
        BinaryBuilder, Float32Builder, Float64Builder, Int64Builder, StringBuilder, UInt64Builder,
    },
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_kernel::{
    BackpressureSupport, Batch, BatchId, BatchStream, CapabilitySupport, CdfError,
    CompiledSourcePlanHash, CursorPosition, CursorValue, EffectiveSchemaCatalogEntry,
    EffectiveSchemaRuntime, EstimateSupport, FilterCapabilities, IncrementalShape,
    PLAN_PHYSICAL_SCHEMA_HASH_KEY, PartitionAttestation, PartitionAttestationAttempt,
    PartitionPlan, PartitioningCapabilities, PayloadRetention, PushdownFidelity, QueryableResource,
    ReplaySupport, ResourceCapabilities, ResourceDescriptor, ResourceStream, Result, ScanPlan,
    ScanRequest, SchemaHash, ScopeKind, SourcePosition,
};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve};
use cdf_runtime::{ExecutionServices, RunCancellation, SourceEgressScope, TaskStreamSender};
use mysql_async::{Conn, Opts, Row, Value, prelude::Queryable};

use crate::{
    error::classify_mysql_error,
    native::{MySqlNativeOptions, MySqlSourceInput},
    query::{field_by_name, negotiate_scan, plan_partition, scan_query, validate_resource_shape},
    schema::{generation_position, schema_from_columns},
};

pub(crate) const MYSQL_MAXIMUM_BATCH_BYTES: u64 = 64 * 1024 * 1024;
const MYSQL_STREAM_BUFFER_BATCHES: usize = 1;

type ConnectionResolver = Arc<dyn Fn(RunCancellation) -> Result<String> + Send + Sync + 'static>;

#[derive(Clone)]
pub(crate) struct MySqlSourceResource {
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    input: MySqlSourceInput,
    options: MySqlNativeOptions,
    resolve_connection: ConnectionResolver,
    capabilities: ResourceCapabilities,
    execution: ExecutionServices,
    egress: SourceEgressScope,
    type_policy_allowances: cdf_kernel::TypePolicyAllowances,
    compiled_source_plan_hash: CompiledSourcePlanHash,
    effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
}

impl MySqlSourceResource {
    pub(crate) fn from_compiled_plan<F>(
        compiled: &cdf_runtime::CompiledSourcePlan,
        input: MySqlSourceInput,
        options: MySqlNativeOptions,
        execution: ExecutionServices,
        egress: SourceEgressScope,
        resolve_connection: F,
    ) -> Result<Self>
    where
        F: Fn(RunCancellation) -> Result<String> + Send + Sync + 'static,
    {
        let schema = Arc::new(compiled.schema.clone());
        validate_resource_shape(&compiled.descriptor, &schema, &input)?;
        options.validate()?;
        Ok(Self {
            descriptor: compiled.descriptor.clone(),
            schema,
            input,
            options,
            resolve_connection: Arc::new(resolve_connection),
            capabilities: mysql_source_capabilities(&compiled.descriptor),
            execution,
            egress,
            type_policy_allowances: compiled.type_policy_allowances,
            compiled_source_plan_hash: compiled.compiled_source_plan_hash()?,
            effective_schema_runtime: compiled.effective_schema_runtime.clone(),
            baseline_observation_schema_catalog: compiled
                .baseline_observation_schema_catalog
                .clone(),
        })
    }

    fn open_owned(self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'static> {
        if let Err(error) = validate_resource_shape(&self.descriptor, &self.schema, &self.input)
            .and_then(|()| {
                scan_query(&self.descriptor, &self.schema, &self.input, &partition).map(|_| ())
            })
        {
            return cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(
                async move { Err(error) },
            ));
        }
        let memory = self.execution.memory();
        let task = match self.execution.spawn_io_stream(
            "mysql-source-open",
            MYSQL_STREAM_BUFFER_BATCHES,
            move |sender, cancellation| async move {
                let connection = (self.resolve_connection)(cancellation.clone())?;
                execute_mysql(
                    MySqlExecutionInput {
                        connection,
                        descriptor: self.descriptor,
                        schema: self.schema,
                        input: self.input,
                        options: self.options,
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

impl fmt::Debug for MySqlSourceResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MySqlSourceResource")
            .field("descriptor", &self.descriptor)
            .field("schema", &self.schema)
            .field("input", &self.input.redacted_evidence())
            .field("options", &self.options)
            .field("connection", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl ResourceStream for MySqlSourceResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        &self.descriptor
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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

    fn validate_runtime_dependencies(&self) -> Result<()> {
        let connection = (self.resolve_connection)(RunCancellation::default())?;
        self.egress.authorize(&connection)
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        let mut partition = plan_partition(&self.descriptor, &self.schema, &self.input, request)?;
        partition.scan_intent = cdf_kernel::CompiledScanIntent::full_scan();
        if self.effective_schema_runtime.is_some() {
            cdf_kernel::bind_partition_schema_candidate(&mut partition, "runtime.mysql")?;
        }
        Ok(vec![partition])
    }

    fn rebind_scan_for_resume(
        &self,
        mut scan: ScanPlan,
        committed_frontier: &SourcePosition,
    ) -> Result<ScanPlan> {
        if self.descriptor.cursor.is_none() {
            committed_frontier.validate()?;
            let expected = generation_position(
                &self.descriptor,
                &self.input.location_summary(),
                self.schema.as_ref(),
            )?;
            if committed_frontier != &expected {
                return Err(CdfError::data(format!(
                    "MySQL replace resource `{}` has a checkpoint from a different prepared-schema generation; create a new plan",
                    self.descriptor.resource_id
                )));
            }
            let partitions = scan
                .inline_partitions_mut()
                .ok_or_else(|| CdfError::contract("MySQL replace requires one inline partition"))?;
            let [partition] = partitions.as_mut_slice() else {
                return Err(CdfError::contract(
                    "MySQL replace requires exactly one partition",
                ));
            };
            partition.start_position = None;
            return Ok(scan);
        }
        let cursor = self
            .descriptor
            .cursor
            .as_ref()
            .ok_or_else(|| CdfError::internal("MySQL cursor authority disappeared"))?;
        let SourcePosition::Cursor(position) = committed_frontier else {
            return Err(CdfError::contract(format!(
                "MySQL cursor resource `{}` cannot resume from {} authority",
                self.descriptor.resource_id,
                committed_frontier.kind().as_str()
            )));
        };
        if position.field != cursor.field {
            return Err(CdfError::contract(format!(
                "MySQL resource `{}` expected cursor `{}`, found `{}`",
                self.descriptor.resource_id, cursor.field, position.field
            )));
        }
        let partitions = scan
            .inline_partitions_mut()
            .ok_or_else(|| CdfError::contract("MySQL resume requires one inline partition"))?;
        let [partition] = partitions.as_mut_slice() else {
            return Err(CdfError::contract(
                "MySQL resume requires exactly one partition",
            ));
        };
        partition.start_position = Some(committed_frontier.clone());
        Ok(scan)
    }

    fn attest_partition(&self, partition: PartitionPlan) -> PartitionAttestationAttempt<'_> {
        let expected = match generation_position(
            &self.descriptor,
            &self.input.location_summary(),
            self.schema.as_ref(),
        ) {
            Ok(expected) => expected,
            Err(error) => {
                return PartitionAttestationAttempt::materialized(Box::pin(
                    async move { Err(error) },
                ));
            }
        };
        if partition.planned_position.as_ref() != Some(&expected) {
            return PartitionAttestationAttempt::materialized(Box::pin(async {
                Err(CdfError::contract(
                    "MySQL partition generation differs from compiled source authority",
                ))
            }));
        }
        if let Err(error) = scan_query(&self.descriptor, &self.schema, &self.input, &partition) {
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
        let descriptor = self.descriptor.clone();
        let input = self.input.clone();
        let options = self.options.clone();
        let resolve_connection = Arc::clone(&self.resolve_connection);
        let egress = self.egress.clone();
        let cancellation = self.execution.run_cancellation();
        let observed = self.execution.run_io(async move {
            let connection = resolve_connection(cancellation.clone())?;
            egress.authorize(&connection)?;
            let opts = Opts::from_url(&connection)
                .map_err(|_| CdfError::auth("MySQL source connection URI is invalid"))?;
            let mut connection = Conn::new(opts).await.map_err(|error| {
                classify_mysql_error("connect for MySQL source attestation", error)
            })?;
            apply_session_options(&mut connection, &options, &cancellation).await?;
            let mut transaction = connection
                .start_transaction(options.transaction_options())
                .await
                .map_err(|error| {
                    classify_mysql_error("begin MySQL source attestation snapshot", error)
                })?;
            let sql = match &input {
                MySqlSourceInput::Table { target } => {
                    format!("SELECT * FROM {} LIMIT 0", target.sql())
                }
                MySqlSourceInput::Query { sql, .. } => sql.clone(),
            };
            let statement = transaction
                .prep(sql)
                .await
                .map_err(|error| classify_mysql_error("prepare MySQL source attestation", error))?;
            let schema =
                schema_from_columns(&descriptor.resource_id, statement.columns().as_ref())?;
            transaction.rollback().await.map_err(|error| {
                classify_mysql_error("close MySQL source attestation snapshot", error)
            })?;
            let position = generation_position(&descriptor, &input.location_summary(), &schema)?;
            Ok(Some(PartitionAttestation::new(
                position,
                physical_schema_hash,
            )))
        });
        PartitionAttestationAttempt::materialized(Box::pin(async move { observed }))
    }

    fn open(&self, partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
        self.clone().open_owned(partition)
    }
}

impl QueryableResource for MySqlSourceResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        &self.capabilities
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        let mut scan = negotiate_scan(&self.descriptor, &self.schema, &self.input, request)?;
        if self.effective_schema_runtime.is_some() {
            let partition = scan
                .inline_partitions_mut()
                .and_then(|partitions| partitions.first_mut())
                .ok_or_else(|| CdfError::internal("MySQL scan omitted its partition"))?;
            cdf_kernel::bind_partition_schema_candidate(partition, "runtime.mysql")?;
        }
        Ok(scan)
    }
}

pub(crate) fn mysql_source_capabilities(descriptor: &ResourceDescriptor) -> ResourceCapabilities {
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
        limits: CapabilitySupport::Supported,
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
        estimates: EstimateSupport::None,
    }
}

struct MySqlExecutionInput {
    connection: String,
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    input: MySqlSourceInput,
    options: MySqlNativeOptions,
    partition: PartitionPlan,
    memory: Arc<dyn MemoryCoordinator>,
    egress: SourceEgressScope,
}

async fn execute_mysql(
    input: MySqlExecutionInput,
    mut sender: TaskStreamSender<Batch>,
    cancellation: RunCancellation,
) -> Result<()> {
    validate_resource_shape(&input.descriptor, &input.schema, &input.input)?;
    input.options.validate()?;
    input.egress.authorize(&input.connection)?;
    cancellation.check()?;
    let opts = Opts::from_url(&input.connection)
        .map_err(|_| CdfError::auth("MySQL source connection URI is invalid"))?;
    let mut connection = cancellation
        .await_or_cancel(async {
            Conn::new(opts)
                .await
                .map_err(|error| classify_mysql_error("connect to MySQL source", error))
        })
        .await?;
    apply_session_options(&mut connection, &input.options, &cancellation).await?;
    let mut transaction = cancellation
        .await_or_cancel(async {
            connection
                .start_transaction(input.options.transaction_options())
                .await
                .map_err(|error| {
                    classify_mysql_error("begin MySQL read-only consistent snapshot", error)
                })
        })
        .await?;
    let query = scan_query(
        &input.descriptor,
        &input.schema,
        &input.input,
        &input.partition,
    )?;
    let statement = cancellation
        .await_or_cancel(async {
            transaction
                .prep(&query.sql)
                .await
                .map_err(|error| classify_mysql_error("prepare MySQL binary source query", error))
        })
        .await?;
    let mut result = cancellation
        .await_or_cancel(async {
            transaction
                .exec_iter(&statement, query.params)
                .await
                .map_err(|error| classify_mysql_error("execute MySQL binary source query", error))
        })
        .await?;
    let columns = result.columns().ok_or_else(|| {
        CdfError::data("MySQL prepared source query returned no result-set metadata")
    })?;
    let physical_schema = Arc::new(schema_from_columns(
        &input.descriptor.resource_id,
        columns.as_ref(),
    )?);
    validate_live_projection(&input.schema, &physical_schema, &query.projection)?;
    let observed_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&physical_schema)?;
    let output_schema = logical_output_schema(&input.schema, &query.projection)?;
    let full_scan_position = input
        .descriptor
        .cursor
        .is_none()
        .then(|| {
            input.partition.planned_position.clone().ok_or_else(|| {
                CdfError::contract(
                    "MySQL bounded scan omitted prepared-schema generation authority",
                )
            })
        })
        .transpose()?;
    let mut batch_index = 0_u64;
    let mut rows_since_poll = 0_usize;
    loop {
        cancellation.check()?;
        let lease = cancellation
            .await_or_cancel(reserve(
                Arc::clone(&input.memory),
                ReservationRequest::new(
                    ConsumerKey::new("mysql-source-batch", MemoryClass::Source)?,
                    MYSQL_MAXIMUM_BATCH_BYTES,
                )?,
            ))
            .await?;
        let mut builder =
            BatchBuilder::new(Arc::clone(&output_schema), input.options.output_batch_rows)?;
        while builder.rows() < input.options.output_batch_rows {
            let next = cancellation
                .await_or_cancel(async {
                    result.next().await.map_err(|error| {
                        classify_mysql_error("read MySQL binary result stream", error)
                    })
                })
                .await?;
            let Some(row) = next else {
                break;
            };
            builder.append(row)?;
            rows_since_poll = rows_since_poll.saturating_add(1);
            if rows_since_poll >= input.options.fetch_rows {
                cancellation.check()?;
                rows_since_poll = 0;
            }
            if builder.estimated_bytes() >= MYSQL_MAXIMUM_BATCH_BYTES {
                break;
            }
        }
        if builder.rows() == 0 {
            break;
        }
        let record_batch = builder.finish()?;
        let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
        if retained_bytes == 0 || retained_bytes > MYSQL_MAXIMUM_BATCH_BYTES {
            return Err(CdfError::data(format!(
                "MySQL Arrow batch retains {retained_bytes} bytes outside its compiled 1..={MYSQL_MAXIMUM_BATCH_BYTES}-byte bound; reduce output_batch_rows or project fewer columns"
            )));
        }
        lease.reconcile(retained_bytes)?;
        batch_index = batch_index.saturating_add(1);
        let source_position =
            batch_cursor_position(&input.descriptor, &query.projection, &record_batch)?
                .or_else(|| full_scan_position.clone());
        let mut batch = Batch::from_record_batch(
            BatchId::new(format!(
                "{}-mysql-{batch_index:06}",
                sanitize_id_part(input.descriptor.resource_id.as_str())
            ))?,
            input.descriptor.resource_id.clone(),
            input.partition.partition_id.clone(),
            observed_schema_hash.clone(),
            record_batch,
        )?
        .with_retention(PayloadRetention::new(Arc::new(lease), retained_bytes)?)?;
        batch.header.mark_materialized_output(&physical_schema)?;
        batch.header.source_position = source_position;
        sender.send(batch).await?;
    }
    drop(result);
    cancellation
        .await_or_cancel(async {
            transaction
                .rollback()
                .await
                .map_err(|error| classify_mysql_error("close MySQL read-only snapshot", error))
        })
        .await
}

pub(crate) async fn apply_session_options(
    connection: &mut Conn,
    options: &MySqlNativeOptions,
    cancellation: &RunCancellation,
) -> Result<()> {
    let mut commands = Vec::new();
    if let Some(timeout) = options.max_execution_time_ms {
        commands.push(format!("SET SESSION max_execution_time = {timeout}"));
    }
    if let Some(timeout_ms) = options.lock_wait_timeout_ms {
        let seconds = timeout_ms.div_ceil(1_000).max(1);
        commands.push(format!("SET SESSION lock_wait_timeout = {seconds}"));
    }
    if options.use_invisible_indexes {
        commands.push("SET SESSION optimizer_switch = 'use_invisible_indexes=on'".to_owned());
    }
    for command in commands {
        cancellation
            .await_or_cancel(async {
                connection.query_drop(command).await.map_err(|error| {
                    classify_mysql_error("apply MySQL source session control", error)
                })
            })
            .await?;
    }
    Ok(())
}

fn logical_output_schema(schema: &SchemaRef, projection: &[String]) -> Result<SchemaRef> {
    let fields = projection
        .iter()
        .map(|name| {
            field_by_name(schema, name).cloned().ok_or_else(|| {
                CdfError::contract(format!("MySQL projection field `{name}` disappeared"))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn validate_live_projection(
    compiled: &SchemaRef,
    live: &SchemaRef,
    projection: &[String],
) -> Result<()> {
    if live.fields().len() != projection.len() {
        return Err(CdfError::data(
            "MySQL prepared query column count changed after discovery; refresh the resource",
        ));
    }
    for (live, logical_name) in live.fields().iter().zip(projection) {
        let compiled = field_by_name(compiled, logical_name).ok_or_else(|| {
            CdfError::data(format!("MySQL compiled field `{logical_name}` disappeared"))
        })?;
        if live.name() != logical_name || live.data_type() != compiled.data_type() {
            return Err(CdfError::data(format!(
                "MySQL field `{logical_name}` changed after discovery; expected {:?}, observed `{}` {:?}; refresh discovery before retrying",
                compiled.data_type(),
                live.name(),
                live.data_type()
            )));
        }
    }
    Ok(())
}

enum ColumnBuilder {
    Int64(Int64Builder),
    UInt64(UInt64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
}

impl ColumnBuilder {
    fn new(field: &Field, rows: usize) -> Result<Self> {
        Ok(match field.data_type() {
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(rows)),
            DataType::UInt64 => Self::UInt64(UInt64Builder::with_capacity(rows)),
            DataType::Float32 => Self::Float32(Float32Builder::with_capacity(rows)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(rows)),
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(rows, 0)),
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(rows, 0)),
            other => {
                return Err(CdfError::data(format!(
                    "MySQL batch builder does not support Arrow type {other:?}"
                )));
            }
        })
    }

    fn append(&mut self, field: &Field, value: Value) -> Result<u64> {
        if value == Value::NULL {
            match self {
                Self::Int64(builder) => builder.append_null(),
                Self::UInt64(builder) => builder.append_null(),
                Self::Float32(builder) => builder.append_null(),
                Self::Float64(builder) => builder.append_null(),
                Self::Utf8(builder) => builder.append_null(),
                Self::Binary(builder) => builder.append_null(),
            }
            return Ok(1);
        }
        let bytes = match (self, value) {
            (Self::Int64(builder), Value::Int(value)) => {
                builder.append_value(value);
                8
            }
            (Self::Int64(builder), Value::UInt(value)) => {
                let value = i64::try_from(value).map_err(|_| type_mismatch(field))?;
                builder.append_value(value);
                8
            }
            (Self::UInt64(builder), Value::UInt(value)) => {
                builder.append_value(value);
                8
            }
            (Self::UInt64(builder), Value::Int(value)) if value >= 0 => {
                builder.append_value(value as u64);
                8
            }
            (Self::Float32(builder), Value::Float(value)) => {
                builder.append_value(value);
                4
            }
            (Self::Float32(builder), Value::Double(value)) => {
                builder.append_value(value as f32);
                4
            }
            (Self::Float64(builder), Value::Double(value)) => {
                builder.append_value(value);
                8
            }
            (Self::Float64(builder), Value::Float(value)) => {
                builder.append_value(f64::from(value));
                8
            }
            (Self::Utf8(builder), Value::Bytes(value)) => {
                let value = String::from_utf8(value).map_err(|_| type_mismatch(field))?;
                let len = u64::try_from(value.len()).map_err(|_| type_mismatch(field))?;
                builder.append_value(value);
                len.saturating_add(8)
            }
            (Self::Utf8(builder), Value::Date(y, m, d, h, min, s, micros)) => {
                let value = format_date(y, m, d, h, min, s, micros);
                let len = value.len() as u64;
                builder.append_value(value);
                len.saturating_add(8)
            }
            (Self::Utf8(builder), Value::Time(negative, days, hours, min, s, micros)) => {
                let value = format_time(negative, days, hours, min, s, micros);
                let len = value.len() as u64;
                builder.append_value(value);
                len.saturating_add(8)
            }
            (Self::Binary(builder), Value::Bytes(value)) => {
                let len = u64::try_from(value.len()).map_err(|_| type_mismatch(field))?;
                builder.append_value(value);
                len.saturating_add(8)
            }
            _ => return Err(type_mismatch(field)),
        };
        Ok(bytes)
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::UInt64(builder) => Arc::new(builder.finish()),
            Self::Float32(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
        }
    }
}

struct BatchBuilder {
    schema: SchemaRef,
    columns: Vec<ColumnBuilder>,
    rows: usize,
    estimated_bytes: u64,
}

impl BatchBuilder {
    fn new(schema: SchemaRef, rows: usize) -> Result<Self> {
        let columns = schema
            .fields()
            .iter()
            .map(|field| ColumnBuilder::new(field, rows))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            schema,
            columns,
            rows: 0,
            estimated_bytes: 0,
        })
    }

    fn append(&mut self, row: Row) -> Result<()> {
        let values = row.unwrap();
        if values.len() != self.columns.len() {
            return Err(CdfError::data(
                "MySQL row column count changed inside the binary result stream",
            ));
        }
        for ((builder, field), value) in self
            .columns
            .iter_mut()
            .zip(self.schema.fields())
            .zip(values)
        {
            self.estimated_bytes = self
                .estimated_bytes
                .checked_add(builder.append(field, value)?)
                .ok_or_else(|| CdfError::data("MySQL batch byte estimate overflowed"))?;
            if self.estimated_bytes > MYSQL_MAXIMUM_BATCH_BYTES {
                return Err(CdfError::data(
                    "one MySQL source row exceeds the compiled Arrow batch memory bound",
                ));
            }
        }
        self.rows = self.rows.saturating_add(1);
        Ok(())
    }

    fn rows(&self) -> usize {
        self.rows
    }

    fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    fn finish(mut self) -> Result<RecordBatch> {
        let arrays = self
            .columns
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect::<Vec<_>>();
        RecordBatch::try_new(self.schema, arrays).map_err(|error| {
            CdfError::data(format!(
                "MySQL binary rows contradicted their prepared schema: {error}"
            ))
        })
    }
}

fn format_date(y: u16, m: u8, d: u8, h: u8, min: u8, s: u8, micros: u32) -> String {
    if h == 0 && min == 0 && s == 0 && micros == 0 {
        format!("{y:04}-{m:02}-{d:02}")
    } else if micros == 0 {
        format!("{y:04}-{m:02}-{d:02} {h:02}:{min:02}:{s:02}")
    } else {
        format!("{y:04}-{m:02}-{d:02} {h:02}:{min:02}:{s:02}.{micros:06}")
    }
}

fn format_time(negative: bool, days: u32, hours: u8, min: u8, s: u8, micros: u32) -> String {
    let sign = if negative { "-" } else { "" };
    let total_hours = u64::from(days)
        .saturating_mul(24)
        .saturating_add(u64::from(hours));
    if micros == 0 {
        format!("{sign}{total_hours:02}:{min:02}:{s:02}")
    } else {
        format!("{sign}{total_hours:02}:{min:02}:{s:02}.{micros:06}")
    }
}

fn type_mismatch(field: &Field) -> CdfError {
    CdfError::data(format!(
        "MySQL binary value for field `{}` contradicted prepared Arrow type {:?}",
        field.name(),
        field.data_type()
    ))
}

fn batch_cursor_position(
    descriptor: &ResourceDescriptor,
    projection: &[String],
    batch: &RecordBatch,
) -> Result<Option<SourcePosition>> {
    let Some(cursor) = &descriptor.cursor else {
        return Ok(None);
    };
    let index = projection
        .iter()
        .position(|field| field == &cursor.field)
        .ok_or_else(|| CdfError::data("MySQL cursor field is absent from emitted rows"))?;
    let field = batch.schema().field(index).clone();
    let array = batch.column(index);
    let mut max = None;
    for row in 0..array.len() {
        if array.is_null(row) {
            return Err(CdfError::data(format!(
                "MySQL cursor field `{}` is NULL",
                field.name()
            )));
        }
        let value = cursor_value(&field, array, row)?;
        if max
            .as_ref()
            .is_none_or(|current| cursor_greater(&value, current))
        {
            max = Some(value);
        }
    }
    Ok(max.map(|value| {
        SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: cursor.field.clone(),
            value,
        })
    }))
}

fn cursor_value(field: &Field, array: &ArrayRef, row: usize) -> Result<CursorValue> {
    Ok(match field.data_type() {
        DataType::Int64 => CursorValue::I64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| type_mismatch(field))?
                .value(row),
        ),
        DataType::UInt64 => CursorValue::U64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| type_mismatch(field))?
                .value(row),
        ),
        DataType::Float64 => CursorValue::DecimalString(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| type_mismatch(field))?
                .value(row)
                .to_string(),
        ),
        DataType::Utf8 => CursorValue::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| type_mismatch(field))?
                .value(row)
                .to_owned(),
        ),
        other => {
            return Err(CdfError::data(format!(
                "MySQL cursor field `{}` has unsupported type {other:?}",
                field.name()
            )));
        }
    })
}

fn cursor_greater(value: &CursorValue, current: &CursorValue) -> bool {
    match (value, current) {
        (CursorValue::I64(value), CursorValue::I64(current)) => value > current,
        (CursorValue::U64(value), CursorValue::U64(current)) => value > current,
        (CursorValue::String(value), CursorValue::String(current)) => value > current,
        (CursorValue::DecimalString(value), CursorValue::DecimalString(current)) => value
            .parse::<f64>()
            .ok()
            .zip(current.parse::<f64>().ok())
            .is_some_and(|(value, current)| value > current),
        _ => false,
    }
}

fn sanitize_id_part(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '-'
            }
        })
        .collect()
}
