use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
        TimestampSecondBuilder, UInt64Builder,
    },
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_kernel::{
    Batch, BatchId, CanonicalArrowSchema, CdfError, CursorPosition, EffectiveSchemaRuntime,
    PLAN_PHYSICAL_SCHEMA_HASH_KEY, PartitionPlan, PayloadRetention,
    PhysicalObservationRepresentation, ResourceDescriptor, Result, SourcePosition,
    partition_schema_observation_id, source_name,
};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve_blocking,
};
use cdf_runtime::{
    BlockingLaneSpec, BlockingTaskStreamSender, InterruptionSafety, LaneAffinity, RunCancellation,
};
use rusqlite::{Connection, TransactionBehavior, params_from_iter, types::ValueRef};

use crate::{
    catalog::{discover_sqlite_table_on_connection, validate_live_unique_stable_key},
    error::{classify_sqlite_error, validate_source_file},
    identifier::SqliteIdentifier,
    native::{SqliteNativeOptions, SqliteSourceInput, prepare_runtime_query},
};

use super::{
    query::{SqliteSourceScan, build_query, projected_fields, scan_from_partition},
    schema::{SqliteTemporalEncoding, storage_class_mismatch, temporal_encoding, type_mismatch},
    temporal::{ObservedCursor, decode_date_days, decode_timestamp, observed_cursor},
};

pub(crate) const SQLITE_SOURCE_BLOCKING_LANE_ID: &str = "sqlite-source.sync";
const SQLITE_TARGET_BATCH_BYTES: u64 = 16 * 1024 * 1024;
pub(crate) const SQLITE_MAXIMUM_BATCH_BYTES: u64 = 32 * 1024 * 1024;
const SQLITE_BUILDER_RESERVATION_BYTES: u64 = 64 * 1024 * 1024;
const CANCELLATION_ROW_GROUP: usize = 256;
const SQLITE_PROGRESS_VM_OPERATIONS: i32 = 8 * 1024;

pub(crate) fn sqlite_source_blocking_lane() -> BlockingLaneSpec {
    BlockingLaneSpec {
        lane_id: SQLITE_SOURCE_BLOCKING_LANE_ID.to_owned(),
        binding: cdf_runtime::BlockingLaneBinding::Static,
        maximum_concurrency: 1,
        cpu_slot_cost: 1,
        native_internal_parallelism: 1,
        affinity: LaneAffinity::Pinned,
        interruption: InterruptionSafety::CooperativeOnly,
    }
}

pub(super) struct SqliteExecutionInput {
    pub(super) database_path: PathBuf,
    pub(super) descriptor: ResourceDescriptor,
    pub(super) schema: SchemaRef,
    pub(super) input: SqliteSourceInput,
    pub(super) stable_key: Option<SqliteIdentifier>,
    pub(super) temporal_encodings: BTreeMap<String, SqliteTemporalEncoding>,
    pub(super) options: SqliteNativeOptions,
    pub(super) source_generation: SourcePosition,
    pub(super) type_policy_allowances: cdf_kernel::TypePolicyAllowances,
    pub(super) effective_schema_runtime: Option<EffectiveSchemaRuntime>,
    pub(super) partition: PartitionPlan,
    pub(super) memory: Arc<dyn MemoryCoordinator>,
}

pub(super) fn execute_sqlite_source(
    input: SqliteExecutionInput,
    mut sender: BlockingTaskStreamSender<Batch>,
    cancellation: RunCancellation,
) -> Result<()> {
    cancellation.check()?;
    validate_source_file(&input.database_path)?;
    let scan = scan_from_partition(
        &input.descriptor,
        &input.schema,
        &input.input,
        input.stable_key.as_ref(),
        &input.temporal_encodings,
        Some(&input.source_generation),
        &input.partition,
    )?;
    let query = build_query(
        &input.descriptor,
        &input.schema,
        &input.input,
        input.stable_key.as_ref(),
        &input.temporal_encodings,
        &input.partition,
        &scan,
    )?;
    let mut connection = input
        .options
        .open_read_only(&input.database_path, "open SQLite source database")?;
    install_progress_handler(&connection, &cancellation)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Deferred)
        .map_err(|error| classify_sqlite_error("begin read-only SQLite source snapshot", error))?;
    if let (Some(table), Some(stable_key)) = (input.input.table(), input.stable_key.as_ref()) {
        validate_live_unique_stable_key(&transaction, table, stable_key)?;
    }
    let projected_fields = projected_fields(&input.schema, &scan.projection)?;
    let output_schema = Arc::new(Schema::new(projected_fields.clone()));
    let physical_schema = execution_physical_schema(&input, &transaction, &scan)?;
    let observed_schema_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref())?;
    // Physical schema authority is invariant for this snapshot; canonicalize it once rather than
    // repeating schema conversion and hashing for every emitted Arrow batch.
    let canonical_physical_schema = CanonicalArrowSchema::from_arrow(physical_schema.as_ref())?;
    let mut statement = match &input.input {
        SqliteSourceInput::Table { .. } => transaction
            .prepare(&query.sql)
            .map_err(|error| classify_sqlite_error("prepare SQLite source query", error))?,
        SqliteSourceInput::Query { .. } => prepare_runtime_query(&transaction, &query.sql)?,
    };
    if !statement.readonly() {
        return Err(CdfError::internal(
            "SQLite source generated a query that is not read-only",
        ));
    }
    validate_statement_output(&statement, &projected_fields)?;
    let mut rows = statement
        .query(params_from_iter(query.params.iter()))
        .map_err(|error| classify_sqlite_error("bind SQLite source query", error))?;
    let field_encodings = projected_fields
        .iter()
        .map(|field| temporal_encoding(field, &input.temporal_encodings))
        .collect::<Vec<_>>();
    let fixed_value_bytes = projected_fields
        .iter()
        .map(|field| fixed_width_value_bytes(field))
        .collect::<Vec<_>>();
    let fixed_row_bytes = fixed_value_bytes.iter().flatten().sum::<u64>();
    let cursor_index = input.descriptor.cursor.as_ref().map(|cursor| {
        scan.projection
            .iter()
            .position(|field| field == &cursor.field)
            .expect("cursor projection validated")
    });
    let exact_non_null_int64_projection = cursor_index.is_none()
        && input.type_policy_allowances == cdf_kernel::TypePolicyAllowances::default()
        && projected_fields
            .iter()
            .all(|field| !field.is_nullable() && field.data_type() == &DataType::Int64);
    let mut batch_index = 0_usize;
    loop {
        cancellation.check()?;
        let lease = reserve_blocking(
            Arc::clone(&input.memory),
            &ReservationRequest::new(
                ConsumerKey::new("sqlite-source-batch", MemoryClass::Source)?,
                SQLITE_BUILDER_RESERVATION_BYTES,
            )?,
        )?;
        let mut builders = if exact_non_null_int64_projection {
            BatchColumnBuilders::ExactNonNullInt64(
                projected_fields
                    .iter()
                    .map(|_| Int64Builder::with_capacity(input.options.output_batch_rows))
                    .collect(),
            )
        } else {
            BatchColumnBuilders::General(
                projected_fields
                    .iter()
                    .map(|field| ColumnBuilder::new(field, input.options.output_batch_rows))
                    .collect::<Result<Vec<_>>>()?,
            )
        };
        let mut row_count = 0_usize;
        let mut estimated_bytes = 0_u64;
        let mut maximum_cursor = None::<ObservedCursor>;
        while row_count < input.options.output_batch_rows
            && estimated_bytes < SQLITE_TARGET_BATCH_BYTES
        {
            if row_count.is_multiple_of(CANCELLATION_ROW_GROUP) {
                cancellation.check()?;
            }
            let Some(row) = rows.next().map_err(|error| {
                classify_execution_error("read SQLite source row", error, &cancellation)
            })?
            else {
                break;
            };
            match &mut builders {
                BatchColumnBuilders::ExactNonNullInt64(builders) => {
                    for (index, (field, builder)) in
                        projected_fields.iter().zip(builders).enumerate()
                    {
                        let value = row.get_ref(index).map_err(|error| {
                            classify_sqlite_error("read SQLite source value", error)
                        })?;
                        match value {
                            ValueRef::Integer(value) => builder.append_value(value),
                            ValueRef::Null => {
                                return Err(CdfError::data(format!(
                                    "SQLite row has NULL for non-nullable field `{}`",
                                    field.name()
                                )));
                            }
                            _ => return type_mismatch(field, value, "SQLite integer"),
                        }
                    }
                }
                BatchColumnBuilders::General(builders) => {
                    for (index, (field, builder)) in
                        projected_fields.iter().zip(builders).enumerate()
                    {
                        let value = row.get_ref(index).map_err(|error| {
                            classify_sqlite_error("read SQLite source value", error)
                        })?;
                        if fixed_value_bytes[index].is_none() {
                            let value_bytes = estimated_value_bytes(value);
                            estimated_bytes =
                                estimated_bytes.checked_add(value_bytes).ok_or_else(|| {
                                    CdfError::data("SQLite source batch byte estimate overflowed")
                                })?;
                            if value_bytes > SQLITE_MAXIMUM_BATCH_BYTES
                                || estimated_bytes > SQLITE_MAXIMUM_BATCH_BYTES
                            {
                                return Err(CdfError::data(format!(
                                    "SQLite field `{}` has a value that would exceed the compiled {SQLITE_MAXIMUM_BATCH_BYTES}-byte batch bound before Arrow allocation; project fewer or smaller columns",
                                    field.name()
                                )));
                            }
                        }
                        let encoding = field_encodings[index];
                        builder.append(field, value, encoding, input.type_policy_allowances)?;
                        if cursor_index == Some(index) {
                            let observed = observed_cursor(field, value, encoding)?;
                            if maximum_cursor
                                .as_ref()
                                .is_none_or(|current| observed.greater_than(current))
                            {
                                maximum_cursor = Some(observed);
                            }
                        }
                    }
                }
            }
            estimated_bytes = estimated_bytes
                .checked_add(fixed_row_bytes)
                .ok_or_else(|| CdfError::data("SQLite source batch byte estimate overflowed"))?;
            row_count += 1;
            if estimated_bytes > SQLITE_MAXIMUM_BATCH_BYTES {
                return Err(CdfError::data(format!(
                    "SQLite source row group exceeds its compiled {SQLITE_MAXIMUM_BATCH_BYTES}-byte batch bound; project fewer or smaller columns"
                )));
            }
        }
        if row_count == 0 {
            break;
        }
        let arrays = builders.finish();
        let record_batch =
            RecordBatch::try_new(Arc::clone(&output_schema), arrays).map_err(|error| {
                CdfError::internal(format!("assemble SQLite source Arrow batch: {error}"))
            })?;
        let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
        if retained_bytes > SQLITE_MAXIMUM_BATCH_BYTES {
            return Err(CdfError::data(format!(
                "SQLite source batch retains {retained_bytes} bytes above its compiled {SQLITE_MAXIMUM_BATCH_BYTES}-byte limit; project fewer or smaller columns"
            )));
        }
        lease.reconcile(retained_bytes)?;
        batch_index = batch_index.saturating_add(1);
        let mut batch = Batch::from_record_batch(
            BatchId::new(format!(
                "{}-sqlite-{batch_index:06}",
                sanitize_id_part(input.descriptor.resource_id.as_str())
            ))?,
            input.descriptor.resource_id.clone(),
            input.partition.partition_id.clone(),
            observed_schema_hash.clone(),
            record_batch,
        )?
        .with_retention(PayloadRetention::new(Arc::new(lease), retained_bytes)?)?;
        batch.header.observation_representation =
            PhysicalObservationRepresentation::MaterializedOutput;
        batch.header.physical_observation_schema = Some(canonical_physical_schema.clone());
        batch.header.source_position = match (&input.descriptor.cursor, maximum_cursor) {
            (Some(cursor), Some(value)) => Some(SourcePosition::Cursor(CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: cursor.field.clone(),
                value: value.into_cursor_value(),
            })),
            (Some(cursor), None) => {
                return Err(CdfError::data(format!(
                    "SQLite cursor field `{}` produced no non-null values",
                    cursor.field
                )));
            }
            (None, _) => Some(input.source_generation.clone()),
        };
        sender.send(batch)?;
        cancellation.check()?;
    }
    drop(rows);
    drop(statement);
    transaction
        .commit()
        .map_err(|error| classify_sqlite_error("close SQLite source snapshot", error))?;
    Ok(())
}

fn validate_statement_output(
    statement: &rusqlite::Statement<'_>,
    projected_fields: &[Arc<Field>],
) -> Result<()> {
    if statement.column_count() != projected_fields.len() {
        return Err(CdfError::data(format!(
            "SQLite source query produced {} columns, expected {} from compiled authority; compile the resource again",
            statement.column_count(),
            projected_fields.len()
        )));
    }
    for (index, field) in projected_fields.iter().enumerate() {
        let observed = statement
            .column_name(index)
            .map_err(|error| classify_sqlite_error("inspect SQLite source query output", error))?;
        if observed != field.name() {
            return Err(CdfError::data(format!(
                "SQLite source query column {index} is `{observed}`, expected `{}` from compiled authority; compile the resource again",
                field.name()
            )));
        }
    }
    Ok(())
}

fn execution_physical_schema(
    input: &SqliteExecutionInput,
    connection: &Connection,
    scan: &SqliteSourceScan,
) -> Result<SchemaRef> {
    let runtime_observation = input
        .effective_schema_runtime
        .as_ref()
        .map(|runtime| {
            let observation_id = partition_schema_observation_id(&input.partition);
            let observation = runtime
                .evidence
                .observation(observation_id)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "SQLite partition references absent schema observation {observation_id:?}"
                    ))
                })?;
            let physical = runtime
                .physical_schema(&observation.physical_schema_hash)
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "SQLite schema observation {observation_id:?} references absent physical schema {}",
                        observation.physical_schema_hash
                    ))
                })?;
            Ok::<_, CdfError>((observation_id, observation, physical))
        })
        .transpose()?;
    let live = match &input.input {
        SqliteSourceInput::Table { table } => {
            discover_sqlite_table_on_connection(connection, &input.descriptor.resource_id, table)?
                .schema
        }
        SqliteSourceInput::Query { .. } => runtime_observation.as_ref().map_or_else(
            || input.schema.as_ref().clone(),
            |(_, _, schema)| schema.as_ref().clone(),
        ),
    };
    let projected = project_physical_schema(&live, &input.schema, &scan.projection)?;
    let Some((_observation_id, observation, expected_physical)) = runtime_observation else {
        return Ok(Arc::new(projected));
    };
    let live_hash = cdf_kernel::canonical_arrow_schema_hash(&live)?;
    if live_hash != observation.physical_schema_hash {
        return Err(CdfError::data(format!(
            "SQLite input `{}` physical schema changed after verified discovery; expected {}, observed {live_hash}; refresh discovery before retrying",
            input.input.location_summary(),
            observation.physical_schema_hash
        )));
    }
    if live != **expected_physical {
        return Err(CdfError::data(format!(
            "SQLite input `{}` live physical schema does not match verified catalog entry {}",
            input.input.location_summary(),
            observation.physical_schema_hash
        )));
    }
    let projected_hash = cdf_kernel::canonical_arrow_schema_hash(&projected)?;
    let planned_hash = input
        .partition
        .metadata
        .get(PLAN_PHYSICAL_SCHEMA_HASH_KEY)
        .ok_or_else(|| {
            CdfError::data(
                "SQLite effective-schema partition omitted its planned physical schema hash",
            )
        })?;
    if projected_hash.as_str() != planned_hash {
        return Err(CdfError::data(format!(
            "SQLite projected physical schema hash {projected_hash} does not match planned authority {planned_hash}"
        )));
    }
    Ok(Arc::new(projected))
}

fn project_physical_schema(
    physical: &Schema,
    effective: &Schema,
    projection: &[String],
) -> Result<Schema> {
    let fields = projection
        .iter()
        .map(|logical_name| {
            let effective_field = effective.field_with_name(logical_name).map_err(|_| {
                CdfError::contract(format!(
                    "SQLite projection field `{logical_name}` is absent from the effective schema"
                ))
            })?;
            let physical_name = source_name(effective_field).unwrap_or(effective_field.name());
            physical
                .fields()
                .iter()
                .find(|field| {
                    field.name() == physical_name
                        || source_name(field.as_ref()) == Some(physical_name)
                })
                .cloned()
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "SQLite physical schema observation omitted projected source field `{physical_name}` for effective field `{logical_name}`"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new_with_metadata(
        fields,
        physical.metadata().clone(),
    ))
}

pub(super) fn install_progress_handler(
    connection: &Connection,
    cancellation: &RunCancellation,
) -> Result<()> {
    let cancellation = cancellation.clone();
    connection
        .progress_handler(
            SQLITE_PROGRESS_VM_OPERATIONS,
            Some(move || cancellation.is_cancelled()),
        )
        .map_err(|error| classify_sqlite_error("install SQLite source cancellation hook", error))
}

pub(super) fn classify_execution_error(
    action: &str,
    error: rusqlite::Error,
    cancellation: &RunCancellation,
) -> CdfError {
    if cancellation.is_cancelled()
        && matches!(
            &error,
            rusqlite::Error::SqliteFailure(failure, _)
                if failure.code == rusqlite::ffi::ErrorCode::OperationInterrupted
        )
    {
        CdfError::internal("SQLite source execution was cancelled")
    } else {
        classify_sqlite_error(action, error)
    }
}

enum BatchColumnBuilders {
    ExactNonNullInt64(Vec<Int64Builder>),
    General(Vec<ColumnBuilder>),
}

impl BatchColumnBuilders {
    fn finish(self) -> Vec<ArrayRef> {
        match self {
            Self::ExactNonNullInt64(builders) => builders
                .into_iter()
                .map(|mut builder| Arc::new(builder.finish()) as ArrayRef)
                .collect(),
            Self::General(builders) => builders.into_iter().map(ColumnBuilder::finish).collect(),
        }
    }
}

pub(super) enum ColumnBuilder {
    Boolean(BooleanBuilder),
    Int64(Int64Builder),
    UInt64(UInt64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date32(Date32Builder),
    TimestampSecond(TimestampSecondBuilder, Option<Arc<str>>),
    TimestampMillisecond(TimestampMillisecondBuilder, Option<Arc<str>>),
    TimestampMicrosecond(TimestampMicrosecondBuilder, Option<Arc<str>>),
    TimestampNanosecond(TimestampNanosecondBuilder, Option<Arc<str>>),
}

impl ColumnBuilder {
    pub(super) fn new(field: &Field, row_capacity: usize) -> Result<Self> {
        Ok(match field.data_type() {
            DataType::Boolean => Self::Boolean(BooleanBuilder::with_capacity(row_capacity)),
            DataType::Int64 => Self::Int64(Int64Builder::with_capacity(row_capacity)),
            DataType::UInt64 => Self::UInt64(UInt64Builder::with_capacity(row_capacity)),
            DataType::Float64 => Self::Float64(Float64Builder::with_capacity(row_capacity)),
            DataType::Utf8 => Self::Utf8(StringBuilder::with_capacity(row_capacity, 0)),
            DataType::Binary => Self::Binary(BinaryBuilder::with_capacity(row_capacity, 0)),
            DataType::Date32 => Self::Date32(Date32Builder::with_capacity(row_capacity)),
            DataType::Timestamp(TimeUnit::Second, timezone) => Self::TimestampSecond(
                TimestampSecondBuilder::with_capacity(row_capacity),
                timezone.clone(),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, timezone) => Self::TimestampMillisecond(
                TimestampMillisecondBuilder::with_capacity(row_capacity),
                timezone.clone(),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => Self::TimestampMicrosecond(
                TimestampMicrosecondBuilder::with_capacity(row_capacity),
                timezone.clone(),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, timezone) => Self::TimestampNanosecond(
                TimestampNanosecondBuilder::with_capacity(row_capacity),
                timezone.clone(),
            ),
            other => {
                return Err(CdfError::data(format!(
                    "SQLite source does not support Arrow type {other:?}"
                )));
            }
        })
    }

    pub(super) fn append(
        &mut self,
        field: &Field,
        value: ValueRef<'_>,
        encoding: Option<SqliteTemporalEncoding>,
        allowances: cdf_kernel::TypePolicyAllowances,
    ) -> Result<()> {
        if matches!(value, ValueRef::Null) {
            if !field.is_nullable() {
                return Err(CdfError::data(format!(
                    "SQLite row has NULL for non-nullable field `{}`",
                    field.name()
                )));
            }
            match self {
                Self::Boolean(b) => b.append_null(),
                Self::Int64(b) => b.append_null(),
                Self::UInt64(b) => b.append_null(),
                Self::Float64(b) => b.append_null(),
                Self::Utf8(b) => b.append_null(),
                Self::Binary(b) => b.append_null(),
                Self::Date32(b) => b.append_null(),
                Self::TimestampSecond(b, _) => b.append_null(),
                Self::TimestampMillisecond(b, _) => b.append_null(),
                Self::TimestampMicrosecond(b, _) => b.append_null(),
                Self::TimestampNanosecond(b, _) => b.append_null(),
            }
            return Ok(());
        }
        match self {
            Self::Boolean(builder) => match value {
                ValueRef::Integer(0) => builder.append_value(false),
                ValueRef::Integer(1) => builder.append_value(true),
                ValueRef::Text(value) if allowances.coerce_types => {
                    match std::str::from_utf8(value).ok() {
                        Some("true" | "TRUE" | "1") => builder.append_value(true),
                        Some("false" | "FALSE" | "0") => builder.append_value(false),
                        _ => {
                            return Err(storage_class_mismatch(
                                field,
                                "text",
                                "a coercible boolean",
                            ));
                        }
                    }
                }
                _ => return type_mismatch(field, value, "SQLite integer 0 or 1"),
            },
            Self::Int64(builder) => match value {
                ValueRef::Integer(value) => builder.append_value(value),
                ValueRef::Real(value) if allowances.coerce_types => {
                    match lossless_f64_to_i64(value) {
                        Some(value) => builder.append_value(value),
                        None if allowances.allow_lossy_mapping && value.is_finite() => {
                            builder.append_value(value as i64)
                        }
                        None => {
                            return type_mismatch(
                                field,
                                ValueRef::Real(value),
                                "a lossless integer coercion",
                            );
                        }
                    }
                }
                ValueRef::Real(value) if allowances.allow_lossy_mapping && value.is_finite() => {
                    builder.append_value(value as i64)
                }
                ValueRef::Text(value) if allowances.coerce_types => {
                    let value = std::str::from_utf8(value)
                        .ok()
                        .and_then(|value| value.parse::<i64>().ok())
                        .ok_or_else(|| {
                            storage_class_mismatch(field, "text", "a coercible integer")
                        })?;
                    builder.append_value(value);
                }
                _ => return type_mismatch(field, value, "SQLite integer"),
            },
            Self::UInt64(builder) => match value {
                ValueRef::Integer(value) if value >= 0 => builder.append_value(value as u64),
                ValueRef::Real(value) if allowances.coerce_types => {
                    match lossless_f64_to_u64(value) {
                        Some(value) => builder.append_value(value),
                        None if allowances.allow_lossy_mapping
                            && value.is_finite()
                            && value >= 0.0 =>
                        {
                            builder.append_value(value as u64)
                        }
                        None => {
                            return type_mismatch(
                                field,
                                ValueRef::Real(value),
                                "a lossless unsigned-integer coercion",
                            );
                        }
                    }
                }
                ValueRef::Real(value)
                    if allowances.allow_lossy_mapping && value.is_finite() && value >= 0.0 =>
                {
                    builder.append_value(value as u64)
                }
                ValueRef::Text(value) if allowances.coerce_types => {
                    let value = std::str::from_utf8(value)
                        .ok()
                        .and_then(|value| value.parse::<u64>().ok())
                        .ok_or_else(|| {
                            storage_class_mismatch(field, "text", "a coercible unsigned integer")
                        })?;
                    builder.append_value(value);
                }
                _ => return type_mismatch(field, value, "non-negative SQLite integer"),
            },
            Self::Float64(builder) => {
                let value = match value {
                    ValueRef::Integer(value)
                        if (value as f64) as i64 == value || allowances.allow_lossy_mapping =>
                    {
                        value as f64
                    }
                    ValueRef::Real(value) if value.is_finite() => value,
                    ValueRef::Text(value) if allowances.coerce_types => std::str::from_utf8(value)
                        .ok()
                        .and_then(|value| value.parse::<f64>().ok())
                        .filter(|value| value.is_finite())
                        .ok_or_else(|| {
                            storage_class_mismatch(field, "text", "a coercible finite real")
                        })?,
                    _ => return type_mismatch(field, value, "finite SQLite integer or real"),
                };
                builder.append_value(value);
            }
            Self::Utf8(builder) => match value {
                ValueRef::Text(value) => {
                    builder.append_value(std::str::from_utf8(value).map_err(|error| {
                        CdfError::data(format!(
                            "SQLite field `{}` contains invalid UTF-8: {error}",
                            field.name()
                        ))
                    })?)
                }
                ValueRef::Blob(value) if allowances.coerce_types => {
                    builder.append_value(std::str::from_utf8(value).map_err(|_| {
                        storage_class_mismatch(field, "blob", "a UTF-8 blob coercible to text")
                    })?)
                }
                _ => return type_mismatch(field, value, "SQLite text"),
            },
            Self::Binary(builder) => match value {
                ValueRef::Blob(value) => builder.append_value(value),
                ValueRef::Text(value) if allowances.coerce_types => builder.append_value(value),
                _ => return type_mismatch(field, value, "SQLite blob"),
            },
            Self::Date32(builder) => {
                builder.append_value(decode_date_days(field, value, encoding)?)
            }
            Self::TimestampSecond(builder, _) => {
                builder.append_value(decode_timestamp(field, value, encoding, TimeUnit::Second)?)
            }
            Self::TimestampMillisecond(builder, _) => builder.append_value(decode_timestamp(
                field,
                value,
                encoding,
                TimeUnit::Millisecond,
            )?),
            Self::TimestampMicrosecond(builder, _) => builder.append_value(decode_timestamp(
                field,
                value,
                encoding,
                TimeUnit::Microsecond,
            )?),
            Self::TimestampNanosecond(builder, _) => builder.append_value(decode_timestamp(
                field,
                value,
                encoding,
                TimeUnit::Nanosecond,
            )?),
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> ArrayRef {
        match self {
            Self::Boolean(ref mut b) => Arc::new(b.finish()),
            Self::Int64(ref mut b) => Arc::new(b.finish()),
            Self::UInt64(ref mut b) => Arc::new(b.finish()),
            Self::Float64(ref mut b) => Arc::new(b.finish()),
            Self::Utf8(ref mut b) => Arc::new(b.finish()),
            Self::Binary(ref mut b) => Arc::new(b.finish()),
            Self::Date32(ref mut b) => Arc::new(b.finish()),
            Self::TimestampSecond(ref mut b, ref timezone) => {
                Arc::new(b.finish().with_timezone_opt(timezone.clone()))
            }
            Self::TimestampMillisecond(ref mut b, ref timezone) => {
                Arc::new(b.finish().with_timezone_opt(timezone.clone()))
            }
            Self::TimestampMicrosecond(ref mut b, ref timezone) => {
                Arc::new(b.finish().with_timezone_opt(timezone.clone()))
            }
            Self::TimestampNanosecond(ref mut b, ref timezone) => {
                Arc::new(b.finish().with_timezone_opt(timezone.clone()))
            }
        }
    }
}

fn lossless_f64_to_i64(value: f64) -> Option<i64> {
    if !value.is_finite() || value.fract() != 0.0 {
        return None;
    }
    let converted = value as i64;
    ((converted as f64) == value).then_some(converted)
}

fn lossless_f64_to_u64(value: f64) -> Option<u64> {
    if !value.is_finite() || value.fract() != 0.0 || value < 0.0 {
        return None;
    }
    let converted = value as u64;
    ((converted as f64) == value).then_some(converted)
}

fn estimated_value_bytes(value: ValueRef<'_>) -> u64 {
    match value {
        ValueRef::Null => 1,
        ValueRef::Integer(_) | ValueRef::Real(_) => 9,
        ValueRef::Text(value) | ValueRef::Blob(value) => u64::try_from(value.len())
            .unwrap_or(u64::MAX)
            .saturating_add(9),
    }
}

fn fixed_width_value_bytes(field: &Field) -> Option<u64> {
    matches!(
        field.data_type(),
        DataType::Boolean
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Date32
            | DataType::Timestamp(..)
    )
    .then_some(9)
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
