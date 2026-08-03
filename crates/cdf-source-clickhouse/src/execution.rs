use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, Date32Array, Date64Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampSecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_kernel::{
    Batch, BatchId, CdfError, CursorPosition, CursorValue, EffectiveSchemaRuntime,
    PLAN_PHYSICAL_SCHEMA_HASH_KEY, PartitionPlan, PayloadRetention, ResourceDescriptor, Result,
    SourcePosition, partition_schema_observation_id, physical_type, source_name,
};
use cdf_memory::MemoryCoordinator;
use cdf_runtime::{RunCancellation, SourceEgressScope, TaskStreamSender};

use crate::{
    client::ClickHouseConnection,
    error::classify_clickhouse_error,
    identifier::ClickHouseIdentifier,
    memory::{
        CLICKHOUSE_CURSOR_STATE_BYTES, CLICKHOUSE_DECODE_LEASE_BYTES,
        CLICKHOUSE_HTTP1_TRANSPORT_BYTES, arrow_stream_limits, reserve_cursor_state,
        reserve_decode, reserve_transport,
    },
    query::{build_query, scan_from_partition},
    types::{
        CLICKHOUSE_MAXIMUM_RECORD_BATCH_ROWS, bounded_block_rows, field_by_name,
        validate_clickhouse_type, validate_resource_shape,
    },
};

pub(crate) const CLICKHOUSE_MAXIMUM_BATCH_BYTES: u64 = CLICKHOUSE_DECODE_LEASE_BYTES;
pub(crate) const CLICKHOUSE_MAXIMUM_POLL_BYTES: u64 = CLICKHOUSE_DECODE_LEASE_BYTES
    + CLICKHOUSE_CURSOR_STATE_BYTES
    + CLICKHOUSE_HTTP1_TRANSPORT_BYTES;

pub(crate) struct ClickHouseExecutionInput {
    pub(crate) connection: ClickHouseConnection,
    pub(crate) descriptor: ResourceDescriptor,
    pub(crate) schema: SchemaRef,
    pub(crate) table: ClickHouseIdentifier,
    pub(crate) stable_key: Option<ClickHouseIdentifier>,
    pub(crate) partition: PartitionPlan,
    pub(crate) memory: Arc<dyn MemoryCoordinator>,
    pub(crate) egress: SourceEgressScope,
    pub(crate) effective_schema_runtime: Option<EffectiveSchemaRuntime>,
}

pub(crate) async fn execute_clickhouse_table(
    input: ClickHouseExecutionInput,
    mut sender: TaskStreamSender<Batch>,
    cancellation: RunCancellation,
) -> Result<()> {
    validate_resource_shape(
        &input.descriptor,
        &input.schema,
        &input.table,
        input.stable_key.as_ref(),
    )?;
    let scan = scan_from_partition(
        &input.descriptor,
        &input.schema,
        &input.table,
        input.stable_key.as_ref(),
        &input.partition,
    )?;
    input.egress.authorize(&input.connection.endpoint)?;
    cancellation.check()?;
    let output_schema = projected_schema(&input.schema, &scan.projection)?;
    let physical_schema = execution_physical_schema(&input, &scan.projection)?;
    let query = build_query(
        &input.descriptor,
        &input.schema,
        &input.connection.database,
        &input.table,
        &input.partition,
        &scan,
    )?;
    let observed_schema_hash = cdf_kernel::canonical_arrow_schema_hash(&physical_schema)?;
    let maximum_block_rows = bounded_block_rows(
        input.schema.as_ref(),
        &scan.projection,
        input.connection.max_block_rows,
    )?;
    // Hyper owns one fixed HTTP/1 read buffer for the pooled connection. Keep that persistent
    // native allocation under a separate lease while per-poll Arrow leases are reconciled into
    // emitted batches.
    let transport_lease = cancellation
        .await_or_cancel(reserve_transport(Arc::clone(&input.memory)))
        .await?;
    input
        .connection
        .install_transport_authority(transport_lease)?;
    let cursor_state_lease = cancellation
        .await_or_cancel(reserve_cursor_state(Arc::clone(&input.memory)))
        .await?;
    // The official Arrow cursor is lazy: reserve its complete decode authority before even
    // constructing it, then retain that authority through the first poll.
    let mut decode_lease = cancellation
        .await_or_cancel(reserve_decode(Arc::clone(&input.memory)))
        .await?;
    let limits = arrow_stream_limits(&decode_lease, CLICKHOUSE_MAXIMUM_RECORD_BATCH_ROWS)?;
    let mut cursor = input.connection.arrow_query_with_max_block_rows(
        &query.sql,
        query.parameters,
        "open ClickHouse source Arrow stream",
        maximum_block_rows,
        cursor_state_lease,
        limits,
    )?;
    let mut batch_index = 0_u64;
    loop {
        cancellation.check()?;
        let next = cancellation
            .await_or_cancel(async {
                cursor.next().await.map_err(|error| {
                    classify_clickhouse_error("read ClickHouse source Arrow stream", error)
                })
            })
            .await?;
        let Some(record_batch) = next else {
            return Ok(());
        };
        if record_batch.num_rows() == 0 {
            continue;
        }
        let record_batch = normalize_record_batch(&output_schema, &physical_schema, record_batch)?;
        let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
        if retained_bytes == 0 || retained_bytes > CLICKHOUSE_MAXIMUM_BATCH_BYTES {
            return Err(CdfError::data(format!(
                "ClickHouse Arrow batch retains {retained_bytes} bytes outside its compiled 1..={CLICKHOUSE_MAXIMUM_BATCH_BYTES}-byte bound; reduce max_block_rows or project fewer columns"
            )));
        }
        decode_lease.reconcile(retained_bytes)?;
        batch_index = batch_index.saturating_add(1);
        let source_position =
            batch_cursor_position(&input.descriptor, &scan.projection, &record_batch)?;
        let mut batch = Batch::from_record_batch(
            BatchId::new(format!(
                "{}-clickhouse-{batch_index:06}",
                sanitize_id_part(input.descriptor.resource_id.as_str())
            ))?,
            input.descriptor.resource_id.clone(),
            input.partition.partition_id.clone(),
            observed_schema_hash.clone(),
            record_batch,
        )?
        .with_retention(PayloadRetention::new(
            Arc::new(decode_lease),
            retained_bytes,
        )?)?;
        batch
            .header
            .mark_materialized_output(physical_schema.as_ref())?;
        batch.header.source_position = source_position;
        sender.send(batch).await?;
        decode_lease = cancellation
            .await_or_cancel(reserve_decode(Arc::clone(&input.memory)))
            .await?;
    }
}

fn projected_schema(schema: &SchemaRef, projection: &[String]) -> Result<SchemaRef> {
    let fields = projection
        .iter()
        .map(|name| {
            field_by_name(schema, name).cloned().ok_or_else(|| {
                CdfError::contract(format!(
                    "ClickHouse projection field `{name}` disappeared from the pinned schema"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn execution_physical_schema(
    input: &ClickHouseExecutionInput,
    projection: &[String],
) -> Result<SchemaRef> {
    let Some(runtime) = &input.effective_schema_runtime else {
        return Err(CdfError::data(
            "ClickHouse execution has no catalog-backed physical schema observation; run schema discovery before execution",
        ));
    };
    let observation_id = partition_schema_observation_id(&input.partition);
    let observation = runtime
        .evidence
        .observation(observation_id)
        .ok_or_else(|| {
            CdfError::data(format!(
                "ClickHouse partition references absent schema observation {observation_id:?}"
            ))
        })?;
    let physical = runtime
        .physical_schema(&observation.physical_schema_hash)
        .ok_or_else(|| {
            CdfError::data(format!(
                "ClickHouse schema observation {observation_id:?} references absent physical schema {}",
                observation.physical_schema_hash
            ))
        })?;
    validate_effective_physical_authority(&input.schema, physical)?;
    let projected = project_physical_schema(physical, &input.schema, projection)?;
    let projected_hash = cdf_kernel::canonical_arrow_schema_hash(&projected)?;
    let planned = input
        .partition
        .metadata
        .get(PLAN_PHYSICAL_SCHEMA_HASH_KEY)
        .ok_or_else(|| {
            CdfError::data(
                "ClickHouse effective-schema partition omitted its planned physical schema hash",
            )
        })?;
    if planned != projected_hash.as_str() {
        return Err(CdfError::data(format!(
            "ClickHouse projected physical schema hash {projected_hash} differs from planned authority {planned}"
        )));
    }
    Ok(Arc::new(projected))
}

pub(crate) fn validate_effective_physical_authority(
    effective: &Schema,
    physical: &Schema,
) -> Result<()> {
    for effective_field in effective.fields() {
        let source_field = source_name(effective_field.as_ref()).unwrap_or(effective_field.name());
        let observed_field = physical
            .fields()
            .iter()
            .find(|field| {
                field.name() == source_field || source_name(field.as_ref()) == Some(source_field)
            })
            .ok_or_else(|| {
                CdfError::data(format!(
                    "ClickHouse physical schema observation omitted source field `{source_field}`"
                ))
            })?;
        let effective_type = physical_type(effective_field.as_ref()).ok_or_else(|| {
            CdfError::data(format!(
                "ClickHouse effective field `{}` omitted catalog-backed physical type metadata",
                effective_field.name()
            ))
        })?;
        let observed_type = physical_type(observed_field.as_ref()).ok_or_else(|| {
            CdfError::data(format!(
                "ClickHouse observed field `{source_field}` omitted physical type metadata"
            ))
        })?;
        validate_clickhouse_type(source_field, observed_type)?;
        if effective_type != observed_type {
            return Err(CdfError::data(format!(
                "ClickHouse effective field `{}` physical type `{effective_type}` differs from catalog authority `{observed_type}`",
                effective_field.name()
            )));
        }
    }
    Ok(())
}

pub(crate) fn project_physical_schema(
    physical: &Schema,
    effective: &Schema,
    projection: &[String],
) -> Result<Schema> {
    let fields = projection
        .iter()
        .map(|logical_name| {
            let effective_field = effective.field_with_name(logical_name).map_err(|_| {
                CdfError::contract(format!(
                    "ClickHouse projection field `{logical_name}` is absent from the effective schema"
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
                        "ClickHouse physical schema observation omitted projected source field `{physical_name}` for effective field `{logical_name}`"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Schema::new_with_metadata(
        fields,
        physical.metadata().clone(),
    ))
}

pub(crate) fn normalize_record_batch(
    output: &SchemaRef,
    physical: &SchemaRef,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    let actual = batch.schema();
    if output.fields().len() != physical.fields().len()
        || output.fields().len() != actual.fields().len()
    {
        return Err(CdfError::data(
            "ClickHouse Arrow batch column count differs from its pinned projection",
        ));
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (((output, physical), actual), column) in output
        .fields()
        .iter()
        .zip(physical.fields())
        .zip(actual.fields())
        .zip(batch.columns())
    {
        if output.name() != actual.name()
            || output.is_nullable() != physical.is_nullable()
            || physical.is_nullable() != actual.is_nullable()
        {
            return Err(CdfError::data(format!(
                "ClickHouse Arrow field `{}` changed after discovery; expected {:?} nullable={}, observed `{}` {:?} nullable={}; refresh discovery before retrying",
                output.name(),
                physical.data_type(),
                physical.is_nullable(),
                actual.name(),
                actual.data_type(),
                actual.is_nullable()
            )));
        }
        if physical_type(physical) == Some("UUID") {
            if output.data_type() != &DataType::Utf8
                || physical.data_type() != &DataType::Utf8
                || actual.data_type() != &DataType::Binary
            {
                return Err(CdfError::data(format!(
                    "ClickHouse UUID field `{}` contradicted its canonical Binary-to-Utf8 mapping; expected {:?}, observed {:?}",
                    output.name(),
                    physical.data_type(),
                    actual.data_type()
                )));
            }
            columns.push(normalize_uuid_column(output, column)?);
        } else {
            if output.data_type() != physical.data_type()
                || physical.data_type() != actual.data_type()
            {
                return Err(CdfError::data(format!(
                    "ClickHouse Arrow field `{}` changed after discovery; expected {:?} nullable={}, observed `{}` {:?} nullable={}; refresh discovery before retrying",
                    output.name(),
                    physical.data_type(),
                    physical.is_nullable(),
                    actual.name(),
                    actual.data_type(),
                    actual.is_nullable()
                )));
            }
            columns.push(Arc::clone(column));
        }
    }
    RecordBatch::try_new(Arc::clone(output), columns).map_err(|error| {
        CdfError::data(format!(
            "ClickHouse Arrow batch contradicted its pinned schema: {error}"
        ))
    })
}

fn normalize_uuid_column(field: &Field, column: &ArrayRef) -> Result<ArrayRef> {
    let binary = column
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| {
            CdfError::data(format!(
                "ClickHouse UUID field `{}` did not produce Arrow Binary",
                field.name()
            ))
        })?;
    let strings = StringArray::try_from_binary(binary.clone()).map_err(|_| {
        CdfError::data(format!(
            "ClickHouse UUID field `{}` canonical cast produced non-UTF-8 bytes",
            field.name()
        ))
    })?;
    for value in strings.iter().flatten() {
        if !canonical_uuid_text(value) {
            return Err(CdfError::data(format!(
                "ClickHouse UUID field `{}` canonical cast produced a noncanonical value",
                field.name()
            )));
        }
    }
    Ok(Arc::new(strings))
}

fn canonical_uuid_text(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'),
        })
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
        .position(|name| name == &cursor.field)
        .ok_or_else(|| CdfError::contract("ClickHouse cursor projection disappeared"))?;
    let field = batch.schema().field(index).clone();
    let row = batch.num_rows().checked_sub(1).ok_or_else(|| {
        CdfError::internal("ClickHouse cursor extraction received an empty batch")
    })?;
    let value = cursor_value(&field, batch.column(index).as_ref(), row)?;
    Ok(Some(SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: cursor.field.clone(),
        value,
    })))
}

pub(crate) fn cursor_value(field: &Field, array: &dyn Array, row: usize) -> Result<CursorValue> {
    if array.is_null(row) {
        return Err(CdfError::data(format!(
            "ClickHouse cursor field `{}` produced NULL",
            field.name()
        )));
    }
    let value =
        match field.data_type() {
            DataType::Int64 => array
                .as_any()
                .downcast_ref::<Int64Array>()
                .map(|array| CursorValue::I64(array.value(row))),
            DataType::UInt64 => array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .map(|array| CursorValue::U64(array.value(row))),
            DataType::Date32 => array
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(|array| CursorValue::I64(i64::from(array.value(row)))),
            DataType::Date64 => array.as_any().downcast_ref::<Date64Array>().map(|array| {
                CursorValue::TimestampMicros {
                    micros: array.value(row).saturating_mul(1_000),
                    timezone: None,
                }
            }),
            DataType::Timestamp(unit, timezone) => {
                timestamp_cursor(array, row, unit, timezone.as_deref())
            }
            _ => None,
        };
    value.ok_or_else(|| {
        CdfError::data(format!(
            "ClickHouse cursor field `{}` has incompatible Arrow array {:?}",
            field.name(),
            array.data_type()
        ))
    })
}

fn timestamp_cursor(
    array: &dyn Array,
    row: usize,
    unit: &TimeUnit,
    timezone: Option<&str>,
) -> Option<CursorValue> {
    let raw = match unit {
        TimeUnit::Second => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .map(|array| array.value(row).saturating_mul(1_000_000)),
        TimeUnit::Millisecond => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(|array| array.value(row).saturating_mul(1_000)),
        TimeUnit::Microsecond => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|array| array.value(row)),
        TimeUnit::Nanosecond => return None,
    }?;
    Some(CursorValue::TimestampMicros {
        micros: raw,
        timezone: timezone.map(str::to_owned),
    })
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
