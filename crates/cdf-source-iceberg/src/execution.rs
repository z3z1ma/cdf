use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, make_array, new_null_array};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, SchemaRef, UnionMode};
use cdf_kernel::{
    Batch, BatchId, CdfError, PartitionAttestation, PartitionCompletion, PartitionPlan,
    PayloadRetention, ResourceDescriptor, Result, SourcePosition,
};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest, reserve,
};
use cdf_runtime::{RunCancellation, TaskStreamSender};
use futures_util::{StreamExt, TryStreamExt, stream};
use iceberg::{
    Error as IcebergError, ErrorKind as IcebergErrorKind, Runtime as IcebergRuntime,
    arrow::ArrowReaderBuilder,
    scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream},
    spec::{DataContentType, DataFileFormat, Literal, Struct},
};

use crate::{
    IcebergCatalogContext, IcebergDeleteContent, IcebergSourceOptions,
    storage::prepare_task_file_io, task_reader::IcebergExecutableTask,
};

pub(crate) struct PreparedIcebergTaskScan {
    executable: IcebergExecutableTask,
    file_io: iceberg::io::FileIO,
}

pub(crate) struct IcebergTaskExecution {
    pub descriptor: ResourceDescriptor,
    pub output_schema: SchemaRef,
    pub partition: PartitionPlan,
    pub source: IcebergSourceOptions,
    pub memory: Arc<dyn MemoryCoordinator>,
    pub sender: TaskStreamSender<Batch>,
    pub cancellation: RunCancellation,
}

pub(crate) fn prepare_task_scan(
    context: &IcebergCatalogContext,
    source: &IcebergSourceOptions,
    executable: IcebergExecutableTask,
    cancellation: RunCancellation,
) -> Result<PreparedIcebergTaskScan> {
    executable.task.validate_against(executable.authority())?;
    let (file_io, generation_hash) =
        prepare_task_file_io(context, source, &executable.task, cancellation)?;
    executable.attest_attempt_generation(&generation_hash)?;
    Ok(PreparedIcebergTaskScan {
        executable,
        file_io,
    })
}

pub(crate) async fn execute_task_scan(
    prepared: PreparedIcebergTaskScan,
    execution: IcebergTaskExecution,
) -> Result<PartitionCompletion> {
    let IcebergTaskExecution {
        descriptor,
        output_schema,
        partition,
        source,
        memory,
        mut sender,
        cancellation,
    } = execution;
    cancellation.check()?;
    let output_schema = project_output_schema(
        output_schema.as_ref(),
        &prepared.executable.authority().projected_field_ids,
    )?;
    let batch_rows =
        effective_parquet_batch_rows(&prepared.executable, output_schema.as_ref(), &source)?;
    let task = upstream_task(&prepared.executable)?;
    let snapshot = prepared
        .executable
        .authority()
        .snapshot
        .clone()
        .ok_or_else(|| CdfError::contract("Iceberg executable task omitted snapshot authority"))?;
    let physical_schema_hash = cdf_kernel::canonical_arrow_schema_hash(output_schema.as_ref())?;
    let runtime = IcebergRuntime::try_current().map_err(from_iceberg_error)?;
    let reader = ArrowReaderBuilder::new(prepared.file_io, runtime)
        // CDF already schedules one canonical file task per partition. Nested data-file fan-out
        // would duplicate the scheduler authority and make output order scheduling-dependent.
        .with_data_file_concurrency_limit(1)
        .with_batch_size(batch_rows)
        .with_metadata_size_hint(source.parquet_metadata_prefetch_bytes)
        .with_range_coalesce_bytes(source.parquet_range_coalesce_bytes)
        .with_range_fetch_concurrency(usize::from(
            source
                .parquet_range_fetch_concurrency
                .min(source.maximum_concurrency),
        ))
        .build();
    let tasks: FileScanTaskStream = stream::once(async move { Ok(task) }).boxed();
    let scan = reader.read(tasks).map_err(from_iceberg_error)?;
    let mut batches = scan.stream();
    let mut batch_ordinal = 0_u64;
    let mut emitter = IcebergBatchEmitter {
        descriptor: &descriptor,
        partition: &partition,
        physical_schema_hash: &physical_schema_hash,
        snapshot: &snapshot,
        batch_ordinal: &mut batch_ordinal,
        sender: &mut sender,
    };
    loop {
        cancellation.check()?;
        let request = ReservationRequest::new(
            ConsumerKey::new("iceberg-parquet-decode", MemoryClass::Source)?,
            source.decode_reservation_bytes,
        )?
        .as_minimum_working_set();
        let lease = cancellation
            .await_or_cancel(reserve(Arc::clone(&memory), request))
            .await?;
        let Some(record_batch) = cancellation
            .await_or_cancel(async { batches.try_next().await.map_err(from_iceberg_error) })
            .await?
        else {
            drop(lease);
            break;
        };
        if record_batch.num_rows() == 0 {
            drop(lease);
            continue;
        }
        let record_batch = align_reader_batch(record_batch, Arc::clone(&output_schema))?;
        let decoded_retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
        if decoded_retained_bytes == 0 {
            return Err(CdfError::data(
                "Iceberg Parquet decoded a nonempty batch with no retained Arrow allocation",
            ));
        }
        if decoded_retained_bytes > source.decode_reservation_bytes {
            return Err(CdfError::data(format!(
                "Iceberg Parquet retained {decoded_retained_bytes} decoded bytes above its configured {}-byte decoder envelope; lower parquet_batch_rows or increase decode_reservation_bytes",
                source.decode_reservation_bytes
            )));
        }
        lease.reconcile(decoded_retained_bytes)?;
        let (record_batch, lease) =
            canonicalize_accounted_all_null_columns(record_batch, lease, Arc::clone(&memory))?;
        let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
        if retained_bytes == 0 {
            return Err(CdfError::data(
                "Iceberg Parquet decoded a nonempty batch with no retained Arrow allocation",
            ));
        }
        lease.reconcile(retained_bytes)?;
        if retained_bytes > source.maximum_emitted_batch_bytes {
            return Err(CdfError::data(format!(
                "Iceberg Parquet batch retains {retained_bytes} bytes above its configured {}-byte source-frontier envelope; lower parquet_batch_rows or increase maximum_emitted_batch_bytes",
                source.maximum_emitted_batch_bytes
            )));
        }
        emitter.send(record_batch, lease).await?;
    }
    cancellation.check()?;
    Ok(PartitionCompletion::new(
        Some(PartitionAttestation::new(
            SourcePosition::TableSnapshot(Box::new(snapshot)),
            Some(physical_schema_hash),
        )),
        None,
    ))
}

fn effective_parquet_batch_rows(
    executable: &IcebergExecutableTask,
    output_schema: &Schema,
    source: &IcebergSourceOptions,
) -> Result<usize> {
    let file_schema = executable
        .authority()
        .schema(executable.task.file_schema_id)?;
    parquet_batch_rows_for_observation(
        output_schema,
        file_schema.as_struct().fields().len(),
        executable.task.data_file.file_size_bytes,
        executable.task.data_file.record_count,
        source,
    )
}

fn parquet_batch_rows_for_observation(
    output_schema: &Schema,
    source_field_count: usize,
    file_size_bytes: u64,
    record_count: Option<u64>,
    source: &IcebergSourceOptions,
) -> Result<usize> {
    let structural_bytes = schema_layout_bits_per_row(output_schema)?
        .div_ceil(8)
        .max(1);
    let compressed_bytes = record_count
        .filter(|rows| *rows != 0)
        .map(|rows| file_size_bytes.div_ceil(rows))
        .unwrap_or(0);
    let compressed_bytes = projected_compressed_row_bytes(
        compressed_bytes,
        output_schema.fields().len(),
        source_field_count,
    )?;
    let expanded_compressed = scale_bytes(
        compressed_bytes,
        source.parquet_decode_amplification_bps,
        "Iceberg compressed-row amplification",
    )?;
    let estimated_row_bytes = scale_bytes(
        structural_bytes.max(expanded_compressed).max(1),
        source.parquet_batch_headroom_bps,
        "Iceberg batch-estimate headroom",
    )?
    .max(1);
    let byte_rows = source.target_batch_bytes / estimated_row_bytes;
    Ok(source
        .parquet_batch_rows
        .min(usize::try_from(byte_rows.max(1)).unwrap_or(usize::MAX))
        .max(1))
}

fn projected_compressed_row_bytes(
    complete_row_bytes: u64,
    projected_fields: usize,
    source_fields: usize,
) -> Result<u64> {
    if complete_row_bytes == 0 || projected_fields >= source_fields || source_fields == 0 {
        return Ok(complete_row_bytes);
    }
    let scaled = u128::from(complete_row_bytes)
        .checked_mul(projected_fields as u128)
        .ok_or_else(|| CdfError::data("Iceberg projected compressed-row estimate exceeds u128"))?
        .div_ceil(source_fields as u128)
        .max(1);
    u64::try_from(scaled)
        .map_err(|_| CdfError::data("Iceberg projected compressed-row estimate exceeds u64"))
}

fn scale_bytes(bytes: u64, basis_points: u32, label: &str) -> Result<u64> {
    bytes
        .checked_mul(u64::from(basis_points))
        .map(|scaled| scaled.div_ceil(10_000))
        .ok_or_else(|| CdfError::data(format!("{label} exceeds u64")))
}

fn schema_layout_bits_per_row(schema: &Schema) -> Result<u64> {
    schema.fields().iter().try_fold(0_u64, |total, field| {
        total
            .checked_add(field_layout_bits_per_row(field.as_ref())?)
            .ok_or_else(|| CdfError::data("Iceberg Arrow row-layout estimate exceeds u64"))
    })
}

fn field_layout_bits_per_row(field: &Field) -> Result<u64> {
    let validity = u64::from(field.is_nullable());
    validity
        .checked_add(data_type_layout_bits_per_row(field.data_type())?)
        .ok_or_else(|| CdfError::data("Iceberg Arrow field-layout estimate exceeds u64"))
}

fn data_type_layout_bits_per_row(data_type: &DataType) -> Result<u64> {
    let bits = match data_type {
        DataType::Null | DataType::RunEndEncoded(_, _) => 0,
        DataType::Boolean => 1,
        DataType::Int8 | DataType::UInt8 => 8,
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => 16,
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Decimal32(_, _)
        | DataType::Interval(IntervalUnit::YearMonth) => 32,
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Decimal64(_, _)
        | DataType::Interval(IntervalUnit::DayTime) => 64,
        DataType::Decimal128(_, _)
        | DataType::Interval(IntervalUnit::MonthDayNano)
        | DataType::BinaryView
        | DataType::Utf8View => 128,
        DataType::Decimal256(_, _) => 256,
        DataType::Binary | DataType::Utf8 | DataType::List(_) | DataType::Map(_, _) => 32,
        DataType::LargeBinary | DataType::LargeUtf8 | DataType::LargeList(_) => 64,
        DataType::ListView(_) => 64,
        DataType::LargeListView(_) => 128,
        DataType::FixedSizeBinary(width) => u64::try_from(*width)
            .map_err(|_| CdfError::data("Iceberg fixed-size binary width is negative"))?
            .checked_mul(8)
            .ok_or_else(|| CdfError::data("Iceberg fixed-size binary layout exceeds u64"))?,
        DataType::FixedSizeList(field, length) => {
            let length = u64::try_from(*length)
                .map_err(|_| CdfError::data("Iceberg fixed-size list length is negative"))?;
            field_layout_bits_per_row(field.as_ref())?
                .checked_mul(length)
                .ok_or_else(|| CdfError::data("Iceberg fixed-size list layout exceeds u64"))?
        }
        DataType::Struct(fields) => fields.iter().try_fold(0_u64, |total, field| {
            total
                .checked_add(field_layout_bits_per_row(field.as_ref())?)
                .ok_or_else(|| CdfError::data("Iceberg struct layout exceeds u64"))
        })?,
        DataType::Union(fields, mode) => {
            let child_bits = if *mode == UnionMode::Sparse {
                fields.iter().try_fold(0_u64, |total, (_, field)| {
                    total
                        .checked_add(field_layout_bits_per_row(field.as_ref())?)
                        .ok_or_else(|| CdfError::data("Iceberg sparse-union layout exceeds u64"))
                })?
            } else {
                32
            };
            8_u64
                .checked_add(child_bits)
                .ok_or_else(|| CdfError::data("Iceberg union layout exceeds u64"))?
        }
        DataType::Dictionary(key, _) => data_type_layout_bits_per_row(key.as_ref())?,
    };
    Ok(bits)
}

fn canonicalize_all_null_columns(record_batch: RecordBatch) -> Result<RecordBatch> {
    let row_count = record_batch.num_rows();
    if row_count == 0 {
        return Ok(record_batch);
    }
    let mut canonical = HashMap::<DataType, ArrayRef>::new();
    let columns = record_batch
        .columns()
        .iter()
        .map(|column| {
            if column.null_count() != row_count {
                return Arc::clone(column);
            }
            Arc::clone(
                canonical
                    .entry(column.data_type().clone())
                    .or_insert_with(|| new_null_array(column.data_type(), row_count)),
            )
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(record_batch.schema(), columns).map_err(CdfError::from)
}

/// Transfers an accounted decoded batch into its compact all-null representation only when the
/// ledger can cover both representations during the short ownership handoff.
///
/// Under pressure CDF keeps the original batch rather than blocking while holding the allocation
/// whose release would satisfy the wait. This preserves forward progress and never allocates a
/// second representation outside the ledger.
fn canonicalize_accounted_all_null_columns(
    record_batch: RecordBatch,
    lease: MemoryLease,
    memory: Arc<dyn MemoryCoordinator>,
) -> Result<(RecordBatch, MemoryLease)> {
    let row_count = record_batch.num_rows();
    if row_count == 0
        || !record_batch
            .columns()
            .iter()
            .any(|column| column.null_count() == row_count)
    {
        return Ok((record_batch, lease));
    }
    let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
    let request = ReservationRequest::new(
        ConsumerKey::new("iceberg-parquet-null-canonicalization", MemoryClass::Source)?,
        retained_bytes,
    )?
    .as_minimum_working_set();
    let Some(transition) = memory.try_reserve(&request)? else {
        return Ok((record_batch, lease));
    };
    let canonical = canonicalize_all_null_columns(record_batch)?;
    let canonical_bytes = cdf_memory::record_batch_retained_bytes(&canonical)?;
    drop(lease);
    transition.reconcile(canonical_bytes)?;
    Ok((canonical, transition))
}

struct IcebergBatchEmitter<'a> {
    descriptor: &'a ResourceDescriptor,
    partition: &'a PartitionPlan,
    physical_schema_hash: &'a cdf_kernel::SchemaHash,
    snapshot: &'a cdf_kernel::TableSnapshotPosition,
    batch_ordinal: &'a mut u64,
    sender: &'a mut TaskStreamSender<Batch>,
}

impl IcebergBatchEmitter<'_> {
    async fn send(
        &mut self,
        record_batch: RecordBatch,
        lease: cdf_memory::MemoryLease,
    ) -> Result<()> {
        let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
        lease.reconcile(retained_bytes)?;
        let batch_id = BatchId::new(format!(
            "{}-{:020}",
            self.partition.partition_id.as_str(),
            *self.batch_ordinal
        ))?;
        *self.batch_ordinal = self
            .batch_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("Iceberg batch ordinal exceeds u64"))?;
        let physical_schema = record_batch.schema();
        let mut batch = Batch::from_record_batch(
            batch_id,
            self.descriptor.resource_id.clone(),
            self.partition.partition_id.clone(),
            self.physical_schema_hash.clone(),
            record_batch,
        )?
        .with_retention(PayloadRetention::new(Arc::new(lease), retained_bytes)?)?;
        batch
            .header
            .mark_materialized_output(physical_schema.as_ref())?;
        batch.header.source_position = Some(SourcePosition::TableSnapshot(Box::new(
            self.snapshot.clone(),
        )));
        self.sender.send(batch).await
    }
}

pub(crate) fn project_output_schema(
    schema: &arrow_schema::Schema,
    projected_field_ids: &[i32],
) -> Result<SchemaRef> {
    let mut fields_by_id = std::collections::BTreeMap::new();
    for field in schema.fields() {
        let field_id = arrow_iceberg_field_id(field)?;
        if fields_by_id.insert(field_id, Arc::clone(field)).is_some() {
            return Err(CdfError::data(format!(
                "Iceberg compiled schema repeats field id {field_id}"
            )));
        }
    }
    let fields = projected_field_ids
        .iter()
        .map(|field_id| {
            fields_by_id.get(field_id).cloned().ok_or_else(|| {
                CdfError::contract(format!(
                    "Iceberg compiled projection references field id {field_id} outside the effective Arrow schema"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(arrow_schema::Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn align_reader_batch(
    record_batch: arrow_array::RecordBatch,
    output_schema: SchemaRef,
) -> Result<arrow_array::RecordBatch> {
    let mut input_by_field_id = std::collections::BTreeMap::new();
    for (index, field) in record_batch.schema().fields().iter().enumerate() {
        let field_id = arrow_iceberg_field_id(field)?;
        if input_by_field_id.insert(field_id, index).is_some() {
            return Err(CdfError::data(format!(
                "Iceberg reader output repeats field id {field_id}"
            )));
        }
    }
    let columns = output_schema
        .fields()
        .iter()
        .map(|field| {
            let field_id = arrow_iceberg_field_id(field)?;
            if let Some(index) = input_by_field_id.get(&field_id).copied() {
                return align_array_data_type(
                    Arc::clone(record_batch.column(index)),
                    field.data_type(),
                    field.name(),
                );
            }
            Err(CdfError::data(format!(
                "Iceberg reader output omitted compiled field `{}` (id {field_id})",
                field.name()
            )))
        })
        .collect::<Result<Vec<_>>>()?;
    if input_by_field_id.len() != columns.len() {
        return Err(CdfError::data(format!(
            "Iceberg reader emitted {} fields but the compiled projection contains {}",
            input_by_field_id.len(),
            columns.len()
        )));
    }
    arrow_array::RecordBatch::try_new(output_schema, columns).map_err(|error| {
        CdfError::data(format!(
            "align Iceberg reader output to the compiled snapshot schema: {error}"
        ))
    })
}

fn align_array_data_type(
    array: ArrayRef,
    expected: &arrow_schema::DataType,
    field_name: &str,
) -> Result<ArrayRef> {
    if array.data_type() == expected {
        return Ok(array);
    }
    if !array.data_type().equals_datatype(expected) {
        return Err(CdfError::data(format!(
            "Iceberg reader field `{field_name}` has physical Arrow type {} but the compiled snapshot requires {expected}",
            array.data_type()
        )));
    }
    let data = array
        .to_data()
        .into_builder()
        .data_type(expected.clone())
        .build()
        .map_err(|error| {
            CdfError::data(format!(
                "align nested Iceberg field metadata for `{field_name}` without copying buffers: {error}"
            ))
        })?;
    Ok(make_array(data))
}

fn arrow_iceberg_field_id(field: &arrow_schema::Field) -> Result<i32> {
    let value = field
        .metadata()
        .get("PARQUET:field_id")
        .or_else(|| field.metadata().get("cdf:iceberg_field_id"))
        .ok_or_else(|| {
            CdfError::data(format!(
                "Iceberg Arrow field `{}` omits field-id metadata",
                field.name()
            ))
        })?;
    let field_id = value.parse::<i32>().map_err(|error| {
        CdfError::data(format!(
            "Iceberg Arrow field `{}` has invalid field id `{value}`: {error}",
            field.name()
        ))
    })?;
    if field_id <= 0 {
        return Err(CdfError::data(format!(
            "Iceberg Arrow field `{}` has nonpositive field id {field_id}",
            field.name()
        )));
    }
    Ok(field_id)
}

fn upstream_task(executable: &IcebergExecutableTask) -> Result<FileScanTask> {
    let task = &executable.task;
    let authority = executable.authority();
    task.validate_against(authority)?;
    let schema = authority.schema(authority.output_schema_id)?;
    let file_schema = authority.schema(task.file_schema_id)?;
    let partition_spec = authority.partition_spec(task.partition_spec_id)?;
    let partition_type = partition_spec
        .partition_type(&file_schema)
        .map_err(|error| CdfError::data(format!("bind Iceberg task partition values: {error}")))?;
    let partition = task
        .partition_values
        .iter()
        .zip(partition_type.fields())
        .map(|(value, field)| {
            Literal::try_from_json(
                value.clone().unwrap_or(serde_json::Value::Null),
                field.field_type.as_ref(),
            )
            .map_err(|error| {
                CdfError::data(format!(
                    "decode Iceberg partition value for field {}: {error}",
                    field.id
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .collect::<Struct>();
    let deletes = task
        .deletes
        .iter()
        .map(|delete| {
            Ok(FileScanTaskDeleteFile::builder()
                .with_file_path(delete.path.clone())
                .with_file_size_in_bytes(delete.file_size_bytes)
                .with_file_type(match delete.content {
                    IcebergDeleteContent::Position => DataContentType::PositionDeletes,
                    IcebergDeleteContent::Equality => DataContentType::EqualityDeletes,
                })
                .with_partition_spec_id(delete.partition_spec_id)
                .with_equality_ids(
                    (!delete.equality_field_ids.is_empty())
                        .then(|| delete.equality_field_ids.clone()),
                )
                .build())
        })
        .collect::<Result<Vec<_>>>()?;
    let name_mapping = authority.name_mapping();
    Ok(FileScanTask::builder()
        .with_file_size_in_bytes(task.data_file.file_size_bytes)
        .with_start(task.data_file.range_start)
        .with_length(task.data_file.range_length)
        .with_record_count(task.data_file.record_count)
        .with_data_file_path(task.data_file.path.clone())
        .with_data_file_format(DataFileFormat::Parquet)
        .with_schema(schema)
        .with_project_field_ids(authority.projected_field_ids.clone())
        .with_deletes(deletes)
        .with_partition(Some(partition))
        .with_partition_spec(Some(partition_spec))
        .with_name_mapping(name_mapping)
        .with_case_sensitive(authority.case_sensitive)
        .build())
}

fn from_iceberg_error(error: IcebergError) -> CdfError {
    if error.retryable() {
        return CdfError::transient(error.to_string());
    }
    match error.kind() {
        IcebergErrorKind::DataInvalid => CdfError::data(error.to_string()),
        IcebergErrorKind::PreconditionFailed | IcebergErrorKind::FeatureUnsupported => {
            CdfError::contract(error.to_string())
        }
        _ => CdfError::internal(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        sync::Arc,
    };

    use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, new_null_array};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn field(name: &str, data_type: DataType, field_id: i32) -> Field {
        Field::new(name, data_type, false).with_metadata(HashMap::from([(
            "PARQUET:field_id".to_owned(),
            field_id.to_string(),
        )]))
    }

    #[test]
    fn reader_output_aligns_by_iceberg_field_id_without_copying() {
        let boolean: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        let integer: ArrayRef = Arc::new(Int32Array::from(vec![2025, 2026]));
        let reader = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                field("is_weekday", DataType::Boolean, 12),
                field("year", DataType::Int32, 13),
            ])),
            vec![Arc::clone(&boolean), Arc::clone(&integer)],
        )
        .unwrap();
        let output_schema = Arc::new(Schema::new(vec![
            field("year", DataType::Int32, 13),
            field("is_weekday", DataType::Boolean, 12),
        ]));

        let aligned = align_reader_batch(reader, output_schema.clone()).unwrap();

        assert_eq!(aligned.schema(), output_schema);
        assert!(Arc::ptr_eq(aligned.column(0), &integer));
        assert!(Arc::ptr_eq(aligned.column(1), &boolean));
    }

    #[test]
    fn execution_schema_is_the_compiled_reader_projection_in_declared_order() {
        let schema = Schema::new_with_metadata(
            vec![
                field("name", DataType::Utf8, 20),
                field("id", DataType::Int32, 10),
                field("ignored", DataType::Boolean, 30),
            ],
            HashMap::from([("cdf:snapshot".to_owned(), "pinned".to_owned())]),
        );

        let projected = project_output_schema(&schema, &[10, 20]).unwrap();

        assert_eq!(
            projected
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            ["id", "name"]
        );
        assert_eq!(projected.metadata(), schema.metadata());
    }

    #[test]
    fn nested_field_metadata_is_rebound_without_changing_logical_type() {
        let physical_element = Arc::new(Field::new("element", DataType::Utf8, true).with_metadata(
            HashMap::from([("PARQUET:field_id".to_owned(), "3".to_owned())]),
        ));
        let physical_type = DataType::List(physical_element);
        let physical: ArrayRef = new_null_array(&physical_type, 2);
        let reader = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tags", physical_type, true).with_metadata(HashMap::from([(
                    "PARQUET:field_id".to_owned(),
                    "2".to_owned(),
                )])),
            ])),
            vec![physical],
        )
        .unwrap();
        let expected_element = Arc::new(Field::new("element", DataType::Utf8, true).with_metadata(
            HashMap::from([
                ("PARQUET:field_id".to_owned(), "3".to_owned()),
                ("cdf:iceberg_field_id".to_owned(), "3".to_owned()),
            ]),
        ));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("tags", DataType::List(expected_element), true).with_metadata(
                HashMap::from([("PARQUET:field_id".to_owned(), "2".to_owned())]),
            ),
        ]));

        let aligned = align_reader_batch(reader, Arc::clone(&output_schema)).unwrap();
        assert_eq!(aligned.schema(), output_schema);
        assert_eq!(aligned.column(0).null_count(), 2);
    }

    #[test]
    fn byte_adaptive_rows_preserve_narrow_ceiling_and_shrink_wide_schema() {
        let source: IcebergSourceOptions = serde_json::from_value(serde_json::json!({
            "catalog": {"kind": "filesystem", "warehouse": ".warehouse"}
        }))
        .unwrap();
        let narrow = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        assert_eq!(
            parquet_batch_rows_for_observation(
                &narrow,
                narrow.fields().len(),
                8 * 1024 * 1024,
                Some(1_000_000),
                &source,
            )
            .unwrap(),
            source.parquet_batch_rows
        );

        let wide = Schema::new(
            (0..2_048)
                .map(|index| Field::new(format!("field_{index}"), DataType::Utf8, true))
                .collect::<Vec<_>>(),
        );
        let rows = parquet_batch_rows_for_observation(
            &wide,
            wide.fields().len(),
            8 * 1024 * 1024,
            Some(12_000),
            &source,
        )
        .unwrap();
        assert!(
            rows > 1_000,
            "wide default should retain useful vectorization"
        );
        assert!(rows < 8_000, "wide default must fit the byte target");

        let one_column = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        assert_eq!(
            parquet_batch_rows_for_observation(
                &one_column,
                2_048,
                4 * 1024 * 1024 * 1024,
                Some(1_000_000),
                &source,
            )
            .unwrap(),
            source.parquet_batch_rows,
            "a narrow projection must not inherit the complete wide-file compressed row width"
        );
    }

    #[test]
    fn all_null_columns_share_one_canonical_allocation_per_type() {
        let rows = 8_192;
        let schema = Arc::new(Schema::new(vec![
            Field::new("first", DataType::Utf8, true),
            Field::new("second", DataType::Utf8, true),
            Field::new("third", DataType::Utf8, true),
        ]));
        let original = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                new_null_array(&DataType::Utf8, rows),
                new_null_array(&DataType::Utf8, rows),
                new_null_array(&DataType::Utf8, rows),
            ],
        )
        .unwrap();
        let original_bytes = cdf_memory::record_batch_retained_bytes(&original).unwrap();

        let canonical = canonicalize_all_null_columns(original).unwrap();
        let canonical_bytes = cdf_memory::record_batch_retained_bytes(&canonical).unwrap();

        assert!(Arc::ptr_eq(canonical.column(0), canonical.column(1)));
        assert!(Arc::ptr_eq(canonical.column(1), canonical.column(2)));
        assert!(canonical_bytes < original_bytes / 2);
    }

    #[test]
    fn null_canonicalization_transfers_complete_ledger_authority() {
        let rows = 8_192;
        let original = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("first", DataType::Utf8, true),
                Field::new("second", DataType::Utf8, true),
            ])),
            vec![
                new_null_array(&DataType::Utf8, rows),
                new_null_array(&DataType::Utf8, rows),
            ],
        )
        .unwrap();
        let original_bytes = cdf_memory::record_batch_retained_bytes(&original).unwrap();
        let coordinator = Arc::new(
            cdf_memory::DeterministicMemoryCoordinator::new(original_bytes * 3, BTreeMap::new())
                .unwrap(),
        );
        let memory: Arc<dyn MemoryCoordinator> = coordinator.clone();
        let lease = memory
            .try_reserve(
                &ReservationRequest::new(
                    ConsumerKey::new("test-decode", MemoryClass::Source).unwrap(),
                    original_bytes,
                )
                .unwrap(),
            )
            .unwrap()
            .unwrap();

        let (canonical, canonical_lease) =
            canonicalize_accounted_all_null_columns(original, lease, Arc::clone(&memory)).unwrap();
        let canonical_bytes = cdf_memory::record_batch_retained_bytes(&canonical).unwrap();

        assert_eq!(canonical_lease.bytes(), canonical_bytes);
        assert_eq!(memory.snapshot().current_bytes, canonical_bytes);
        drop(canonical);
        drop(canonical_lease);
        assert_eq!(memory.snapshot().current_bytes, 0);
    }
}
