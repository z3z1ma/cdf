use std::sync::Arc;

use arrow_schema::SchemaRef;
use cdf_kernel::{
    Batch, BatchId, CdfError, PartitionAttestation, PartitionCompletion, PartitionPlan,
    PayloadRetention, ResourceDescriptor, Result, SourcePosition,
};
use cdf_memory::{ConsumerKey, MemoryClass, MemoryCoordinator, ReservationRequest, reserve};
use cdf_runtime::{RunCancellation, TaskStreamSender};
use futures_util::{StreamExt, TryStreamExt, stream};
use iceberg::{
    Error as IcebergError, ErrorKind as IcebergErrorKind, Runtime as IcebergRuntime,
    arrow::ArrowReaderBuilder,
    scan::{FileScanTask, FileScanTaskDeleteFile, FileScanTaskStream},
    spec::{DataContentType, DataFileFormat, Literal, NameMapping, Struct},
};

use crate::{
    IcebergCatalogContext, IcebergDeleteContent, IcebergSourceOptions,
    scan_task::{decode_partition_spec, decode_schema},
    storage::prepare_task_file_io,
    task_reader::IcebergExecutableTask,
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
        .with_batch_size(source.parquet_batch_rows)
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
    loop {
        cancellation.check()?;
        let request = ReservationRequest::new(
            ConsumerKey::new("iceberg-parquet-output", MemoryClass::Source)?,
            source.maximum_batch_bytes,
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
        let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
        if retained_bytes == 0 || retained_bytes > source.maximum_batch_bytes {
            return Err(CdfError::data(format!(
                "Iceberg Parquet batch retains {retained_bytes} bytes above its compiled {}-byte maximum; lower parquet_batch_rows or increase maximum_batch_bytes",
                source.maximum_batch_bytes
            )));
        }
        lease.reconcile(retained_bytes)?;
        let batch_id = BatchId::new(format!(
            "{}-{batch_ordinal:020}",
            partition.partition_id.as_str()
        ))?;
        batch_ordinal = batch_ordinal
            .checked_add(1)
            .ok_or_else(|| CdfError::data("Iceberg batch ordinal exceeds u64"))?;
        let physical_schema = record_batch.schema();
        let mut batch = Batch::from_record_batch(
            batch_id,
            descriptor.resource_id.clone(),
            partition.partition_id.clone(),
            physical_schema_hash.clone(),
            record_batch,
        )?
        .with_retention(PayloadRetention::new(Arc::new(lease), retained_bytes)?)?;
        batch
            .header
            .mark_materialized_output(physical_schema.as_ref())?;
        batch.header.source_position =
            Some(SourcePosition::TableSnapshot(Box::new(snapshot.clone())));
        sender.send(batch).await?;
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
            let index = input_by_field_id.get(&field_id).copied().ok_or_else(|| {
                CdfError::data(format!(
                    "Iceberg reader output omitted compiled field `{}` (id {field_id})",
                    field.name()
                ))
            })?;
            Ok(Arc::clone(record_batch.column(index)))
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
    let schema = Arc::new(decode_schema(
        authority
            .schemas
            .get(&authority.output_schema_id)
            .expect("validated authority contains output schema"),
    )?);
    let file_schema = decode_schema(
        authority
            .schemas
            .get(&task.file_schema_id)
            .expect("validated task contains file schema"),
    )?;
    let partition_spec = Arc::new(decode_partition_spec(
        authority
            .partition_specs
            .get(&task.partition_spec_id)
            .expect("validated task contains partition spec"),
    )?);
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
    let name_mapping = authority
        .name_mapping
        .as_ref()
        .map(|mapping| serde_json::from_value::<NameMapping>(mapping.value.clone()).map(Arc::new))
        .transpose()
        .map_err(|error| CdfError::data(format!("decode Iceberg name mapping: {error}")))?;
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
    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{ArrayRef, BooleanArray, Int32Array, RecordBatch};
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
}
