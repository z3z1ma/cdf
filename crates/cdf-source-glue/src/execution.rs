use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_cast::cast;
use arrow_schema::{Schema, SchemaRef};
use cdf_http::{EgressAllowlist, SecretUri};
use cdf_kernel::{
    Batch, BatchPayload, CdfError, FileManifest, PartitionAttestation, PartitionCompletion, Result,
    SourcePosition,
};
use cdf_memory::{ConsumerKey, MemoryClass, ReservationRequest, reserve};
use cdf_object_access::{FileTransport, FileTransportControl, FileTransportResource};
use cdf_runtime::{
    BoundedFormatRequest, ByteSource, ByteTransformRegistry, DecodeSchemaPlan, FormatRegistry,
    ObservedByteSource, ReadOptions, SourceIoObserver, TransformSourceConfig,
    TransformedByteSource, decode_format_stream,
};
use futures_util::TryStreamExt;

use crate::{GlueObjectTask, GlueSourceOptions};

pub(crate) struct PreparedGlueObject {
    pub source: Arc<dyn ByteSource>,
    pub observer: SourceIoObserver,
    pub format: Arc<dyn cdf_runtime::FormatDriver>,
    pub format_options: serde_json::Value,
    pub data_schema: SchemaRef,
    pub full_schema: SchemaRef,
    pub partition_schema: SchemaRef,
    pub partition_values: Vec<Option<String>>,
    pub planned_position: SourcePosition,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_object(
    task: &GlueObjectTask,
    authority: &crate::GlueTaskAuthority,
    source_options: &GlueSourceOptions,
    table_data_schema: &SchemaRef,
    table_partition_schema: &SchemaRef,
    full_schema: &SchemaRef,
    object_access: &Arc<dyn FileTransport>,
    formats: &Arc<FormatRegistry>,
    transforms: &Arc<ByteTransformRegistry>,
    egress: &cdf_runtime::SourceEgressScope,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    cancellation: cdf_runtime::RunCancellation,
) -> Result<PreparedGlueObject> {
    cancellation.check()?;
    task.validate_against(authority)?;
    let logical = transport_resource(&task.file.path, source_options)?;
    let control = FileTransportControl::new(cancellation.clone(), None);
    let observation = object_access.metadata(egress, &logical, &control)?;
    let access = observation.access_resource(&logical);
    let identity = observation.into_identity();
    if identity.file_position_evidence()? != task.file {
        return Err(CdfError::data(format!(
            "Glue object `{}` changed after planning; re-plan against the current generation",
            task.file.path
        )));
    }
    let source = object_access.open_byte_source(egress, &access, &identity, memory.clone())?;
    let observed = Arc::new(ObservedByteSource::new(source));
    let observer = observed.observer();
    let source: Arc<dyn ByteSource> =
        transform_source(observed, &task.file.path, transforms, memory)?;
    let format = formats.resolve(&task.format.format_id)?;
    let format_options = format.canonical_options(task.format.options.clone())?;
    let data_fields = task
        .data_columns
        .iter()
        .map(|name| {
            table_data_schema
                .field_with_name(name)
                .cloned()
                .map(Arc::new)
                .map_err(|_| {
                    CdfError::data(format!(
                        "Glue partition descriptor column `{name}` is absent from table schema"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    if task.partition_values.len() != table_partition_schema.fields().len() {
        return Err(CdfError::data(format!(
            "Glue object task has {} partition values for {} partition fields",
            task.partition_values.len(),
            table_partition_schema.fields().len()
        )));
    }
    Ok(PreparedGlueObject {
        source,
        observer,
        format,
        format_options,
        data_schema: Arc::new(Schema::new(data_fields)),
        full_schema: Arc::clone(full_schema),
        partition_schema: Arc::clone(table_partition_schema),
        partition_values: task.partition_values.clone(),
        planned_position: SourcePosition::FileManifest(FileManifest {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            files: vec![task.file.clone()],
        }),
    })
}

pub(crate) async fn execute_object(
    prepared: PreparedGlueObject,
    source_options: GlueSourceOptions,
    resource_id: cdf_kernel::ResourceId,
    partition_id: cdf_kernel::PartitionId,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
    cancellation: cdf_runtime::RunCancellation,
    mut sender: cdf_runtime::TaskStreamSender<Batch>,
) -> Result<PartitionCompletion> {
    let mut read_options = ReadOptions::new(resource_id, partition_id);
    read_options.batch_size = source_options.batch_rows;
    let decoded = decode_format_stream(
        prepared.format,
        prepared.source,
        BoundedFormatRequest::new(read_options, Arc::clone(&memory))
            .with_options(prepared.format_options)
            .with_schema(DecodeSchemaPlan::fixed_admission(prepared.data_schema))
            .with_source_position(prepared.planned_position.clone())
            .with_cancellation(cancellation.clone()),
    )
    .await?;
    let mut batches = decoded.batches;
    while let Some(batch) = cancellation.await_or_cancel(batches.try_next()).await? {
        let batch = append_partition_values(
            batch,
            &prepared.full_schema,
            &prepared.partition_schema,
            &prepared.partition_values,
            Arc::clone(&memory),
        )
        .await?;
        sender.send(batch).await?;
    }
    Ok(PartitionCompletion::new(
        Some(PartitionAttestation::new(
            prepared.planned_position,
            Some(cdf_kernel::canonical_arrow_schema_hash(
                prepared.full_schema.as_ref(),
            )?),
        )),
        Some(prepared.observer.snapshot()),
    ))
}

async fn append_partition_values(
    batch: Batch,
    full_schema: &SchemaRef,
    partition_schema: &SchemaRef,
    values: &[Option<String>],
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
) -> Result<Batch> {
    let record = batch
        .record_batch()
        .ok_or_else(|| CdfError::data("Glue format driver emitted a referenced batch"))?;
    let data_by_name = record
        .schema()
        .fields()
        .iter()
        .zip(record.columns())
        .map(|(field, array)| (field.name().clone(), Arc::clone(array)))
        .collect::<BTreeMap<_, _>>();
    if values.len() != partition_schema.fields().len() {
        return Err(CdfError::data(
            "Glue partition-value count does not match the table output schema",
        ));
    }
    let mut value_index = 0_usize;
    let mut columns = Vec::<ArrayRef>::with_capacity(full_schema.fields().len());
    for field in full_schema.fields() {
        let source_name = cdf_kernel::source_name(field.as_ref()).unwrap_or_else(|| field.name());
        let is_partition = partition_schema.fields().iter().any(|partition| {
            cdf_kernel::source_name(partition.as_ref()).unwrap_or_else(|| partition.name())
                == source_name
        });
        if !is_partition {
            let array = data_by_name.get(source_name).ok_or_else(|| {
                CdfError::data(format!(
                    "Glue decoded object omitted declared data column `{source_name}`"
                ))
            })?;
            columns.push(Arc::clone(array));
            continue;
        }
        let value = values.get(value_index).ok_or_else(|| {
            CdfError::data("Glue partition constants ended before the output schema")
        })?;
        value_index += 1;
        let strings = StringArray::from_iter_values(std::iter::repeat_n(
            value.as_deref().unwrap_or_default(),
            record.num_rows(),
        ));
        let array = if value.is_none() {
            arrow_array::new_null_array(field.data_type(), record.num_rows())
        } else if field.data_type() == &arrow_schema::DataType::Utf8 {
            Arc::new(strings) as ArrayRef
        } else {
            cast(&strings, field.data_type()).map_err(|error| {
                CdfError::data(format!(
                    "Glue partition value {:?} cannot be represented as {} for `{}`: {error}",
                    value,
                    field.data_type(),
                    field.name()
                ))
            })?
        };
        columns.push(array);
    }
    let output = RecordBatch::try_new(Arc::clone(full_schema), columns)?;
    let retained = cdf_memory::record_batch_retained_bytes(&output)?;
    let lease = reserve(
        memory,
        ReservationRequest::new(
            ConsumerKey::new("glue-materialized-partition-values", MemoryClass::Source)?,
            retained.max(1),
        )?,
    )
    .await?;
    let Batch { mut header, .. } = batch;
    header.mark_materialized_output(full_schema.as_ref())?;
    if header.source_position.is_none() {
        return Err(CdfError::internal(
            "Glue decoded batch omitted its planned file position",
        ));
    }
    header.set_payload_counts(
        u64::try_from(output.num_rows()).unwrap_or(u64::MAX),
        u64::try_from(output.get_array_memory_size()).unwrap_or(u64::MAX),
    );
    Batch {
        header,
        payload: BatchPayload::in_memory(output),
    }
    .with_retention(cdf_kernel::PayloadRetention::new(
        Arc::new(lease),
        retained.max(1),
    )?)
}

fn transform_source(
    upstream: Arc<ObservedByteSource>,
    path: &str,
    transforms: &ByteTransformRegistry,
    memory: Arc<dyn cdf_memory::MemoryCoordinator>,
) -> Result<Arc<dyn ByteSource>> {
    let Some(extension) = path.rsplit('.').next().map(str::to_ascii_lowercase) else {
        return Ok(upstream);
    };
    let Some(transform) = transforms.by_extension(&extension) else {
        return Ok(upstream);
    };
    let maximum_expanded_bytes = transform.descriptor().maximum_expanded_bytes;
    let maximum_expansion_ratio = transform.descriptor().maximum_expansion_ratio;
    let preferred = (1024 * 1024_u64).clamp(
        upstream.capabilities().minimum_chunk_bytes,
        upstream.capabilities().maximum_chunk_bytes,
    );
    Ok(Arc::new(TransformedByteSource::new(
        upstream,
        transform,
        TransformSourceConfig {
            preferred_input_chunk_bytes: preferred,
            maximum_expanded_bytes,
            maximum_expansion_ratio,
            memory,
            consumer: ConsumerKey::new("glue-object-transform", MemoryClass::Transform)?,
        },
    )?))
}

fn transport_resource(path: &str, source: &GlueSourceOptions) -> Result<FileTransportResource> {
    let mut resource = FileTransportResource::remote_url(path.to_owned()).with_egress_allowlist(
        if source.egress_allowlist.is_empty() {
            EgressAllowlist::allow_any()
        } else {
            EgressAllowlist::from_hosts(source.egress_allowlist.clone())
        },
    );
    if let Some(reference) = &source.object_credentials {
        resource = resource.with_credentials(SecretUri::new(reference.clone())?);
    }
    Ok(resource)
}
