use std::sync::Arc;

use arrow_array::{
    Array, Date32Array, Int32Array, Int64Array, RecordBatch, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_kernel::{
    Batch, BatchId, CdfError, CursorPosition, CursorValue, PartitionPlan, PayloadRetention,
    ResourceDescriptor, Result, SourcePosition,
};
use cdf_memory::{
    ConsumerKey, MemoryClass, MemoryCoordinator, MemoryLease, ReservationRequest, reserve,
};
use cdf_runtime::{RunCancellation, SourceEgressScope, TaskStreamSender};
use futures::StreamExt;
use mongodb::{
    Client,
    bson::Document,
    options::{ClientOptions, Credential, ServerApi, ServerApiVersion},
};

use crate::{
    driver::MongoDbRuntimeConfig,
    error::classify_mongodb_error,
    identifier::MongoDbIdentifier,
    query::{build_query, field_by_name, scan_from_partition},
    resource::validate_resource_shape,
    schema::decode_batch_with_physical_schema,
};

pub(crate) const MONGODB_MAXIMUM_WIRE_BATCH_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MONGODB_MAXIMUM_OUTPUT_BATCH_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const MONGODB_MAXIMUM_DECODE_BYTES: u64 = 128 * 1024 * 1024;
const MONGODB_CLIENT_POOL_BYTES: u64 = 64 * 1024 * 1024;

pub(crate) struct MongoDbClientHandle {
    pub(crate) client: Client,
    _pool_lease: MemoryLease,
}

pub(crate) struct MongoDbExecutionInput {
    pub(crate) runtime: MongoDbRuntimeConfig,
    pub(crate) client: Arc<tokio::sync::OnceCell<MongoDbClientHandle>>,
    pub(crate) descriptor: ResourceDescriptor,
    pub(crate) schema: SchemaRef,
    pub(crate) decoder_schema: SchemaRef,
    pub(crate) physical_schema: SchemaRef,
    pub(crate) database: MongoDbIdentifier,
    pub(crate) collection: MongoDbIdentifier,
    pub(crate) batch_rows: u32,
    pub(crate) partition: PartitionPlan,
    pub(crate) memory: Arc<dyn MemoryCoordinator>,
    pub(crate) egress: SourceEgressScope,
}

pub(crate) async fn connect_mongodb(
    runtime: &MongoDbRuntimeConfig,
    memory: Arc<dyn MemoryCoordinator>,
    egress: &SourceEgressScope,
    cancellation: &RunCancellation,
) -> Result<MongoDbClientHandle> {
    egress.authorize(&runtime.endpoint)?;
    cancellation.check()?;
    let pool_lease = cancellation
        .await_or_cancel(reserve(
            memory,
            ReservationRequest::new(
                ConsumerKey::new("mongodb-client-pool", MemoryClass::Source)?,
                MONGODB_CLIENT_POOL_BYTES,
            )?,
        ))
        .await?;
    let mut options = cancellation
        .await_or_cancel(async {
            ClientOptions::parse(&runtime.endpoint)
                .await
                .map_err(|error| classify_mongodb_error("parse MongoDB endpoint", error))
        })
        .await?;
    options.app_name = Some("cdf".to_owned());
    options.max_pool_size = Some(runtime.max_pool_size);
    options.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
    if runtime.username.is_some() || runtime.password.is_some() {
        options.credential = Some(
            Credential::builder()
                .username(runtime.username.clone())
                .password(runtime.password.clone())
                .source(runtime.auth_source.clone())
                .build(),
        );
    }
    let client = Client::with_options(options)
        .map_err(|error| classify_mongodb_error("construct MongoDB client", error))?;
    Ok(MongoDbClientHandle {
        client,
        _pool_lease: pool_lease,
    })
}

pub(crate) async fn execute_mongodb_collection(
    input: MongoDbExecutionInput,
    mut sender: TaskStreamSender<Batch>,
    cancellation: RunCancellation,
) -> Result<()> {
    validate_resource_shape(&input.descriptor, &input.schema, &input.collection)?;
    let scan = scan_from_partition(
        &input.descriptor,
        &input.schema,
        &input.collection,
        &input.partition,
    )?;
    let query = build_query(&input.descriptor, &input.schema, &input.partition, &scan)?;
    let output_schema = projected_schema(&input.decoder_schema, &scan.projection)?;
    let physical_schema = projected_physical_schema(
        &input.physical_schema,
        &input.decoder_schema,
        &scan.projection,
    )?;
    let handle = input
        .client
        .get_or_try_init(|| {
            connect_mongodb(
                &input.runtime,
                Arc::clone(&input.memory),
                &input.egress,
                &cancellation,
            )
        })
        .await?;
    let collection = handle
        .client
        .database(input.database.as_str())
        .collection::<Document>(input.collection.as_str());
    let cursor_lease = cancellation
        .await_or_cancel(reserve(
            Arc::clone(&input.memory),
            ReservationRequest::new(
                ConsumerKey::new("mongodb-raw-cursor", MemoryClass::Decode)?,
                MONGODB_MAXIMUM_WIRE_BATCH_BYTES,
            )?,
        ))
        .await?;
    let mut find = collection.find(query.filter).batch_size(input.batch_rows);
    if !query.sort.is_empty() {
        find = find.sort(query.sort);
    }
    if let Some(limit) = query.limit {
        find = find.limit(limit);
    }
    let mut cursor = cancellation
        .await_or_cancel(async {
            find.batch()
                .await
                .map_err(|error| classify_mongodb_error("open MongoDB raw BSON cursor", error))
        })
        .await?;
    let mut batch_index = 0_u64;
    let mut source_row_offset = 0_u64;
    while let Some(raw_batch) = cancellation
        .await_or_cancel(async {
            cursor
                .next()
                .await
                .transpose()
                .map_err(|error| classify_mongodb_error("read MongoDB raw BSON cursor", error))
        })
        .await?
    {
        cancellation.check()?;
        let raw_bytes = u64::try_from(raw_batch.as_raw_document().as_bytes().len())
            .map_err(|_| CdfError::internal("MongoDB raw batch size exceeds u64"))?;
        if raw_bytes > MONGODB_MAXIMUM_WIRE_BATCH_BYTES {
            return Err(CdfError::data(format!(
                "MongoDB wire batch contains {raw_bytes} bytes beyond the {MONGODB_MAXIMUM_WIRE_BATCH_BYTES}-byte admitted bound"
            )));
        }
        let documents = raw_batch
            .doc_slices()
            .map_err(|error| classify_mongodb_error("decode MongoDB raw batch envelope", error))?
            .into_iter()
            .map(|value| {
                value
                    .map_err(|error| {
                        CdfError::data(format!(
                            "MongoDB raw batch contains malformed BSON: {error}"
                        ))
                    })?
                    .as_document()
                    .ok_or_else(|| CdfError::data("MongoDB raw batch item is not a document"))
            })
            .collect::<Result<Vec<_>>>()?;
        if documents.is_empty() {
            continue;
        }
        let output_lease = cancellation
            .await_or_cancel(reserve(
                Arc::clone(&input.memory),
                ReservationRequest::new(
                    ConsumerKey::new("mongodb-arrow-decode", MemoryClass::Decode)?,
                    MONGODB_MAXIMUM_DECODE_BYTES,
                )?,
            ))
            .await?;
        let decoded = decode_batch_with_physical_schema(
            Arc::clone(&input.decoder_schema),
            Arc::clone(&output_schema),
            Arc::clone(&physical_schema),
            &documents,
            source_row_offset,
        )?;
        let progressive_evidence_bytes = decoded.pre_contract_evidence_bytes;
        let record_batch = decoded.record_batch;
        let retained_bytes = cdf_memory::record_batch_retained_bytes(&record_batch)?;
        batch_index = batch_index.saturating_add(1);
        let source_position =
            batch_cursor_position(&input.descriptor, &scan.projection, &record_batch)?;
        let mut batch = Batch::from_record_batch(
            BatchId::new(format!(
                "{}-mongodb-{batch_index:06}",
                sanitize_id_part(input.descriptor.resource_id.as_str())
            ))?,
            input.descriptor.resource_id.clone(),
            input.partition.partition_id.clone(),
            cdf_kernel::canonical_arrow_schema_hash(&decoded.physical_schema)?,
            record_batch,
        )?;
        batch
            .header
            .mark_materialized_output(&decoded.physical_schema)?;
        batch
            .header
            .extend_residual_candidates(decoded.residual_candidates);
        batch
            .header
            .extend_physical_reconciliations(decoded.physical_reconciliations);
        batch.header.mark_materialized_residuals_complete();
        batch.header.source_position = source_position;
        let evidence_bytes =
            progressive_evidence_bytes.max(batch.header.pre_contract_evidence_retained_bytes()?);
        let retained_total = retained_bytes.checked_add(evidence_bytes).ok_or_else(|| {
            CdfError::internal("MongoDB decoded batch retained-memory accounting overflow")
        })?;
        if retained_bytes == 0 || retained_total > MONGODB_MAXIMUM_OUTPUT_BATCH_BYTES {
            return Err(CdfError::data(format!(
                "MongoDB Arrow batch and drift evidence retain {retained_total} bytes outside the compiled 1..={MONGODB_MAXIMUM_OUTPUT_BATCH_BYTES}-byte bound; reduce batch_rows or project fewer fields"
            )));
        }
        output_lease.reconcile(retained_total)?;
        let batch = batch.with_retention(PayloadRetention::new(
            Arc::new(output_lease),
            retained_total,
        )?)?;
        sender.send(batch).await?;
        source_row_offset = source_row_offset.saturating_add(documents.len() as u64);
    }
    drop(cursor_lease);
    Ok(())
}

fn projected_schema(schema: &SchemaRef, projection: &[String]) -> Result<SchemaRef> {
    let fields = projection
        .iter()
        .map(|name| {
            field_by_name(schema, name).cloned().ok_or_else(|| {
                CdfError::contract(format!(
                    "MongoDB projection field `{name}` disappeared from the pinned schema"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // Schema-level reconciliation metadata is compile-time evidence. This source has already
    // materialized the effective field names and types, while the batch header carries the exact
    // physical observation that justified them. Do not publish the plan as untrusted Arrow
    // metadata on the runtime batch.
    Ok(Arc::new(Schema::new(fields)))
}

fn projected_physical_schema(
    physical_schema: &SchemaRef,
    decoder_schema: &SchemaRef,
    projection: &[String],
) -> Result<SchemaRef> {
    let fields = projection
        .iter()
        .map(|logical_name| {
            let decoder = field_by_name(decoder_schema, logical_name).ok_or_else(|| {
                CdfError::contract(format!(
                    "MongoDB decoder projection field `{logical_name}` disappeared"
                ))
            })?;
            let source = cdf_kernel::source_name(decoder).unwrap_or_else(|| decoder.name());
            physical_schema
                .fields()
                .iter()
                .find(|field| {
                    field.name() == source
                        || cdf_kernel::source_name(field.as_ref()) == Some(source)
                })
                .cloned()
                .ok_or_else(|| {
                    CdfError::data(format!(
                        "MongoDB physical observation omitted projected source field `{source}`"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        physical_schema.metadata().clone(),
    )))
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
        .ok_or_else(|| CdfError::contract("MongoDB cursor projection disappeared"))?;
    let row = batch
        .num_rows()
        .checked_sub(1)
        .ok_or_else(|| CdfError::internal("MongoDB cursor batch is empty"))?;
    let field = batch.schema().field(index).clone();
    let value = cursor_value(&field, batch.column(index).as_ref(), row)?;
    Ok(Some(SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: cursor.field.clone(),
        value,
    })))
}

pub(crate) fn cursor_value(field: &Field, array: &dyn Array, row: usize) -> Result<CursorValue> {
    if array.is_null(row) {
        return Err(CdfError::data(format!(
            "MongoDB cursor field `{}` produced NULL",
            field.name()
        )));
    }
    match field.data_type() {
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|array| CursorValue::I64(i64::from(array.value(row)))),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|array| CursorValue::I64(array.value(row))),
        DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|array| CursorValue::I64(i64::from(array.value(row)))),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(|array| {
                array
                    .value(row)
                    .checked_mul(1_000)
                    .map(|micros| CursorValue::TimestampMicros {
                        micros,
                        timezone: timezone.as_deref().map(str::to_owned),
                    })
                    .ok_or_else(|| {
                        CdfError::data(
                            "MongoDB DateTime cursor exceeds the durable microsecond position domain",
                        )
                    })
            })
            .transpose()?,
        _ => None,
    }
    .ok_or_else(|| {
        CdfError::data(format!(
            "MongoDB cursor field `{}` has incompatible Arrow array {:?}",
            field.name(),
            array.data_type()
        ))
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
