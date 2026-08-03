use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_array::{
    Array, BinaryArray, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{CdfError, ResourceId, Result, with_physical_type};
use cdf_memory::MemoryCoordinator;
use cdf_runtime::{RunCancellation, SourceEgressScope};

use crate::{
    client::ClickHouseConnection,
    error::classify_clickhouse_error,
    identifier::ClickHouseIdentifier,
    memory::{
        CLICKHOUSE_CATALOG_METADATA_BYTES, arrow_stream_limits, reserve_catalog_metadata,
        reserve_cursor_state, reserve_decode, reserve_transport,
    },
    query::{QueryParameter, source_expression_with_cursor_cast},
    types::{
        ClickHouseCursorCast, cursor_cast_for_physical_type, validate_arrow_field,
        validate_clickhouse_type, with_cursor_cast,
    },
};

const MAXIMUM_CATALOG_COLUMNS: usize = 16_384;
const CATALOG_COLUMN_MODEL_BYTES: u64 = 256;
const CATALOG_TEXT_DUPLICATION_FACTOR: u64 = 4;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ClickHouseCatalogDiscovery {
    pub(crate) schema: Schema,
    pub(crate) source_identity: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CatalogColumn {
    name: String,
    physical_type: String,
}

pub(crate) async fn discover_clickhouse_table(
    connection: ClickHouseConnection,
    resource_id: ResourceId,
    table: ClickHouseIdentifier,
    cursor_field: Option<String>,
    memory: Arc<dyn MemoryCoordinator>,
    egress: SourceEgressScope,
    cancellation: RunCancellation,
) -> Result<ClickHouseCatalogDiscovery> {
    connection.validate()?;
    egress.authorize(&connection.endpoint)?;
    cancellation.check()?;

    // Catalog strings and the derived schema coexist after the Arrow batches are released. Keep
    // that bounded retained model under its own authority rather than charging it to one poll.
    let transport_lease = cancellation
        .await_or_cancel(reserve_transport(Arc::clone(&memory)))
        .await?;
    connection.install_transport_authority(transport_lease)?;
    let _catalog_metadata_lease = cancellation
        .await_or_cancel(reserve_catalog_metadata(Arc::clone(&memory)))
        .await?;
    let mut decode_lease = Some(
        cancellation
            .await_or_cancel(reserve_decode(Arc::clone(&memory)))
            .await?,
    );

    let catalog_limits = arrow_stream_limits(
        decode_lease.as_ref().ok_or_else(|| {
            CdfError::internal("ClickHouse catalog query lost its admitted decode authority")
        })?,
        MAXIMUM_CATALOG_COLUMNS,
    )?;
    let catalog_cursor_state = cancellation
        .await_or_cancel(reserve_cursor_state(Arc::clone(&memory)))
        .await?;
    let mut catalog = connection.arrow_query_with_max_block_rows(
        concat!(
            "SELECT name, type FROM system.columns ",
            "WHERE database = ? AND table = ? ORDER BY position"
        ),
        vec![
            QueryParameter::String(connection.database.as_str().to_owned()),
            QueryParameter::String(table.as_str().to_owned()),
        ],
        "open ClickHouse catalog Arrow stream",
        MAXIMUM_CATALOG_COLUMNS as u64,
        catalog_cursor_state,
        catalog_limits,
    )?;
    let mut columns = Vec::new();
    let mut catalog_metadata_bytes = 0_u64;
    loop {
        let poll_lease = decode_lease
            .take()
            .ok_or_else(|| CdfError::internal("ClickHouse catalog poll lost decode authority"))?;
        let batch = cancellation
            .await_or_cancel(async {
                catalog
                    .next()
                    .await
                    .map_err(|error| classify_clickhouse_error("read ClickHouse catalog", error))
            })
            .await?;
        let Some(batch) = batch else { break };
        decode_catalog_batch(&batch, &mut columns, &mut catalog_metadata_bytes)?;
        if columns.len() > MAXIMUM_CATALOG_COLUMNS {
            return Err(CdfError::data(format!(
                "ClickHouse catalog for resource `{resource_id}` exceeds the {MAXIMUM_CATALOG_COLUMNS}-column discovery bound"
            )));
        }
        drop(batch);
        drop(poll_lease);
        decode_lease = Some(
            cancellation
                .await_or_cancel(reserve_decode(Arc::clone(&memory)))
                .await?,
        );
    }
    if columns.is_empty() {
        return Err(CdfError::data(format!(
            "ClickHouse catalog for resource `{resource_id}` found no columns for table `{table}`"
        )));
    }
    drop(catalog);
    let mut unique = BTreeSet::new();
    for column in &columns {
        if !unique.insert(column.name.as_str()) {
            return Err(CdfError::data(format!(
                "ClickHouse catalog for resource `{resource_id}` repeats column `{}`",
                column.name
            )));
        }
        ClickHouseIdentifier::new(column.name.clone())?;
        validate_clickhouse_type(&column.name, &column.physical_type)?;
    }

    let schema_projection = columns
        .iter()
        .map(|column| {
            let identifier = ClickHouseIdentifier::new(column.name.clone())?;
            let cursor_cast = (cursor_field.as_deref() == Some(column.name.as_str()))
                .then(|| cursor_cast_for_physical_type(&column.physical_type))
                .flatten();
            Ok(format!(
                "{} AS {}",
                source_expression_with_cursor_cast(
                    &identifier,
                    Some(&column.physical_type),
                    cursor_cast,
                )?,
                identifier.quoted()
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let schema_sql = format!(
        "SELECT {} FROM {}.{} LIMIT 0",
        schema_projection.join(", "),
        connection.database.quoted(),
        table.quoted()
    );
    let schema_decode_lease = cancellation
        .await_or_cancel(reserve_decode(Arc::clone(&memory)))
        .await?;
    let schema_limits = arrow_stream_limits(&schema_decode_lease, 1)?;
    let schema_cursor_state = cancellation
        .await_or_cancel(reserve_cursor_state(memory))
        .await?;
    let mut schema_cursor = connection.arrow_query_with_max_block_rows(
        &schema_sql,
        Vec::new(),
        "open ClickHouse schema Arrow stream",
        1,
        schema_cursor_state,
        schema_limits,
    )?;
    let first = cancellation
        .await_or_cancel(async {
            schema_cursor
                .next()
                .await
                .map_err(|error| classify_clickhouse_error("read ClickHouse Arrow schema", error))
        })
        .await?;
    if first.as_ref().is_some_and(|batch| batch.num_rows() != 0) {
        return Err(CdfError::data(
            "ClickHouse LIMIT 0 schema probe returned source rows instead of schema-only output",
        ));
    }
    let arrow_schema = schema_cursor.schema().ok_or_else(|| {
        CdfError::data(format!(
            "ClickHouse Arrow schema probe for resource `{resource_id}` returned no schema"
        ))
    })?;
    drop(first);
    drop(schema_cursor);
    drop(schema_decode_lease);
    if arrow_schema.fields().len() != columns.len() {
        return Err(CdfError::data(format!(
            "ClickHouse catalog/Arrow schema column count differs for resource `{resource_id}`"
        )));
    }
    let fields = arrow_schema
        .fields()
        .iter()
        .zip(columns)
        .map(|(field, column)| {
            let cursor_cast = (cursor_field.as_deref() == Some(column.name.as_str()))
                .then(|| cursor_cast_for_physical_type(&column.physical_type))
                .flatten();
            if field.name() != &column.name {
                return Err(CdfError::data(format!(
                    "ClickHouse catalog column `{}` disagrees with Arrow field `{}` for resource `{resource_id}`",
                    column.name,
                    field.name()
                )));
            }
            let data_type = if column.physical_type == "UUID" {
                if field.data_type() != &DataType::Binary {
                    return Err(CdfError::data(format!(
                        "ClickHouse UUID field `{}` canonical cast produced unexpected Arrow type {:?}",
                        column.name,
                        field.data_type()
                    )));
                }
                DataType::Utf8
            } else {
                validate_arrow_field(field)?;
                field.data_type().clone()
            };
            if let Some(cursor_cast) = cursor_cast {
                let expected = match cursor_cast {
                    ClickHouseCursorCast::Signed64 => &DataType::Int64,
                    ClickHouseCursorCast::Unsigned64 => &DataType::UInt64,
                };
                if &data_type != expected {
                    return Err(CdfError::data(format!(
                        "ClickHouse cursor field `{}` canonical widening produced unexpected Arrow type {:?}",
                        column.name, data_type
                    )));
                }
            }
            let field = with_physical_type(
                Field::new(field.name(), data_type, field.is_nullable())
                    .with_metadata(field.metadata().clone()),
                column.physical_type,
            );
            Ok(cursor_cast.map_or(field.clone(), |cast| with_cursor_cast(field, cast)))
        })
        .collect::<Result<Vec<_>>>()?;
    let schema = Schema::new_with_metadata(fields, arrow_schema.metadata().clone());
    let source_identity = BTreeMap::from([
        ("source_kind".to_owned(), "clickhouse".to_owned()),
        ("dialect".to_owned(), "clickhouse".to_owned()),
        (
            "database".to_owned(),
            connection.database.as_str().to_owned(),
        ),
        ("table".to_owned(), table.as_str().to_owned()),
    ]);
    Ok(ClickHouseCatalogDiscovery {
        schema,
        source_identity,
    })
}

fn decode_catalog_batch(
    batch: &RecordBatch,
    output: &mut Vec<CatalogColumn>,
    retained_bytes: &mut u64,
) -> Result<()> {
    if batch.num_columns() != 2 {
        return Err(CdfError::data(
            "ClickHouse system.columns Arrow response must contain name and type",
        ));
    }
    for row in 0..batch.num_rows() {
        let name = text_at(batch.column(0).as_ref(), row, "name")?;
        let physical_type = text_at(batch.column(1).as_ref(), row, "type")?;
        let text_bytes = u64::try_from(name.len())
            .ok()
            .and_then(|name| {
                u64::try_from(physical_type.len())
                    .ok()
                    .and_then(|physical_type| name.checked_add(physical_type))
            })
            .ok_or_else(|| CdfError::data("ClickHouse catalog text size overflowed"))?;
        let column_bytes = text_bytes
            .checked_mul(CATALOG_TEXT_DUPLICATION_FACTOR)
            .and_then(|bytes| bytes.checked_add(CATALOG_COLUMN_MODEL_BYTES))
            .ok_or_else(|| CdfError::data("ClickHouse catalog model size overflowed"))?;
        let next_retained = retained_bytes
            .checked_add(column_bytes)
            .ok_or_else(|| CdfError::data("ClickHouse catalog model size overflowed"))?;
        if next_retained > CLICKHOUSE_CATALOG_METADATA_BYTES {
            return Err(CdfError::data(format!(
                "ClickHouse catalog metadata exceeds the {CLICKHOUSE_CATALOG_METADATA_BYTES}-byte retained discovery bound"
            )));
        }
        output.push(CatalogColumn {
            name: name.to_owned(),
            physical_type: physical_type.to_owned(),
        });
        *retained_bytes = next_retained;
    }
    Ok(())
}

fn text_at<'a>(array: &'a dyn Array, row: usize, label: &str) -> Result<&'a str> {
    if array.is_null(row) {
        return Err(CdfError::data(format!(
            "ClickHouse catalog returned NULL for column {label}"
        )));
    }
    if let Some(array) = array.as_any().downcast_ref::<BinaryArray>() {
        return std::str::from_utf8(array.value(row))
            .map_err(|_| CdfError::data(format!("ClickHouse catalog {label} is not UTF-8")));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeBinaryArray>() {
        return std::str::from_utf8(array.value(row))
            .map_err(|_| CdfError::data(format!("ClickHouse catalog {label} is not UTF-8")));
    }
    if let Some(array) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(array.value(row));
    }
    if let Some(array) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(array.value(row));
    }
    Err(CdfError::data(format!(
        "ClickHouse catalog {label} has unexpected Arrow type {:?}",
        array.data_type()
    )))
}
