use std::{mem::ManuallyDrop, ptr};

use arrow_array::{RecordBatch, RecordBatchIterator, ffi_stream::FFI_ArrowArrayStream};
use duckdb::arrow::{
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream as DuckFfiArrowArrayStream},
    record_batch::RecordBatch as DuckRecordBatch,
};

use crate::*;

const _: () = assert!(
    std::mem::size_of::<FFI_ArrowArrayStream>() == std::mem::size_of::<DuckFfiArrowArrayStream>()
);
const _: () = assert!(
    std::mem::align_of::<FFI_ArrowArrayStream>() == std::mem::align_of::<DuckFfiArrowArrayStream>()
);

/// Moves one Arrow 59 batch through the stable Arrow C Stream ABI into the
/// Arrow 58 type consumed by the pinned DuckDB binding without copying buffers.
pub(crate) fn into_duckdb_batch(batch: RecordBatch) -> Result<DuckRecordBatch> {
    let schema = batch.schema();
    let reader = RecordBatchIterator::new(std::iter::once(Ok(batch)), schema);
    let source = ManuallyDrop::new(FFI_ArrowArrayStream::new(Box::new(reader)));

    // SAFETY: Both values are the Arrow C Stream ABI's #[repr(C)] five-pointer
    // structure. Compile-time assertions above prove equal size/alignment for
    // the pinned Arrow 59/58 tuple. `source` is ManuallyDrop, so the pointer read
    // transfers its sole release callback/private-data ownership to `stream`;
    // Arrow 58's reader then invokes the Arrow 59 callbacks through the C ABI.
    let stream = unsafe {
        ptr::read((&*source as *const FFI_ArrowArrayStream).cast::<DuckFfiArrowArrayStream>())
    };
    let mut reader = ArrowArrayStreamReader::try_new(stream)
        .map_err(|error| CdfError::data(format!("import Arrow batch into DuckDB ABI: {error}")))?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|error| CdfError::data(format!("read Arrow batch through DuckDB ABI: {error}")))?
        .ok_or_else(|| CdfError::internal("Arrow C Stream bridge returned no batch"))?;
    if reader.next().is_some() {
        return Err(CdfError::internal(
            "Arrow C Stream bridge returned more than one batch",
        ));
    }
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Instant};

    use arrow_array::{
        Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
        TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use duckdb::{appender_params_from_iter, types::Value};
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn bridge_appends_batch_without_scalar_rows() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE target (id INTEGER, name VARCHAR)")
            .unwrap();
        let mut appender = conn.appender("target").unwrap();
        appender
            .append_record_batch(into_duckdb_batch(batch).unwrap())
            .unwrap();
        appender.flush().unwrap();
        drop(appender);
        let count: u64 = conn
            .query_row("SELECT count(*) FROM target", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 3);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn c_stream_bridge_preserves_lengths_and_nulls(
            values in prop::collection::vec(prop::option::of(any::<i32>()), 0..4096),
        ) {
            let expected_nulls = values.iter().filter(|value| value.is_none()).count();
            let batch = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, true)])),
                vec![Arc::new(Int32Array::from(values))],
            ).unwrap();
            let imported = into_duckdb_batch(batch).unwrap();
            prop_assert_eq!(imported.num_rows(), imported.column(0).len());
            prop_assert_eq!(imported.column(0).null_count(), expected_nulls);
        }
    }

    fn tlc_batch(rows: usize) -> RecordBatch {
        let mut fields = vec![
            Field::new("vendor_id", DataType::Int32, false),
            Field::new(
                "tpep_pickup_datetime",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "tpep_dropoff_datetime",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("passenger_count", DataType::Int64, false),
            Field::new("trip_distance", DataType::Float64, false),
            Field::new("ratecode_id", DataType::Int64, false),
            Field::new("store_and_fwd_flag", DataType::Utf8, false),
            Field::new("pu_location_id", DataType::Int32, false),
            Field::new("do_location_id", DataType::Int32, false),
            Field::new("payment_type", DataType::Int64, false),
        ];
        fields.extend(
            [
                "fare_amount",
                "extra",
                "mta_tax",
                "tip_amount",
                "tolls_amount",
                "improvement_surcharge",
                "total_amount",
                "congestion_surcharge",
                "airport_fee",
            ]
            .map(|name| Field::new(name, DataType::Float64, false)),
        );
        let int32 = || {
            Arc::new(Int32Array::from_iter_values(
                (0..rows).map(|row| (row % 265) as i32),
            )) as Arc<dyn Array>
        };
        let int64 = || {
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|row| (row % 8) as i64),
            )) as Arc<dyn Array>
        };
        let float64 = || {
            Arc::new(Float64Array::from_iter_values(
                (0..rows).map(|row| (row % 10_000) as f64 / 100.0),
            )) as Arc<dyn Array>
        };
        let timestamp = || {
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                (0..rows).map(|row| 1_704_067_200_000_000_i64 + row as i64),
            )) as Arc<dyn Array>
        };
        let mut columns = vec![
            int32(),
            timestamp(),
            timestamp(),
            int64(),
            float64(),
            int64(),
            Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                "N", rows,
            ))) as Arc<dyn Array>,
            int32(),
            int32(),
            int64(),
        ];
        columns.extend((0..9).map(|_| float64()));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    #[ignore = "performance lab benchmark; run explicitly in release mode"]
    fn arrow_appender_tlc_envelope_benchmark() {
        const BATCH_ROWS: usize = 65_536;
        const BATCHES: usize = 16;
        const SCALAR_ROWS: usize = 262_144;
        let batch = tlc_batch(BATCH_ROWS);
        let user_fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| crate::package::field_plan(field).unwrap())
            .collect::<Vec<_>>();
        let persisted_fields = crate::package::persistence_fields(&user_fields);
        let package_hash =
            cdf_kernel::PackageHash::new(format!("sha256:{}", "a".repeat(64))).unwrap();
        let segment_id = cdf_kernel::SegmentId::new("segment-000001").unwrap();

        let vector_conn = Connection::open_in_memory().unwrap();
        vector_conn
            .execute_batch(&format!(
                "CREATE TABLE target ({}); CREATE TEMP TABLE ingress ({}, {} UBIGINT NOT NULL)",
                crate::table::create_target_columns_sql(&persisted_fields),
                crate::table::create_columns_sql(&user_fields),
                crate::sql::quote_ident(CDF_ROW_COLUMN),
            ))
            .unwrap();
        let target = crate::api::TargetRef {
            schema: MAIN_SCHEMA.to_owned(),
            table: "target".to_owned(),
        };
        let ingress = crate::api::TargetRef {
            schema: MAIN_SCHEMA.to_owned(),
            table: "ingress".to_owned(),
        };
        let started = Instant::now();
        for ordinal in 0..BATCHES {
            let persisted =
                crate::package::ingress_batch(batch.clone(), (ordinal * BATCH_ROWS) as u64, None)
                    .unwrap();
            crate::commit::append_arrow_batch_to_table(&vector_conn, &ingress, persisted).unwrap();
        }
        crate::commit::transfer_ingress_segment(
            &vector_conn,
            crate::commit::IngressSegmentTransfer {
                ingress: &ingress,
                target: &target,
                persisted_fields: &persisted_fields,
                user_field_count: user_fields.len(),
                package_hash: &package_hash,
                segment_id: &segment_id,
                include_stage_order: false,
            },
        )
        .unwrap();
        let vector_elapsed = started.elapsed();
        let vector_rows = (BATCH_ROWS * BATCHES) as f64;
        let vector_rows_per_second = vector_rows / vector_elapsed.as_secs_f64();

        let scalar_conn = Connection::open_in_memory().unwrap();
        scalar_conn
            .execute_batch(&format!(
                "CREATE TABLE target ({})",
                crate::table::create_target_columns_sql(&persisted_fields)
            ))
            .unwrap();
        let names = persisted_fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        let mut appender = scalar_conn.appender_with_columns("target", &names).unwrap();
        let schema = batch.schema();
        let started = Instant::now();
        let materialized = (0..SCALAR_ROWS)
            .map(|row| {
                let source_row = row % BATCH_ROWS;
                batch
                    .columns()
                    .iter()
                    .zip(schema.fields())
                    .map(|(array, field)| {
                        crate::rows::cell_value(array.as_ref(), field.data_type(), source_row)
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let persisted = materialized
            .into_iter()
            .enumerate()
            .map(|(row, values)| {
                let mut values = values.clone();
                values.extend([
                    crate::api::CellValue {
                        value: Value::Text(package_hash.to_string()),
                        key: crate::api::CellKey::Text(package_hash.to_string()),
                    },
                    crate::api::CellValue {
                        value: Value::Text(segment_id.to_string()),
                        key: crate::api::CellKey::Text(segment_id.to_string()),
                    },
                    crate::api::CellValue {
                        value: Value::UBigInt(row as u64),
                        key: crate::api::CellKey::U64(row as u64),
                    },
                ]);
                values
            })
            .collect::<Vec<_>>();
        for row in persisted {
            let values = row
                .iter()
                .map(|cell| cell.value.clone())
                .collect::<Vec<_>>();
            appender
                .append_row(appender_params_from_iter(values))
                .unwrap();
        }
        appender.flush().unwrap();
        let scalar_elapsed = started.elapsed();
        let scalar_rows_per_second = SCALAR_ROWS as f64 / scalar_elapsed.as_secs_f64();
        let speedup = vector_rows_per_second / scalar_rows_per_second;
        eprintln!(
            "duckdb_tlc_arrow rows_per_second={vector_rows_per_second:.0} scalar_rows_per_second={scalar_rows_per_second:.0} speedup={speedup:.2}x"
        );
        assert!(
            vector_rows_per_second >= 1_000_000.0,
            "Arrow appender produced only {vector_rows_per_second:.0} rows/s"
        );
    }
}
