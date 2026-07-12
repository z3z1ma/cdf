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
        Array, Decimal128Array, Float64Array, Int32Array, Int64Array, ListArray, RecordBatch,
        StringArray, StructArray, TimestampMicrosecondArray, types::Int32Type,
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

    #[test]
    fn arrow_appender_preserves_decimal_and_nested_batches() {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
        ]);
        let struct_fields = vec![
            Arc::new(Field::new("code", DataType::Int32, false)),
            Arc::new(Field::new("label", DataType::Utf8, true)),
        ];
        let structure = StructArray::new(
            struct_fields.clone().into(),
            vec![
                Arc::new(Int32Array::from(vec![7, 8])),
                Arc::new(StringArray::from(vec![Some("seven"), None])),
            ],
            None,
        );
        let decimal = Decimal128Array::from(vec![12345_i128, -6789_i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("amount", DataType::Decimal128(10, 2), false),
                Field::new("items", list.data_type().clone(), false),
                Field::new("detail", DataType::Struct(struct_fields.into()), false),
            ])),
            vec![Arc::new(decimal), Arc::new(list), Arc::new(structure)],
        )
        .unwrap();
        let fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| crate::package::field_plan(field).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(fields[0].sql_type, "DECIMAL(10,2)");
        assert_eq!(fields[1].sql_type, "INTEGER[]");
        assert_eq!(
            fields[2].sql_type,
            "STRUCT(\"code\" INTEGER, \"label\" VARCHAR)"
        );
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Utf8, false)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert_eq!(
            crate::package::duckdb_type(&map_type).unwrap(),
            "MAP(VARCHAR, BIGINT)"
        );

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(&format!(
            "CREATE TABLE target ({})",
            crate::table::create_columns_sql(&fields)
        ))
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
        assert_eq!(count, 2);
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
        const SCALAR_ROWS: usize = BATCH_ROWS * BATCHES;
        let batch = tlc_batch(BATCH_ROWS);
        let user_fields = batch
            .schema()
            .fields()
            .iter()
            .map(|field| crate::package::field_plan(field).unwrap())
            .collect::<Vec<_>>();
        let persisted_fields = crate::package::persistence_fields(&user_fields);
        let vector_conn = Connection::open_in_memory().unwrap();
        vector_conn
            .execute_batch(&format!(
                "CREATE TABLE target ({})",
                crate::table::create_target_columns_sql(&persisted_fields),
            ))
            .unwrap();
        let target = crate::api::TargetRef {
            schema: MAIN_SCHEMA.to_owned(),
            table: "target".to_owned(),
        };
        vector_conn.execute_batch("BEGIN TRANSACTION").unwrap();
        let started = Instant::now();
        for ordinal in 0..BATCHES {
            let row_key_start = u64::try_from(ordinal * BATCH_ROWS).unwrap() + 1;
            let persisted =
                crate::package::persistence_batch(batch.clone(), row_key_start, None).unwrap();
            crate::commit::append_arrow_batch_to_table(&vector_conn, &target, persisted).unwrap();
        }
        vector_conn.execute_batch("COMMIT").unwrap();
        let vector_elapsed = started.elapsed();
        let vector_rows = (BATCH_ROWS * BATCHES) as f64;
        let vector_rows_per_second = vector_rows / vector_elapsed.as_secs_f64();

        let direct_conn = Connection::open_in_memory().unwrap();
        direct_conn
            .execute_batch(&format!(
                "CREATE TABLE target ({})",
                crate::table::create_columns_sql(&user_fields)
            ))
            .unwrap();
        direct_conn.execute_batch("BEGIN TRANSACTION").unwrap();
        let started = Instant::now();
        for _ in 0..BATCHES {
            let mut appender = direct_conn.appender("target").unwrap();
            appender
                .append_record_batch(into_duckdb_batch(batch.clone()).unwrap())
                .unwrap();
            appender.flush().unwrap();
        }
        direct_conn.execute_batch("COMMIT").unwrap();
        let direct_rows_per_second = vector_rows / started.elapsed().as_secs_f64();

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
        scalar_conn.execute_batch("BEGIN TRANSACTION").unwrap();
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
                values.push(crate::api::CellValue {
                    value: Value::UBigInt(u64::try_from(row).unwrap() + 1),
                    key: crate::api::CellKey::U64(u64::try_from(row).unwrap() + 1),
                });
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
        drop(appender);
        scalar_conn.execute_batch("COMMIT").unwrap();
        let scalar_elapsed = started.elapsed();
        let scalar_rows_per_second = SCALAR_ROWS as f64 / scalar_elapsed.as_secs_f64();
        let speedup = vector_rows_per_second / scalar_rows_per_second;
        eprintln!(
            "duckdb_tlc_arrow rows_per_second={vector_rows_per_second:.0} direct_rows_per_second={direct_rows_per_second:.0} scalar_rows_per_second={scalar_rows_per_second:.0} speedup={speedup:.2}x"
        );
        assert!(
            vector_rows_per_second >= 1_000_000.0,
            "Arrow appender produced only {vector_rows_per_second:.0} rows/s"
        );
        assert!(
            speedup >= 5.0,
            "Arrow appender speedup was only {speedup:.2}x"
        );
    }
}
