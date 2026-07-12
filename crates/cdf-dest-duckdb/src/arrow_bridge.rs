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
    use std::sync::Arc;

    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
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
}
