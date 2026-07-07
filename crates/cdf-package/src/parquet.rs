use std::{collections::BTreeSet, fs, sync::Arc};

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, TimestampMicrosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, TimeUnit};
use cdf_kernel::{CdfError, Result};
use duckdb::{
    Connection, appender_params_from_iter,
    types::{TimeUnit as DuckTimeUnit, Value},
};

const SEGMENT_TABLE: &str = "cdf_segment";

pub fn transcode_record_batches_to_parquet_bytes(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    if batches.is_empty() {
        return Err(CdfError::data(
            "Parquet transcode requires at least one record batch",
        ));
    }

    let schema = batches[0].schema();
    for batch in batches {
        if batch.schema().as_ref() != schema.as_ref() {
            return Err(CdfError::data(
                "Parquet transcode requires all record batches to share one schema",
            ));
        }
    }

    let conn = Connection::open_in_memory()
        .map_err(|error| duckdb_error("open in-memory DuckDB writer", error))?;
    conn.execute_batch(&create_table_sql(schema.fields())?)
        .map_err(|error| duckdb_error("create DuckDB Parquet staging table", error))?;
    append_batches(&conn, batches)?;

    let temp_dir = tempfile::tempdir()
        .map_err(|error| io_error("create temporary Parquet directory", error))?;
    let parquet_path = temp_dir.path().join("segment.parquet");
    let parquet_path = parquet_path
        .to_str()
        .ok_or_else(|| CdfError::destination("temporary Parquet path is not valid UTF-8"))?;
    conn.execute_batch(&format!(
        "COPY (SELECT * FROM {}) TO {} (FORMAT PARQUET)",
        quote_ident(SEGMENT_TABLE),
        sql_string(parquet_path)
    ))
    .map_err(|error| duckdb_error("export DuckDB table as Parquet", error))?;
    fs::read(parquet_path).map_err(|error| io_error("read temporary Parquet export", error))
}

fn append_batches(conn: &Connection, batches: &[RecordBatch]) -> Result<()> {
    let mut appender = conn
        .appender(SEGMENT_TABLE)
        .map_err(|error| duckdb_error("open DuckDB Parquet staging appender", error))?;
    for batch in batches {
        for row in 0..batch.num_rows() {
            let values = row_values(batch, row)?;
            appender
                .append_row(appender_params_from_iter(values))
                .map_err(|error| {
                    duckdb_error("append row to DuckDB Parquet staging table", error)
                })?;
        }
    }
    appender
        .flush()
        .map_err(|error| duckdb_error("flush DuckDB Parquet staging appender", error))
}

fn row_values(batch: &RecordBatch, row: usize) -> Result<Vec<Value>> {
    batch
        .columns()
        .iter()
        .zip(batch.schema().fields().iter())
        .map(|(array, field)| cell_value(array.as_ref(), field.data_type(), row))
        .collect()
}

fn cell_value(array: &dyn Array, data_type: &DataType, row: usize) -> Result<Value> {
    if array.is_null(row) {
        return Ok(Value::Null);
    }

    macro_rules! primitive {
        ($array_ty:ty, $value:expr) => {{
            let array = array.as_any().downcast_ref::<$array_ty>().ok_or_else(|| {
                CdfError::internal(format!("Arrow array downcast failed for {data_type:?}"))
            })?;
            Ok($value(array.value(row)))
        }};
    }

    match data_type {
        DataType::Boolean => primitive!(BooleanArray, Value::Boolean),
        DataType::Int8 => primitive!(Int8Array, Value::TinyInt),
        DataType::Int16 => primitive!(Int16Array, Value::SmallInt),
        DataType::Int32 => primitive!(Int32Array, Value::Int),
        DataType::Int64 => primitive!(Int64Array, Value::BigInt),
        DataType::UInt8 => primitive!(UInt8Array, Value::UTinyInt),
        DataType::UInt16 => primitive!(UInt16Array, Value::USmallInt),
        DataType::UInt32 => primitive!(UInt32Array, Value::UInt),
        DataType::UInt64 => primitive!(UInt64Array, Value::UBigInt),
        DataType::Float32 => primitive!(Float32Array, Value::Float),
        DataType::Float64 => primitive!(Float64Array, Value::Double),
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| CdfError::internal("Arrow Utf8 downcast failed"))?;
            Ok(Value::Text(array.value(row).to_owned()))
        }
        DataType::LargeUtf8 => {
            let array = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| CdfError::internal("Arrow LargeUtf8 downcast failed"))?;
            Ok(Value::Text(array.value(row).to_owned()))
        }
        DataType::Binary => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| CdfError::internal("Arrow Binary downcast failed"))?;
            Ok(Value::Blob(array.value(row).to_vec()))
        }
        DataType::LargeBinary => {
            let array = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| CdfError::internal("Arrow LargeBinary downcast failed"))?;
            Ok(Value::Blob(array.value(row).to_vec()))
        }
        DataType::Date32 => primitive!(Date32Array, Value::Date32),
        DataType::Timestamp(TimeUnit::Microsecond, None) => primitive!(
            TimestampMicrosecondArray,
            |value| Value::Timestamp(DuckTimeUnit::Microsecond, value)
        ),
        other => Err(CdfError::contract(format!(
            "Parquet destination does not support Arrow type {other:?}"
        ))),
    }
}

fn create_table_sql(fields: &[Arc<Field>]) -> Result<String> {
    validate_field_names(fields)?;
    let columns = fields
        .iter()
        .map(|field| {
            let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
            Ok(format!(
                "{} {}{}",
                quote_ident(field.name()),
                duckdb_type(field.data_type())?,
                nullable
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    Ok(format!(
        "CREATE TABLE {} ({columns})",
        quote_ident(SEGMENT_TABLE)
    ))
}

fn validate_field_names(fields: &[Arc<Field>]) -> Result<()> {
    let mut seen = BTreeSet::new();
    for field in fields {
        if !seen.insert(field.name()) {
            return Err(CdfError::contract(format!(
                "duplicate Parquet column name {}",
                field.name()
            )));
        }
    }
    Ok(())
}

fn duckdb_type(data_type: &DataType) -> Result<&'static str> {
    let ty = match data_type {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Binary | DataType::LargeBinary => "BLOB",
        DataType::Date32 => "DATE",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "TIMESTAMP",
        other => {
            return Err(CdfError::contract(format!(
                "Parquet destination does not support Arrow type {other:?}"
            )));
        }
    };
    Ok(ty)
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn duckdb_error(context: impl Into<String>, error: duckdb::Error) -> CdfError {
    CdfError::destination(format!("{}: {}", context.into(), error))
}

fn io_error(context: impl Into<String>, error: std::io::Error) -> CdfError {
    CdfError::destination(format!("{}: {}", context.into(), error))
}
