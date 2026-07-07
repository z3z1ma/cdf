use std::{collections::BTreeSet, sync::Arc};

use ::parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, TimeUnit};
use cdf_kernel::{CdfError, Result};

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
    validate_fields(schema.fields())?;

    let properties = WriterProperties::builder()
        .set_created_by("cdf native arrow-rs parquet writer".to_owned())
        .build();
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))
            .map_err(|error| parquet_error("create native Parquet writer", error))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|error| parquet_error("write native Parquet record batch", error))?;
        }
        writer
            .close()
            .map_err(|error| parquet_error("finish native Parquet writer", error))?;
    }
    Ok(bytes)
}

fn validate_fields(fields: &[Arc<Field>]) -> Result<()> {
    validate_field_names(fields)?;
    for field in fields {
        validate_parquet_type(field.data_type())?;
    }
    Ok(())
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

fn validate_parquet_type(data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::Date32
        | DataType::Timestamp(TimeUnit::Microsecond, None) => Ok(()),
        other => Err(CdfError::contract(format!(
            "Parquet destination does not support Arrow type {other:?}"
        ))),
    }
}

fn parquet_error(context: impl Into<String>, error: impl std::fmt::Display) -> CdfError {
    CdfError::destination(format!("{}: {}", context.into(), error))
}
