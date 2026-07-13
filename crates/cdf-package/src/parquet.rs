use std::{collections::BTreeSet, io::Write, sync::Arc};

use ::parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, TimeUnit};
use cdf_kernel::{CdfError, Result};

pub fn transcode_record_batches_to_parquet_bytes(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    validate_batches_for_parquet(batches)?;

    let schema = batches[0].schema();
    let properties = WriterProperties::builder()
        .set_created_by("cdf native arrow-rs parquet writer".to_owned())
        .build();
    let mut bytes = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))
            .map_err(|error| parquet_error("create native Parquet writer", error))?;
        write_parquet_batches(&mut writer, batches)?;
    }
    Ok(bytes)
}

pub(crate) fn transcode_record_batches_to_bounded_parquet_bytes(
    batches: &[RecordBatch],
    maximum_output_bytes: u64,
) -> Result<Vec<u8>> {
    validate_batches_for_parquet(batches)?;
    let mut bytes = BoundedParquetOutput::new(maximum_output_bytes)?;
    let schema = batches[0].schema();
    let properties = WriterProperties::builder()
        .set_created_by("cdf native arrow-rs parquet writer".to_owned())
        .build();
    {
        let mut writer = ArrowWriter::try_new(&mut bytes, schema, Some(properties))
            .map_err(|error| parquet_error("create bounded native Parquet writer", error))?;
        write_parquet_batches(&mut writer, batches)?;
    }
    Ok(bytes.into_inner())
}

fn validate_batches_for_parquet(batches: &[RecordBatch]) -> Result<()> {
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
    validate_parquet_schema(schema.as_ref())?;
    Ok(())
}

fn write_parquet_batches<W: Write + Send>(
    writer: &mut ArrowWriter<W>,
    batches: &[RecordBatch],
) -> Result<()> {
    for batch in batches {
        writer
            .write(batch)
            .map_err(|error| parquet_error("write native Parquet record batch", error))?;
    }
    writer
        .finish()
        .map_err(|error| parquet_error("finish native Parquet writer", error))?;
    Ok(())
}

struct BoundedParquetOutput {
    bytes: Vec<u8>,
    maximum_bytes: usize,
}

impl BoundedParquetOutput {
    fn new(maximum_bytes: u64) -> Result<Self> {
        let maximum_bytes = usize::try_from(maximum_bytes)
            .map_err(|_| CdfError::data("Parquet output window exceeds addressable memory"))?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(maximum_bytes).map_err(|error| {
            CdfError::data(format!(
                "reserve {maximum_bytes}-byte Parquet output window: {error}"
            ))
        })?;
        if bytes.capacity() > maximum_bytes {
            return Err(CdfError::data(format!(
                "Parquet output allocation retained {} bytes above its {maximum_bytes}-byte window",
                bytes.capacity()
            )));
        }
        Ok(Self {
            bytes,
            maximum_bytes,
        })
    }

    fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedParquetOutput {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let next_len = self
            .bytes
            .len()
            .checked_add(buffer.len())
            .ok_or_else(|| std::io::Error::other("Parquet output byte count overflow"))?;
        if next_len > self.maximum_bytes {
            return Err(std::io::Error::other(format!(
                "Parquet output exceeds its {}-byte package archive window",
                self.maximum_bytes
            )));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn validate_parquet_schema(schema: &arrow_schema::Schema) -> Result<()> {
    validate_field_names(schema.fields())?;
    for field in schema.fields() {
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
