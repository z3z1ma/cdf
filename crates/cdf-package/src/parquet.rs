use std::{collections::BTreeSet, io::Write, sync::Arc};

use ::parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, IntervalUnit, TimeUnit};
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
    if schema.fields().is_empty() {
        return Err(CdfError::contract(
            "Parquet requires at least one Arrow field",
        ));
    }
    validate_field_names(schema.fields())?;
    for field in schema.fields() {
        validate_parquet_field(field.name(), field.data_type())?;
    }
    let writer = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        ArrowWriter::try_new(std::io::sink(), Arc::new(schema.clone()), None)
    }))
    .map_err(|_| {
        CdfError::contract(
            "pinned arrow-rs Parquet writer panicked while validating the planned Arrow schema; report this as a codec defect and cast the field or choose another destination",
        )
    })?;
    let _writer = writer.map_err(|error| {
            CdfError::contract(format!(
                "pinned arrow-rs Parquet writer cannot encode the planned Arrow schema: {error}; cast the named field to a supported Arrow type or choose a destination that preserves it"
            ))
        })?;
    Ok(())
}

fn validate_parquet_field(path: &str, data_type: &DataType) -> Result<()> {
    match data_type {
        DataType::Time32(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Time64(TimeUnit::Second | TimeUnit::Millisecond) => {
            Err(CdfError::contract(format!(
                "Parquet cannot encode field `{path}` with invalid Arrow time unit {data_type}; Time32 requires second/millisecond and Time64 requires microsecond/nanosecond"
            )))
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => Err(CdfError::contract(format!(
            "Parquet cannot losslessly encode field `{path}` with Arrow interval month-day-nanosecond; cast it to a supported interval representation or choose a destination that preserves nanosecond intervals"
        ))),
        DataType::Union(_, _) => Err(CdfError::contract(format!(
            "Parquet cannot encode field `{path}` with Arrow union type; project the union into supported typed fields or a governed variant column"
        ))),
        DataType::RunEndEncoded(_, _) => Err(CdfError::contract(format!(
            "Parquet cannot encode field `{path}` with Arrow run-end encoding; materialize the logical values before destination planning"
        ))),
        DataType::Struct(fields) => {
            if fields.is_empty() {
                return Err(CdfError::contract(format!(
                    "Parquet cannot encode empty Arrow struct field `{path}`"
                )));
            }
            validate_field_names(fields)?;
            for child in fields {
                validate_parquet_field(&format!("{path}.{}", child.name()), child.data_type())?;
            }
            Ok(())
        }
        DataType::List(child)
        | DataType::LargeList(child)
        | DataType::ListView(child)
        | DataType::LargeListView(child) => {
            validate_parquet_field(&format!("{path}[]"), child.data_type())
        }
        DataType::FixedSizeList(child, size) => {
            if *size <= 0 {
                return Err(CdfError::contract(format!(
                    "Parquet cannot encode fixed-size list field `{path}` with nonpositive size {size}"
                )));
            }
            validate_parquet_field(&format!("{path}[]"), child.data_type())
        }
        DataType::FixedSizeBinary(size) if *size <= 0 => Err(CdfError::contract(format!(
            "Parquet cannot encode fixed-size binary field `{path}` with nonpositive width {size}"
        ))),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(CdfError::contract(format!(
                    "Parquet map field `{path}` must contain an Arrow struct<key,value>"
                )));
            };
            if fields.len() != 2 || fields[0].is_nullable() {
                return Err(CdfError::contract(format!(
                    "Parquet map field `{path}` requires exactly one non-nullable key and one value field"
                )));
            }
            validate_field_names(fields)?;
            for child in fields {
                validate_parquet_field(&format!("{path}.{}", child.name()), child.data_type())?;
            }
            Ok(())
        }
        DataType::Dictionary(_, value) => {
            validate_parquet_field(&format!("{path}.dictionary_value"), value)
        }
        _ => Ok(()),
    }
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

fn parquet_error(context: impl Into<String>, error: impl std::fmt::Display) -> CdfError {
    CdfError::destination(format!("{}: {}", context.into(), error))
}
