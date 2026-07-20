use std::{fs::File, path::Path, sync::Arc};

use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{CdfError, Result, SourcePosition};
use cdf_package_contract::{QuarantineObservedValue, QuarantineRecord};

use crate::{
    json::canonical_json_bytes,
    storage::{normalize_artifact_path, package_path},
};

pub fn for_each_quarantine_record_in_parquet_file(
    path: impl AsRef<Path>,
    visitor: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
) -> Result<()> {
    let path = path.as_ref();
    let file = File::open(path)
        .map_err(|error| crate::storage::io_error(format!("open {}", path.display()), error))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| CdfError::data(format!("read quarantine parquet metadata: {error}")))?
        .build()
        .map_err(|error| CdfError::data(format!("create quarantine parquet reader: {error}")))?;
    visit_quarantine_batches(reader, visitor)
}

pub fn quarantine_record_count_in_parquet_file(path: impl AsRef<Path>) -> Result<u64> {
    let path = path.as_ref();
    let file = File::open(path)
        .map_err(|error| crate::storage::io_error(format!("open {}", path.display()), error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| CdfError::data(format!("read quarantine parquet metadata: {error}")))?;
    u64::try_from(builder.metadata().file_metadata().num_rows())
        .map_err(|_| CdfError::data("quarantine parquet row count cannot be negative"))
}

pub(crate) fn for_each_quarantine_record_in_package_file(
    package_dir: &Path,
    relative_path: impl AsRef<Path>,
    visitor: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
) -> Result<()> {
    let relative_path = normalize_artifact_path(relative_path.as_ref())?;
    if !relative_path.starts_with("quarantine/") || !relative_path.ends_with(".parquet") {
        return Err(CdfError::data(format!(
            "quarantine artifact path must live under quarantine/ and end in .parquet: {relative_path}"
        )));
    }
    for_each_quarantine_record_in_parquet_file(package_path(package_dir, relative_path), visitor)
}

pub(crate) fn quarantine_record_count_in_package_file(
    package_dir: &Path,
    relative_path: impl AsRef<Path>,
) -> Result<u64> {
    let relative_path = normalize_artifact_path(relative_path.as_ref())?;
    if !relative_path.starts_with("quarantine/") || !relative_path.ends_with(".parquet") {
        return Err(CdfError::data(format!(
            "quarantine artifact path must live under quarantine/ and end in .parquet: {relative_path}"
        )));
    }
    quarantine_record_count_in_parquet_file(package_path(package_dir, relative_path))
}

pub(crate) fn quarantine_record_batch(records: &[QuarantineRecord]) -> Result<RecordBatch> {
    if records.is_empty() {
        return Err(CdfError::data(
            "quarantine artifact requires at least one record",
        ));
    }
    let source_positions = records
        .iter()
        .map(|record| {
            record
                .source_position
                .as_ref()
                .map(|position| {
                    String::from_utf8(canonical_json_bytes(position)?)
                        .map_err(|error| CdfError::internal(error.to_string()))
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    let observed = records.iter().map(observed_columns).collect::<Vec<_>>();

    let schema = quarantine_schema();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(
                records
                    .iter()
                    .map(|record| record.source_row_ordinal)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                records
                    .iter()
                    .map(|record| record.rule_id.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                records
                    .iter()
                    .map(|record| record.error_code.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(source_positions)),
            Arc::new(StringArray::from(
                observed
                    .iter()
                    .map(|(kind, _, _)| (*kind).to_owned())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                observed
                    .iter()
                    .map(|(_, algorithm, _)| algorithm.clone())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                observed
                    .iter()
                    .map(|(_, _, value)| value.clone())
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .map_err(CdfError::from)
}

pub(crate) fn quarantine_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("source_row_ordinal", DataType::UInt64, false),
        Field::new("rule_id", DataType::Utf8, false),
        Field::new("error_code", DataType::Utf8, false),
        Field::new("source_position_json", DataType::Utf8, true),
        Field::new("observed_value_kind", DataType::Utf8, false),
        Field::new("observed_value_algorithm", DataType::Utf8, true),
        Field::new("observed_value", DataType::Utf8, true),
    ]))
}

fn observed_columns(record: &QuarantineRecord) -> (&'static str, Option<String>, Option<String>) {
    match &record.observed_value_redacted {
        QuarantineObservedValue::Null => ("null", None, None),
        QuarantineObservedValue::Preserved { value } => ("preserved", None, Some(value.clone())),
        QuarantineObservedValue::Hashed { algorithm, value } => {
            ("hashed", Some(algorithm.clone()), Some(value.clone()))
        }
        QuarantineObservedValue::Omitted => ("omitted", None, None),
        QuarantineObservedValue::Masked { value } => ("masked", None, Some(value.clone())),
    }
}

fn visit_quarantine_batches<I, E>(
    batches: I,
    visitor: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
) -> Result<()>
where
    I: IntoIterator<Item = std::result::Result<RecordBatch, E>>,
    E: std::fmt::Display,
{
    for batch in batches {
        visit_records_from_batch(
            &batch.map_err(|error| CdfError::data(error.to_string()))?,
            visitor,
        )?;
    }
    Ok(())
}

fn visit_records_from_batch(
    batch: &RecordBatch,
    visitor: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
) -> Result<()> {
    let ordinals = required_array::<UInt64Array>(batch, "source_row_ordinal")?;
    let rule_ids = required_array::<StringArray>(batch, "rule_id")?;
    let error_codes = required_array::<StringArray>(batch, "error_code")?;
    let source_positions = required_array::<StringArray>(batch, "source_position_json")?;
    let value_kinds = required_array::<StringArray>(batch, "observed_value_kind")?;
    let value_algorithms = required_array::<StringArray>(batch, "observed_value_algorithm")?;
    let values = required_array::<StringArray>(batch, "observed_value")?;

    for row in 0..batch.num_rows() {
        visitor(QuarantineRecord {
            source_row_ordinal: ordinals.value(row),
            rule_id: required_string(rule_ids, row, "rule_id")?.to_owned(),
            error_code: required_string(error_codes, row, "error_code")?.to_owned(),
            source_position: optional_string(source_positions, row)
                .map(|json| {
                    serde_json::from_str::<SourcePosition>(json).map_err(crate::json::json_error)
                })
                .transpose()?,
            observed_value_redacted: observed_value_from_columns(
                required_string(value_kinds, row, "observed_value_kind")?,
                optional_string(value_algorithms, row),
                optional_string(values, row),
            )?,
        })?;
    }
    Ok(())
}

fn required_array<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .ok_or_else(|| CdfError::data(format!("quarantine artifact is missing {name:?}")))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| {
            CdfError::data(format!(
                "quarantine artifact column {name:?} has wrong type"
            ))
        })
}

fn required_string<'a>(array: &'a StringArray, row: usize, name: &str) -> Result<&'a str> {
    optional_string(array, row)
        .ok_or_else(|| CdfError::data(format!("quarantine artifact column {name:?} has null row")))
}

fn optional_string(array: &StringArray, row: usize) -> Option<&str> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn observed_value_from_columns(
    kind: &str,
    algorithm: Option<&str>,
    value: Option<&str>,
) -> Result<QuarantineObservedValue> {
    Ok(match kind {
        "null" => QuarantineObservedValue::Null,
        "preserved" => QuarantineObservedValue::Preserved {
            value: required_observed_value(kind, value)?.to_owned(),
        },
        "hashed" => QuarantineObservedValue::Hashed {
            algorithm: algorithm
                .ok_or_else(|| CdfError::data("hashed quarantine value is missing algorithm"))?
                .to_owned(),
            value: required_observed_value(kind, value)?.to_owned(),
        },
        "omitted" => QuarantineObservedValue::Omitted,
        "masked" => QuarantineObservedValue::Masked {
            value: required_observed_value(kind, value)?.to_owned(),
        },
        other => {
            return Err(CdfError::data(format!(
                "unknown quarantine observed value kind {other:?}"
            )));
        }
    })
}

fn required_observed_value<'a>(kind: &str, value: Option<&'a str>) -> Result<&'a str> {
    value.ok_or_else(|| CdfError::data(format!("{kind} quarantine value is missing value")))
}
