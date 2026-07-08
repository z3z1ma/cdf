use std::{fs::File, io::Write, path::Path, sync::Arc};

use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use arrow_array::{Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{CdfError, Result, SourcePosition};
use serde::{Deserialize, Serialize};

use crate::{
    json::canonical_json_bytes,
    parquet::transcode_record_batches_to_parquet_bytes,
    storage::{normalize_artifact_path, package_path},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuarantineRecord {
    pub source_row_ordinal: u64,
    pub rule_id: String,
    pub error_code: String,
    pub source_position: Option<SourcePosition>,
    pub observed_value_redacted: QuarantineObservedValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QuarantineObservedValue {
    Null,
    Preserved { value: String },
    Hashed { algorithm: String, value: String },
    Omitted,
    Masked { value: String },
}

pub fn quarantine_records_to_parquet_bytes(records: &[QuarantineRecord]) -> Result<Vec<u8>> {
    if records.is_empty() {
        return Err(CdfError::data(
            "quarantine artifact requires at least one record",
        ));
    }
    transcode_record_batches_to_parquet_bytes(&[quarantine_record_batch(records)?])
}

pub fn quarantine_records_from_parquet_bytes(bytes: &[u8]) -> Result<Vec<QuarantineRecord>> {
    let mut temp = tempfile::NamedTempFile::new()
        .map_err(|error| CdfError::data(format!("create quarantine parquet temp file: {error}")))?;
    temp.write_all(bytes)
        .map_err(|error| CdfError::data(format!("write quarantine parquet temp file: {error}")))?;
    temp.flush()
        .map_err(|error| CdfError::data(format!("flush quarantine parquet temp file: {error}")))?;
    quarantine_records_from_parquet_file(temp.path())
}

pub fn quarantine_records_from_parquet_file(
    path: impl AsRef<Path>,
) -> Result<Vec<QuarantineRecord>> {
    let path = path.as_ref();
    let file = File::open(path)
        .map_err(|error| crate::storage::io_error(format!("open {}", path.display()), error))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| CdfError::data(format!("read quarantine parquet metadata: {error}")))?
        .build()
        .map_err(|error| CdfError::data(format!("create quarantine parquet reader: {error}")))?;
    read_quarantine_batches(reader)
}

pub(crate) fn quarantine_records_from_package_file(
    package_dir: &Path,
    relative_path: impl AsRef<Path>,
) -> Result<Vec<QuarantineRecord>> {
    let relative_path = normalize_artifact_path(relative_path.as_ref())?;
    if !relative_path.starts_with("quarantine/") || !relative_path.ends_with(".parquet") {
        return Err(CdfError::data(format!(
            "quarantine artifact path must live under quarantine/ and end in .parquet: {relative_path}"
        )));
    }
    quarantine_records_from_parquet_file(package_path(package_dir, relative_path))
}

fn quarantine_record_batch(records: &[QuarantineRecord]) -> Result<RecordBatch> {
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

    let schema = Arc::new(Schema::new(vec![
        Field::new("source_row_ordinal", DataType::UInt64, false),
        Field::new("rule_id", DataType::Utf8, false),
        Field::new("error_code", DataType::Utf8, false),
        Field::new("source_position_json", DataType::Utf8, true),
        Field::new("observed_value_kind", DataType::Utf8, false),
        Field::new("observed_value_algorithm", DataType::Utf8, true),
        Field::new("observed_value", DataType::Utf8, true),
    ]));

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

fn read_quarantine_batches<I, E>(batches: I) -> Result<Vec<QuarantineRecord>>
where
    I: IntoIterator<Item = std::result::Result<RecordBatch, E>>,
    E: std::fmt::Display,
{
    let mut records = Vec::new();
    for batch in batches {
        records.extend(records_from_batch(
            &batch.map_err(|error| CdfError::data(error.to_string()))?,
        )?);
    }
    Ok(records)
}

fn records_from_batch(batch: &RecordBatch) -> Result<Vec<QuarantineRecord>> {
    let ordinals = required_array::<UInt64Array>(batch, "source_row_ordinal")?;
    let rule_ids = required_array::<StringArray>(batch, "rule_id")?;
    let error_codes = required_array::<StringArray>(batch, "error_code")?;
    let source_positions = required_array::<StringArray>(batch, "source_position_json")?;
    let value_kinds = required_array::<StringArray>(batch, "observed_value_kind")?;
    let value_algorithms = required_array::<StringArray>(batch, "observed_value_algorithm")?;
    let values = required_array::<StringArray>(batch, "observed_value")?;

    let mut records = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        records.push(QuarantineRecord {
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
        });
    }
    Ok(records)
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
