use std::{
    fs,
    io::{Cursor, Read},
    path::Path,
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_csv::reader::{Format as ArrowCsvFormat, ReaderBuilder as CsvReaderBuilder};
use arrow_ipc::reader::StreamReader;
use arrow_json::reader::{ReaderBuilder as JsonReaderBuilder, infer_json_schema};
use arrow_schema::{ArrowError, SchemaRef};
use cdf_contract::ObservedSchema;
use cdf_kernel::{
    Batch, BatchId, CdfError, FileManifest, FilePosition, ResourceDescriptor, Result, SchemaSource,
    ScopeKey, SourcePosition, TrustLevel, WriteDisposition,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::schema::schema_hash;
use crate::{CsvOptions, FileFormat, FileSource, FormatRead, JsonOptions, ReadOptions};

pub fn read_file_source(source: &FileSource) -> Result<FormatRead> {
    let position = file_source_position(&source.path)?;
    let scope = ScopeKey::File {
        path: path_string(&source.path)?,
    };

    match &source.format {
        FileFormat::Csv(options) => {
            let bytes = fs::read(&source.path)
                .map_err(|error| io_data_error(format!("read {}", source.path.display()), error))?;
            read_csv_bytes_with_scope(&bytes, &source.options, options, scope, Some(position))
        }
        FileFormat::Json(options) => {
            let bytes = fs::read(&source.path)
                .map_err(|error| io_data_error(format!("read {}", source.path.display()), error))?;
            read_json_bytes_with_scope(&bytes, &source.options, options, scope, Some(position))
        }
        FileFormat::Ndjson(options) => {
            let bytes = fs::read(&source.path)
                .map_err(|error| io_data_error(format!("read {}", source.path.display()), error))?;
            read_ndjson_bytes_with_scope(&bytes, &source.options, options, scope, Some(position))
        }
        FileFormat::Parquet => {
            read_parquet_file_with_scope(&source.path, &source.options, scope, Some(position))
        }
    }
}

pub fn read_arrow_ipc_stream<R: Read>(reader: R, options: &ReadOptions) -> Result<FormatRead> {
    read_arrow_ipc_stream_with_scope(
        reader,
        options,
        ScopeKey::Stream {
            name: "arrow_ipc_stdout".to_owned(),
        },
        None,
    )
}

fn read_arrow_ipc_stream_with_scope<R: Read>(
    reader: R,
    options: &ReadOptions,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let mut reader = StreamReader::try_new(reader, None).map_err(CdfError::from)?;
    let schema = reader.schema();
    let record_batches = collect_record_batches(&mut reader)?;
    build_output(schema, record_batches, options, scope, position)
}

pub fn read_csv_bytes(
    bytes: &[u8],
    options: &ReadOptions,
    csv_options: &CsvOptions,
) -> Result<FormatRead> {
    read_csv_bytes_with_scope(bytes, options, csv_options, ScopeKey::Resource, None)
}

pub fn read_json_bytes(
    bytes: &[u8],
    options: &ReadOptions,
    json_options: &JsonOptions,
) -> Result<FormatRead> {
    read_json_bytes_with_scope(bytes, options, json_options, ScopeKey::Resource, None)
}

pub fn read_ndjson_bytes(
    bytes: &[u8],
    options: &ReadOptions,
    json_options: &JsonOptions,
) -> Result<FormatRead> {
    read_ndjson_bytes_with_scope(bytes, options, json_options, ScopeKey::Resource, None)
}

pub fn infer_ndjson_observed_schema(
    bytes: &[u8],
    json_options: &JsonOptions,
) -> Result<ObservedSchema> {
    let (schema, _) = infer_json_schema(Cursor::new(bytes), json_options.max_read_records)
        .map_err(CdfError::from)?;
    Ok(ObservedSchema::from_arrow(&schema))
}

fn read_csv_bytes_with_scope(
    bytes: &[u8],
    options: &ReadOptions,
    csv_options: &CsvOptions,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let format = ArrowCsvFormat::default()
        .with_header(csv_options.has_header)
        .with_delimiter(csv_options.delimiter);
    let (schema, _) = format
        .infer_schema(Cursor::new(bytes), None)
        .map_err(CdfError::from)?;
    let schema = Arc::new(schema);
    let mut reader = CsvReaderBuilder::new(schema.clone())
        .with_format(format)
        .with_batch_size(options.batch_size)
        .build(Cursor::new(bytes))
        .map_err(CdfError::from)?;
    let record_batches = collect_record_batches(&mut reader)?;
    build_output(schema, record_batches, options, scope, position)
}

fn read_json_bytes_with_scope(
    bytes: &[u8],
    options: &ReadOptions,
    json_options: &JsonOptions,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let ndjson = json_document_to_ndjson(bytes)?;
    read_ndjson_bytes_with_scope(&ndjson, options, json_options, scope, position)
}

fn read_ndjson_bytes_with_scope(
    bytes: &[u8],
    options: &ReadOptions,
    json_options: &JsonOptions,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let (schema, _) = infer_json_schema(Cursor::new(bytes), json_options.max_read_records)
        .map_err(CdfError::from)?;
    let schema = Arc::new(schema);
    let mut reader = JsonReaderBuilder::new(schema.clone())
        .with_batch_size(options.batch_size)
        .build(Cursor::new(bytes))
        .map_err(CdfError::from)?;
    let record_batches = collect_record_batches(&mut reader)?;
    build_output(schema, record_batches, options, scope, position)
}

fn read_parquet_file_with_scope(
    path: &Path,
    options: &ReadOptions,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let file = fs::File::open(path)
        .map_err(|error| io_data_error(format!("open {}", path.display()), error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| parquet_data_error("read Parquet file metadata", error))?
        .with_batch_size(options.batch_size);
    let schema = builder.schema().clone();
    let mut reader = builder
        .build()
        .map_err(|error| parquet_data_error("create Parquet record batch reader", error))?;
    let record_batches = collect_record_batches(&mut reader)?;
    build_output(schema, record_batches, options, scope, position)
}

fn build_output(
    schema: SchemaRef,
    record_batches: Vec<RecordBatch>,
    options: &ReadOptions,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let schema = record_batches
        .first()
        .map(RecordBatch::schema)
        .unwrap_or(schema);
    let observed_schema = ObservedSchema::from_arrow(schema.as_ref());
    let schema_hash = schema_hash(schema.as_ref())?;
    let descriptor = ResourceDescriptor {
        resource_id: options.resource_id.clone(),
        schema_source: SchemaSource::Discovered {
            schema_hash: Some(schema_hash.clone()),
        },
        primary_key: Vec::new(),
        merge_key: Vec::new(),
        cursor: None,
        write_disposition: WriteDisposition::Append,
        contract: None,
        state_scope: scope,
        freshness: None,
        trust_level: TrustLevel::Experimental,
    };

    let mut batches = Vec::with_capacity(record_batches.len());
    for (index, record_batch) in record_batches.into_iter().enumerate() {
        if record_batch.schema().as_ref() != schema.as_ref() {
            return Err(CdfError::data(
                "format reader yielded record batches with different schemas",
            ));
        }

        let mut batch = Batch::from_record_batch(
            BatchId::new(format!("{}-{:06}", options.batch_id_prefix, index + 1))?,
            options.resource_id.clone(),
            options.partition_id.clone(),
            schema_hash.clone(),
            record_batch,
        )?;
        batch.header.source_position = position.clone();
        batches.push(batch);
    }

    Ok(FormatRead {
        descriptor,
        observed_schema,
        schema_hash,
        batches,
    })
}

fn collect_record_batches<I>(reader: &mut I) -> Result<Vec<RecordBatch>>
where
    I: Iterator<Item = std::result::Result<RecordBatch, ArrowError>>,
{
    reader
        .map(|batch| batch.map_err(CdfError::from))
        .collect::<Result<Vec<_>>>()
}

fn json_document_to_ndjson(bytes: &[u8]) -> Result<Vec<u8>> {
    let value: Value = serde_json::from_slice(bytes).map_err(json_error)?;
    let rows = match value {
        Value::Array(rows) => rows,
        Value::Object(_) => vec![value],
        _ => {
            return Err(CdfError::data(
                "JSON file source must be an object or an array of objects",
            ));
        }
    };

    let mut output = Vec::new();
    for row in rows {
        if !row.is_object() {
            return Err(CdfError::data(
                "JSON file source array entries must be objects",
            ));
        }
        serde_json::to_writer(&mut output, &row).map_err(json_error)?;
        output.push(b'\n');
    }
    Ok(output)
}

fn file_source_position(path: &Path) -> Result<SourcePosition> {
    let metadata = fs::metadata(path)
        .map_err(|error| io_data_error(format!("stat {}", path.display()), error))?;
    Ok(SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: path_string(path)?,
            size_bytes: metadata.len(),
            etag: None,
            sha256: Some(file_sha256(path)?),
        }],
    }))
}

fn file_sha256(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)
        .map_err(|error| io_data_error(format!("open {}", path.display()), error))?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut file, &mut hasher)
        .map_err(|error| io_data_error(format!("hash {}", path.display()), error))?;
    Ok(hex::encode(hasher.finalize()))
}

fn path_string(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| CdfError::data(format!("path is not valid UTF-8: {}", path.display())))
}

fn io_data_error(context: impl Into<String>, error: std::io::Error) -> CdfError {
    CdfError::data(format!("{}: {error}", context.into()))
}

fn parquet_data_error(context: impl Into<String>, error: impl std::fmt::Display) -> CdfError {
    CdfError::data(format!("{}: {error}", context.into()))
}

fn json_error(error: serde_json::Error) -> CdfError {
    CdfError::data(error.to_string())
}
