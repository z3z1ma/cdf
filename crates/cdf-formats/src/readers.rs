use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Cursor, Read, Seek},
    path::Path,
    sync::Arc,
};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_cast::cast::{can_cast_types, cast};
use arrow_csv::reader::{Format as ArrowCsvFormat, ReaderBuilder as CsvReaderBuilder};
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_json::reader::{ReaderBuilder as JsonReaderBuilder, infer_json_schema};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use cdf_contract::{
    ContractPolicy, ObservedSchema, PiiRedactionPolicy, RedactionDecision, reconcile_schema,
    redaction_decision_for_field,
};
use cdf_kernel::{
    Batch, BatchId, CdfError, FileManifest, FilePosition, PreContractObservedValue,
    PreContractQuarantineFact, ResourceDescriptor, ResourceId, Result, SchemaHash,
    SchemaSnapshotReference, SchemaSource, ScopeKey, SourcePosition, TrustLevel, WriteDisposition,
    source_name,
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

pub fn read_file_source_with_declared_schema(
    source: &FileSource,
    declared_schema: SchemaRef,
) -> Result<FormatRead> {
    let position = file_source_position(&source.path)?;
    let scope = ScopeKey::File {
        path: path_string(&source.path)?,
    };

    match &source.format {
        FileFormat::Json(options) => {
            let bytes = fs::read(&source.path)
                .map_err(|error| io_data_error(format!("read {}", source.path.display()), error))?;
            read_json_bytes_with_declared_schema_and_scope(
                &bytes,
                &source.options,
                options,
                declared_schema,
                scope,
                Some(position),
            )
        }
        FileFormat::Ndjson(options) => {
            let bytes = fs::read(&source.path)
                .map_err(|error| io_data_error(format!("read {}", source.path.display()), error))?;
            read_ndjson_bytes_with_declared_schema_and_scope(
                &bytes,
                &source.options,
                options,
                declared_schema,
                scope,
                Some(position),
            )
        }
        FileFormat::Parquet => read_parquet_file_with_declared_schema_and_scope(
            &source.path,
            &source.options,
            declared_schema,
            scope,
            Some(position),
        ),
        FileFormat::Csv(_) => read_file_source(source),
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
    finish_arrow_ipc_read(reader.schema(), &mut reader, options, scope, position)
}

pub fn read_arrow_ipc_file<R: Read + Seek>(reader: R, options: &ReadOptions) -> Result<FormatRead> {
    let mut reader = FileReader::try_new(reader, None).map_err(CdfError::from)?;
    finish_arrow_ipc_read(
        reader.schema(),
        &mut reader,
        options,
        ScopeKey::File {
            path: "arrow_ipc_file".to_owned(),
        },
        None,
    )
}

fn finish_arrow_ipc_read<I>(
    schema: SchemaRef,
    reader: &mut I,
    options: &ReadOptions,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead>
where
    I: Iterator<Item = std::result::Result<RecordBatch, ArrowError>>,
{
    let record_batches = collect_record_batches(reader)?;
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

pub fn read_ndjson_bytes_with_declared_schema(
    bytes: &[u8],
    options: &ReadOptions,
    json_options: &JsonOptions,
    declared_schema: SchemaRef,
) -> Result<FormatRead> {
    read_ndjson_bytes_with_declared_schema_and_scope(
        bytes,
        options,
        json_options,
        declared_schema,
        ScopeKey::Resource,
        None,
    )
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

fn read_json_bytes_with_declared_schema_and_scope(
    bytes: &[u8],
    options: &ReadOptions,
    json_options: &JsonOptions,
    declared_schema: SchemaRef,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let ndjson = json_document_to_ndjson(bytes)?;
    read_ndjson_bytes_with_declared_schema_and_scope(
        &ndjson,
        options,
        json_options,
        declared_schema,
        scope,
        position,
    )
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

fn read_ndjson_bytes_with_declared_schema_and_scope(
    bytes: &[u8],
    options: &ReadOptions,
    _json_options: &JsonOptions,
    declared_schema: SchemaRef,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let filtered = filter_declared_ndjson_rows(bytes, declared_schema.as_ref(), &position)?;
    let mut reader = JsonReaderBuilder::new(declared_schema.clone())
        .with_batch_size(options.batch_size)
        .build(Cursor::new(filtered.accepted_ndjson))
        .map_err(CdfError::from)?;
    let mut record_batches = collect_record_batches(&mut reader)?;
    if record_batches.is_empty() && !filtered.quarantine_facts.is_empty() {
        record_batches.push(RecordBatch::new_empty(declared_schema.clone()));
    }
    build_output_with_pre_contract_quarantine(
        declared_schema,
        record_batches,
        options,
        scope,
        position,
        filtered.quarantine_facts,
    )
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

fn read_parquet_file_with_declared_schema_and_scope(
    path: &Path,
    options: &ReadOptions,
    declared_schema: SchemaRef,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let file = fs::File::open(path)
        .map_err(|error| io_data_error(format!("open {}", path.display()), error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| parquet_data_error("read Parquet file metadata", error))?
        .with_batch_size(options.batch_size);
    let physical_schema = builder.schema().clone();
    let mut type_policy = ContractPolicy::default().types;
    type_policy.coerce_types = false;
    let reconciliation = reconcile_schema(
        physical_schema.as_ref(),
        declared_schema.as_ref(),
        &type_policy,
    )?;
    let reconciled_schema = Arc::new(reconciliation.schema);
    let mut reader = builder
        .build()
        .map_err(|error| parquet_data_error("create Parquet record batch reader", error))?;
    let physical_batches = collect_record_batches(&mut reader)?;
    let record_batches = reconcile_parquet_record_batches(
        &physical_schema,
        reconciled_schema.clone(),
        physical_batches,
    )?;

    build_output(reconciled_schema, record_batches, options, scope, position)
}

fn reconcile_parquet_record_batches(
    physical_schema: &Schema,
    reconciled_schema: SchemaRef,
    record_batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    record_batches
        .into_iter()
        .map(|batch| {
            reconcile_parquet_record_batch(physical_schema, reconciled_schema.clone(), batch)
        })
        .collect()
}

fn reconcile_parquet_record_batch(
    physical_schema: &Schema,
    reconciled_schema: SchemaRef,
    batch: RecordBatch,
) -> Result<RecordBatch> {
    let columns = reconciled_schema
        .fields()
        .iter()
        .map(|field| reconciled_parquet_column(physical_schema, &batch, field.as_ref()))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(reconciled_schema, columns).map_err(CdfError::from)
}

fn reconciled_parquet_column(
    physical_schema: &Schema,
    batch: &RecordBatch,
    output_field: &Field,
) -> Result<ArrayRef> {
    let source = source_name(output_field).unwrap_or_else(|| output_field.name());
    let physical_index = physical_schema
        .fields()
        .iter()
        .position(|field| field_source_name(field.as_ref()) == source)
        .ok_or_else(|| {
            CdfError::internal(format!(
                "reconciled Parquet field {:?} has no matching physical source field {source:?}",
                output_field.name()
            ))
        })?;
    let column = batch.column(physical_index);
    if column.data_type() == output_field.data_type() {
        return Ok(column.clone());
    }
    if !can_cast_types(column.data_type(), output_field.data_type()) {
        return Err(CdfError::contract(format!(
            "Parquet schema reconciliation selected unsupported materialized cast for field {:?}: observed type {}; declared type {}",
            source,
            column.data_type(),
            output_field.data_type()
        )));
    }
    cast(column.as_ref(), output_field.data_type()).map_err(CdfError::from)
}

fn field_source_name(field: &Field) -> &str {
    source_name(field).unwrap_or_else(|| field.name())
}

fn build_output(
    schema: SchemaRef,
    record_batches: Vec<RecordBatch>,
    options: &ReadOptions,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    build_output_with_pre_contract_quarantine(
        schema,
        record_batches,
        options,
        scope,
        position,
        Vec::new(),
    )
}

fn build_output_with_pre_contract_quarantine(
    schema: SchemaRef,
    record_batches: Vec<RecordBatch>,
    options: &ReadOptions,
    scope: ScopeKey,
    position: Option<SourcePosition>,
    mut pre_contract_quarantine: Vec<PreContractQuarantineFact>,
) -> Result<FormatRead> {
    let schema = record_batches
        .first()
        .map(RecordBatch::schema)
        .unwrap_or(schema);
    let observed_schema = ObservedSchema::from_arrow(schema.as_ref());
    let schema_hash = schema_hash(schema.as_ref())?;
    let descriptor = ResourceDescriptor {
        resource_id: options.resource_id.clone(),
        schema_source: discovered_schema_source(&options.resource_id, &schema_hash),
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
        if index == 0 {
            batch.header.pre_contract_quarantine = std::mem::take(&mut pre_contract_quarantine);
        }
        batches.push(batch);
    }

    Ok(FormatRead {
        descriptor,
        observed_schema,
        schema_hash,
        batches,
    })
}

fn discovered_schema_source(resource_id: &ResourceId, schema_hash: &SchemaHash) -> SchemaSource {
    SchemaSource::Discovered {
        snapshot: SchemaSnapshotReference {
            schema_hash: schema_hash.clone(),
            path: format!(".cdf/schemas/{resource_id}@{schema_hash}.json"),
            metadata: BTreeMap::from([("probe".to_owned(), "format-read".to_owned())]),
        },
    }
}

struct DeclaredNdjsonFilter {
    accepted_ndjson: Vec<u8>,
    quarantine_facts: Vec<PreContractQuarantineFact>,
}

fn filter_declared_ndjson_rows(
    bytes: &[u8],
    schema: &arrow_schema::Schema,
    position: &Option<SourcePosition>,
) -> Result<DeclaredNdjsonFilter> {
    validate_declared_json_schema(schema)?;
    let declared_fields = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<BTreeSet<_>>();
    let text = std::str::from_utf8(bytes).map_err(|error| {
        CdfError::data(format!("NDJSON file source is not valid UTF-8: {error}"))
    })?;
    let mut accepted_ndjson = Vec::new();
    let mut quarantine_facts = Vec::new();
    let mut row_ordinal = 0_u64;

    for (line_index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            return Err(CdfError::data(format!(
                "NDJSON file source line {} is empty",
                line_index + 1
            )));
        }
        let row: Value = serde_json::from_str(line).map_err(json_error)?;
        let object = row.as_object().ok_or_else(|| {
            CdfError::data(format!(
                "NDJSON file source line {} must be a JSON object",
                line_index + 1
            ))
        })?;
        if let Some(undeclared) = object
            .keys()
            .find(|name| !declared_fields.contains(name.as_str()))
        {
            return Err(CdfError::data(format!(
                "NDJSON file source line {} contains undeclared field {undeclared:?}",
                line_index + 1
            )));
        }

        let mut row_facts = Vec::new();
        for field in schema.fields() {
            let Some(value) = object.get(field.name()) else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            if declared_json_type_mismatch(field.as_ref(), value)? {
                row_facts.push(PreContractQuarantineFact {
                    source_row_ordinal: row_ordinal,
                    rule_id: format!("source-decode:{}:type-mismatch", field.name()),
                    error_code: "source_type_mismatch".to_owned(),
                    source_position: position.clone(),
                    observed_value_redacted: redacted_declared_json_value(field.as_ref(), value)?,
                });
            }
        }

        if row_facts.is_empty() {
            serde_json::to_writer(&mut accepted_ndjson, &row).map_err(json_error)?;
            accepted_ndjson.push(b'\n');
        } else {
            quarantine_facts.extend(row_facts);
        }
        row_ordinal += 1;
    }

    Ok(DeclaredNdjsonFilter {
        accepted_ndjson,
        quarantine_facts,
    })
}

fn validate_declared_json_schema(schema: &arrow_schema::Schema) -> Result<()> {
    for field in schema.fields() {
        if !declared_json_type_supported(field.data_type()) {
            return Err(CdfError::contract(format!(
                "declared NDJSON source decode quarantine does not support field {:?} with type {}",
                field.name(),
                field.data_type()
            )));
        }
    }
    Ok(())
}

fn declared_json_type_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
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
            | DataType::Date32
            | DataType::Timestamp(_, _)
    )
}

fn declared_json_type_mismatch(field: &Field, value: &Value) -> Result<bool> {
    if !is_json_scalar(value) {
        return Err(CdfError::data(format!(
            "NDJSON field {:?} expected scalar {}, got complex JSON value",
            field.name(),
            field.data_type()
        )));
    }
    Ok(match field.data_type() {
        DataType::Boolean => !value.is_boolean(),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64 => !value.is_number(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Date32 | DataType::Timestamp(_, _) => {
            !value.is_string()
        }
        other => {
            return Err(CdfError::contract(format!(
                "declared NDJSON source decode quarantine does not support field {:?} with type {other}",
                field.name()
            )));
        }
    })
}

fn is_json_scalar(value: &Value) -> bool {
    value.is_boolean() || value.is_number() || value.is_string()
}

fn redacted_declared_json_value(field: &Field, value: &Value) -> Result<PreContractObservedValue> {
    let value = source_scalar_string(value).ok_or_else(|| {
        CdfError::data(format!(
            "NDJSON field {:?} type mismatch value is not scalar",
            field.name()
        ))
    })?;
    match redaction_decision_for_field(field, &PiiRedactionPolicy::default()) {
        RedactionDecision::Preserve => Ok(PreContractObservedValue::Preserved { value }),
        RedactionDecision::Hash { algorithm } if algorithm == "sha256" => {
            Ok(PreContractObservedValue::Hashed {
                algorithm,
                value: format!("sha256:{}", sha256_hex(value.as_bytes())),
            })
        }
        RedactionDecision::Hash { algorithm } => Err(CdfError::contract(format!(
            "unsupported quarantine hash algorithm {algorithm:?}"
        ))),
        RedactionDecision::Omit => Ok(PreContractObservedValue::Omitted),
        RedactionDecision::Mask { replacement } => {
            Ok(PreContractObservedValue::Masked { value: replacement })
        }
    }
}

fn source_scalar_string(value: &Value) -> Option<String> {
    match value {
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) => Some(value.clone()),
        Value::Null | Value::Array(_) | Value::Object(_) => None,
    }
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

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
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
