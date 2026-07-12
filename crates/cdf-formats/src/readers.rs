use std::{
    collections::BTreeMap,
    fs,
    io::{BufReader, Cursor, Read, Seek},
    path::Path,
    sync::Arc,
};

use arrow_array::{Array, ArrayRef, RecordBatch, new_null_array};
use arrow_cast::cast::{can_cast_types, cast};
use arrow_csv::reader::{Format as ArrowCsvFormat, ReaderBuilder as CsvReaderBuilder};
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_json::reader::{ReaderBuilder as JsonReaderBuilder, infer_json_schema};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use cdf_contract::{
    ContractPolicy, ObservedSchema, SchemaCoercionPlan, TypePolicy, reconcile_schema,
};
use cdf_kernel::{
    Batch, BatchId, BatchStream, CdfError, FileManifest, FilePosition, PreContractQuarantineFact,
    PreContractResidualCandidate, ResourceDescriptor, ResourceId, Result, SchemaHash,
    SchemaSnapshotReference, SchemaSource, ScopeKey, SourcePosition, TrustLevel, WriteDisposition,
    source_name,
};
use flate2::read::GzDecoder;
use futures_util::stream;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::reader::ChunkReader;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::schema::schema_hash;
use crate::{
    CsvOptions, FileCompression, FileFormat, FileSource, FormatRead, JsonOptions, ReadOptions,
};

pub fn read_file_source(source: &FileSource) -> Result<FormatRead> {
    let position = file_source_position(&source.path)?;
    let scope = ScopeKey::File {
        path: path_string(&source.path)?,
    };

    match &source.format {
        FileFormat::Csv(options) => {
            let bytes = read_file_source_bytes(source)?;
            read_csv_bytes_with_scope(&bytes, &source.options, options, scope, Some(position))
        }
        FileFormat::Json(options) => {
            let bytes = read_file_source_bytes(source)?;
            read_json_bytes_with_scope(&bytes, &source.options, options, scope, Some(position))
        }
        FileFormat::Ndjson(options) => {
            let bytes = read_file_source_bytes(source)?;
            read_ndjson_bytes_with_scope(&bytes, &source.options, options, scope, Some(position))
        }
        FileFormat::Parquet => {
            reject_byte_stream_compression_for_parquet(source)?;
            read_parquet_file_with_scope(&source.path, &source.options, scope, Some(position))
        }
    }
}

pub fn read_file_source_with_declared_schema(
    source: &FileSource,
    declared_schema: SchemaRef,
) -> Result<FormatRead> {
    let type_policy = strict_source_type_policy();
    read_file_source_with_declared_schema_and_type_policy(source, declared_schema, &type_policy)
}

pub fn read_file_source_with_declared_schema_and_type_policy(
    source: &FileSource,
    declared_schema: SchemaRef,
    type_policy: &TypePolicy,
) -> Result<FormatRead> {
    let position = file_source_position(&source.path)?;
    let scope = ScopeKey::File {
        path: path_string(&source.path)?,
    };

    match &source.format {
        FileFormat::Json(options) => {
            let bytes = read_file_source_bytes(source)?;
            read_json_bytes_with_declared_schema_and_scope(
                &bytes,
                &source.options,
                options,
                declared_schema,
                type_policy,
                scope,
                Some(position),
            )
        }
        FileFormat::Ndjson(options) => {
            let bytes = read_file_source_bytes(source)?;
            read_ndjson_bytes_with_declared_schema_and_scope(
                &bytes,
                &source.options,
                options,
                declared_schema,
                type_policy,
                scope,
                Some(position),
            )
        }
        FileFormat::Parquet => read_parquet_file_with_declared_schema_and_scope(
            &source.path,
            &source.options,
            declared_schema,
            type_policy,
            scope,
            Some(position),
        ),
        FileFormat::Csv(_) => read_file_source(source),
    }
}

pub fn stream_file_source_path_with_declared_schema_and_type_policy(
    path: &Path,
    format: FileFormat,
    compression: FileCompression,
    options: ReadOptions,
    declared_schema: SchemaRef,
    type_policy: &TypePolicy,
    position: Option<SourcePosition>,
) -> Result<BatchStream> {
    if format == FileFormat::Parquet {
        if compression != FileCompression::None {
            return Err(CdfError::contract(format!(
                "byte-stream compression `{}` is not supported for Parquet file source {}; Parquet compression must be handled by the Parquet reader",
                compression.as_str(),
                path.display()
            )));
        }
        return stream_parquet_file_with_declared_schema_and_type_policy(
            path,
            &options,
            declared_schema,
            type_policy,
            position,
        );
    }

    let source = FileSource::new(path, format, options).with_compression(compression);
    let mut read = if !declared_schema.fields().is_empty()
        && matches!(source.format, FileFormat::Json(_) | FileFormat::Ndjson(_))
    {
        read_file_source_with_declared_schema_and_type_policy(
            &source,
            declared_schema,
            type_policy,
        )?
    } else {
        read_file_source(&source)?
    };
    for batch in &mut read.batches {
        batch.header.source_position = position.clone();
    }
    Ok(Box::pin(stream::iter(read.batches.into_iter().map(Ok))))
}

fn read_file_source_bytes(source: &FileSource) -> Result<Vec<u8>> {
    match source.compression {
        FileCompression::None => fs::read(&source.path)
            .map_err(|error| io_data_error(format!("read {}", source.path.display()), error)),
        FileCompression::Gzip => read_gzip_file(&source.path),
        FileCompression::Zstd => read_zstd_file(&source.path),
    }
}

fn read_gzip_file(path: &Path) -> Result<Vec<u8>> {
    let file = open_compressed_file(path, "gzip")?;
    let decoder = GzDecoder::new(file);
    read_decoder_to_end(decoder, path, "gzip")
}

fn read_zstd_file(path: &Path) -> Result<Vec<u8>> {
    let file = open_compressed_file(path, "zstd")?;
    let decoder = zstd::stream::read::Decoder::new(file)
        .map_err(|error| io_data_error(format!("open zstd stream {}", path.display()), error))?;
    read_decoder_to_end(decoder, path, "zstd")
}

fn open_compressed_file(path: &Path, compression: &str) -> Result<fs::File> {
    fs::File::open(path).map_err(|error| {
        io_data_error(
            format!("open {compression}-compressed {}", path.display()),
            error,
        )
    })
}

fn read_decoder_to_end<R: Read>(mut decoder: R, path: &Path, compression: &str) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    decoder.read_to_end(&mut bytes).map_err(|error| {
        io_data_error(
            format!("read {compression}-compressed {}", path.display()),
            error,
        )
    })?;
    Ok(bytes)
}

fn reject_byte_stream_compression_for_parquet(source: &FileSource) -> Result<()> {
    if source.compression == FileCompression::None {
        return Ok(());
    }
    Err(CdfError::contract(format!(
        "byte-stream compression `{}` is not supported for Parquet file source {}; Parquet compression must be handled by the Parquet reader",
        source.compression.as_str(),
        source.path.display()
    )))
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

pub fn read_arrow_ipc_file_path(
    path: impl AsRef<Path>,
    options: &ReadOptions,
) -> Result<FormatRead> {
    let path = path.as_ref();
    let position = file_source_position(path)?;
    let scope = ScopeKey::File {
        path: path_string(path)?,
    };
    let file = fs::File::open(path)
        .map_err(|error| io_data_error(format!("read Arrow IPC file {}", path.display()), error))?;
    let mut reader = FileReader::try_new(file, None).map_err(|error| {
        CdfError::data(format!(
            "read Arrow IPC file {} with file framing: {error}",
            path.display()
        ))
    })?;
    finish_arrow_ipc_read(reader.schema(), &mut reader, options, scope, Some(position))
}

pub fn read_arrow_ipc_file_path_with_declared_schema(
    path: impl AsRef<Path>,
    options: &ReadOptions,
    declared_schema: SchemaRef,
) -> Result<FormatRead> {
    let path = path.as_ref();
    let position = file_source_position(path)?;
    let scope = ScopeKey::File {
        path: path_string(path)?,
    };
    let file = fs::File::open(path)
        .map_err(|error| io_data_error(format!("read Arrow IPC file {}", path.display()), error))?;
    let mut reader = FileReader::try_new(file, None).map_err(|error| {
        CdfError::data(format!(
            "read Arrow IPC file {} with file framing: {error}",
            path.display()
        ))
    })?;
    let physical_schema = reader.schema();
    let physical_schema_hash = schema_hash(physical_schema.as_ref())?;
    let reconciliation = reconcile_schema(
        physical_schema.as_ref(),
        declared_schema.as_ref(),
        &strict_source_type_policy(),
    )?;
    let reconciliation_plan = reconciliation.plan;
    let reconciled_schema = Arc::new(reconciliation.schema);
    let physical_batches = collect_record_batches(&mut reader)?;
    let reconciled = reconcile_record_batches(
        physical_schema.as_ref(),
        reconciled_schema.clone(),
        physical_batches,
        "Arrow IPC",
    )?;
    with_observed_schema_hash(
        build_output_with_pre_contract_evidence(
            reconciled_schema,
            reconciled.batches,
            options,
            scope,
            Some(position),
            PreContractReadEvidence {
                quarantine: Vec::new(),
                residual_candidates: reconciled.residual_candidates,
                schema_coercion_plan: Some(&reconciliation_plan),
            },
        )?,
        physical_schema_hash,
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
    let type_policy = strict_source_type_policy();
    read_ndjson_bytes_with_declared_schema_and_type_policy(
        bytes,
        options,
        json_options,
        declared_schema,
        &type_policy,
    )
}

pub fn read_ndjson_bytes_with_declared_schema_and_type_policy(
    bytes: &[u8],
    options: &ReadOptions,
    json_options: &JsonOptions,
    declared_schema: SchemaRef,
    type_policy: &TypePolicy,
) -> Result<FormatRead> {
    read_ndjson_bytes_with_declared_schema_and_scope(
        bytes,
        options,
        json_options,
        declared_schema,
        type_policy,
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

pub fn discover_ndjson_schema_from_reader(
    reader: Box<dyn Read + Send>,
    compression: FileCompression,
    max_read_records: Option<usize>,
) -> Result<SchemaRef> {
    let reader = decoded_discovery_reader(reader, compression)?;
    let (schema, _) =
        infer_json_schema(BufReader::new(reader), max_read_records).map_err(CdfError::from)?;
    Ok(Arc::new(schema))
}

pub fn discover_csv_schema_from_reader(
    reader: Box<dyn Read + Send>,
    compression: FileCompression,
    csv_options: &CsvOptions,
    max_read_records: usize,
) -> Result<SchemaRef> {
    let reader = decoded_discovery_reader(reader, compression)?;
    let format = ArrowCsvFormat::default()
        .with_header(csv_options.has_header)
        .with_delimiter(csv_options.delimiter);
    let (schema, _) = format
        .infer_schema(BufReader::new(reader), Some(max_read_records))
        .map_err(CdfError::from)?;
    Ok(Arc::new(schema))
}

pub fn discover_json_schema_from_reader(
    reader: Box<dyn Read + Send>,
    compression: FileCompression,
    max_read_records: usize,
) -> Result<SchemaRef> {
    let mut reader = decoded_discovery_reader(reader, compression)?;
    let mut document = Vec::new();
    reader
        .read_to_end(&mut document)
        .map_err(|error| io_data_error("read bounded JSON discovery sample", error))?;
    let ndjson = json_document_to_ndjson(&document)?;
    let (schema, _) =
        infer_json_schema(Cursor::new(ndjson), Some(max_read_records)).map_err(CdfError::from)?;
    Ok(Arc::new(schema))
}

fn decoded_discovery_reader(
    reader: Box<dyn Read + Send>,
    compression: FileCompression,
) -> Result<Box<dyn Read + Send>> {
    match compression {
        FileCompression::None => Ok(reader),
        FileCompression::Gzip => Ok(Box::new(GzDecoder::new(reader))),
        FileCompression::Zstd => Ok(Box::new(
            zstd::stream::read::Decoder::new(reader)
                .map_err(|error| io_data_error("open zstd discovery stream", error))?,
        )),
    }
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
    type_policy: &TypePolicy,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let ndjson = json_document_to_ndjson(bytes)?;
    read_ndjson_bytes_with_declared_schema_and_scope(
        &ndjson,
        options,
        json_options,
        declared_schema,
        type_policy,
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
    json_options: &JsonOptions,
    declared_schema: SchemaRef,
    type_policy: &TypePolicy,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let filtered =
        filter_declared_ndjson_rows(bytes, declared_schema.as_ref(), type_policy, &position)?;
    if filtered.accepted_rows == 0 {
        return build_output_with_pre_contract_evidence(
            declared_schema.clone(),
            vec![RecordBatch::new_empty(declared_schema)],
            options,
            scope,
            position,
            PreContractReadEvidence {
                quarantine: filtered.quarantine_facts,
                residual_candidates: Vec::new(),
                schema_coercion_plan: None,
            },
        );
    }

    let (physical_schema, _) = infer_json_schema(
        Cursor::new(filtered.accepted_ndjson.as_slice()),
        json_options.max_read_records,
    )
    .map_err(CdfError::from)?;
    let physical_schema = Arc::new(repair_residual_null_inference(
        physical_schema,
        declared_schema.as_ref(),
        &filtered.residual_candidates,
    ));
    let physical_schema_hash = schema_hash(physical_schema.as_ref())?;
    let reconciliation = reconcile_schema(
        physical_schema.as_ref(),
        declared_schema.as_ref(),
        type_policy,
    )?;
    let reconciliation_plan = reconciliation.plan;
    let reconciled_schema = Arc::new(nullable_residual_decode_schema(
        reconciliation.schema,
        &filtered.residual_candidates,
    ));
    let mut reader = JsonReaderBuilder::new(physical_schema.clone())
        .with_batch_size(options.batch_size)
        .build(Cursor::new(filtered.accepted_ndjson))
        .map_err(CdfError::from)?;
    let physical_batches = collect_record_batches(&mut reader)?;
    let mut reconciled = reconcile_record_batches(
        physical_schema.as_ref(),
        reconciled_schema.clone(),
        physical_batches,
        "JSON",
    )?;
    merge_pending_residual_candidates(
        &mut reconciled.residual_candidates,
        &reconciled.batches,
        filtered.residual_candidates,
    )?;
    with_observed_schema_hash(
        build_output_with_pre_contract_evidence(
            reconciled_schema,
            reconciled.batches,
            options,
            scope,
            position,
            PreContractReadEvidence {
                quarantine: filtered.quarantine_facts,
                residual_candidates: reconciled.residual_candidates,
                schema_coercion_plan: Some(&reconciliation_plan),
            },
        )?,
        physical_schema_hash,
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
    read_parquet_chunk_reader_with_scope(file, options, scope, position)
}

fn read_parquet_chunk_reader_with_scope<T: ChunkReader + 'static>(
    reader: T,
    options: &ReadOptions,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)
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
    type_policy: &TypePolicy,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let file = fs::File::open(path)
        .map_err(|error| io_data_error(format!("open {}", path.display()), error))?;
    read_parquet_chunk_reader_with_declared_schema_and_scope(
        file,
        options,
        declared_schema,
        type_policy,
        scope,
        position,
    )
}

pub fn stream_parquet_file_with_declared_schema_and_type_policy(
    path: &Path,
    options: &ReadOptions,
    declared_schema: SchemaRef,
    type_policy: &TypePolicy,
    position: Option<SourcePosition>,
) -> Result<BatchStream> {
    let file = fs::File::open(path)
        .map_err(|error| io_data_error(format!("open {}", path.display()), error))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|error| parquet_data_error("read Parquet file metadata", error))?
        .with_batch_size(options.batch_size);
    let physical_schema = builder.schema().clone();
    let physical_schema_hash = schema_hash(physical_schema.as_ref())?;
    let declared_schema = if declared_schema.fields().is_empty() {
        Arc::clone(&physical_schema)
    } else {
        declared_schema
    };
    let reconciliation = reconcile_schema(
        physical_schema.as_ref(),
        declared_schema.as_ref(),
        type_policy,
    )?;
    let reconciliation_plan = serde_json::to_string(&reconciliation.plan)
        .map_err(|error| CdfError::internal(format!("serialize schema coercion plan: {error}")))?;
    let reconciled_schema = Arc::new(reconciliation.schema);
    let reconciled_schema_hash = schema_hash(reconciled_schema.as_ref())?;
    let projected_sources = reconciled_schema
        .fields()
        .iter()
        .map(|field| field_source_name(field.as_ref()).to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    let reader = builder
        .build()
        .map_err(|error| parquet_data_error("create Parquet record batch reader", error))?;
    let options = options.clone();
    let state = (reader, 0_usize, 0_u64);
    Ok(Box::pin(stream::try_unfold(
        state,
        move |(mut reader, batch_index, source_row_ordinal)| {
            let physical_schema = Arc::clone(&physical_schema);
            let reconciled_schema = Arc::clone(&reconciled_schema);
            let projected_sources = projected_sources.clone();
            let reconciled_schema_hash = reconciled_schema_hash.clone();
            let physical_schema_hash = physical_schema_hash.clone();
            let reconciliation_plan = reconciliation_plan.clone();
            let position = position.clone();
            let options = options.clone();
            async move {
                let next = reader.next().transpose().map_err(CdfError::from)?;
                let physical_batch = match next {
                    Some(batch) => batch,
                    None if batch_index == 0 => {
                        RecordBatch::new_empty(Arc::clone(&physical_schema))
                    }
                    None => return Ok(None),
                };
                let row_count = physical_batch.num_rows() as u64;
                let mut candidates = Vec::new();
                for (field_index, field) in physical_schema.fields().iter().enumerate() {
                    let source = field_source_name(field.as_ref());
                    if projected_sources.contains(source) {
                        continue;
                    }
                    let values = physical_batch.column(field_index).clone();
                    for row in 0..physical_batch.num_rows() {
                        candidates.push(PreContractResidualCandidate::new(
                            source_row_ordinal + row as u64,
                            row,
                            vec![source.to_owned()],
                            field.as_ref().clone(),
                            None,
                            values.clone(),
                            row,
                        )?);
                    }
                }
                let reconciled = reconcile_record_batch(
                    physical_schema.as_ref(),
                    Arc::clone(&reconciled_schema),
                    physical_batch,
                    "Parquet",
                )?;
                let mut batch = Batch::from_record_batch(
                    BatchId::new(format!(
                        "{}-{:06}",
                        options.batch_id_prefix,
                        batch_index + 1
                    ))?,
                    options.resource_id.clone(),
                    options.partition_id.clone(),
                    reconciled_schema_hash,
                    reconciled,
                )?;
                batch.header.observed_schema_hash = physical_schema_hash;
                batch.header.source_position = position;
                batch.header.schema_coercion_plan = Some(reconciliation_plan);
                batch.header.extend_residual_candidates(candidates);
                Ok(Some((
                    batch,
                    (reader, batch_index + 1, source_row_ordinal + row_count),
                )))
            }
        },
    )))
}

fn read_parquet_chunk_reader_with_declared_schema_and_scope<T: ChunkReader + 'static>(
    reader: T,
    options: &ReadOptions,
    declared_schema: SchemaRef,
    type_policy: &TypePolicy,
    scope: ScopeKey,
    position: Option<SourcePosition>,
) -> Result<FormatRead> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader)
        .map_err(|error| parquet_data_error("read Parquet file metadata", error))?
        .with_batch_size(options.batch_size);
    let physical_schema = builder.schema().clone();
    let physical_schema_hash = schema_hash(physical_schema.as_ref())?;
    let reconciliation = reconcile_schema(
        physical_schema.as_ref(),
        declared_schema.as_ref(),
        type_policy,
    )?;
    let reconciliation_plan = reconciliation.plan;
    let reconciled_schema = Arc::new(reconciliation.schema);
    let mut reader = builder
        .build()
        .map_err(|error| parquet_data_error("create Parquet record batch reader", error))?;
    let physical_batches = collect_record_batches(&mut reader)?;
    let reconciled = reconcile_record_batches(
        physical_schema.as_ref(),
        reconciled_schema.clone(),
        physical_batches,
        "Parquet",
    )?;

    with_observed_schema_hash(
        build_output_with_pre_contract_evidence(
            reconciled_schema,
            reconciled.batches,
            options,
            scope,
            position,
            PreContractReadEvidence {
                quarantine: Vec::new(),
                residual_candidates: reconciled.residual_candidates,
                schema_coercion_plan: Some(&reconciliation_plan),
            },
        )?,
        physical_schema_hash,
    )
}

struct ReconciledRecordBatches {
    batches: Vec<RecordBatch>,
    residual_candidates: Vec<Vec<PreContractResidualCandidate>>,
}

fn reconcile_record_batches(
    physical_schema: &Schema,
    reconciled_schema: SchemaRef,
    record_batches: Vec<RecordBatch>,
    format_name: &str,
) -> Result<ReconciledRecordBatches> {
    let projected_sources = reconciled_schema
        .fields()
        .iter()
        .map(|field| field_source_name(field.as_ref()))
        .collect::<std::collections::BTreeSet<_>>();
    let mut batches = Vec::with_capacity(record_batches.len());
    let mut residual_candidates = Vec::with_capacity(record_batches.len());
    let mut source_row_ordinal = 0_u64;
    for batch in record_batches {
        let mut candidates = Vec::new();
        for (field_index, field) in physical_schema.fields().iter().enumerate() {
            let source = field_source_name(field.as_ref());
            if projected_sources.contains(source) {
                continue;
            }
            let values = batch.column(field_index).clone();
            for row in 0..batch.num_rows() {
                candidates.push(PreContractResidualCandidate::new(
                    source_row_ordinal + row as u64,
                    row,
                    vec![source.to_owned()],
                    field.as_ref().clone(),
                    None,
                    values.clone(),
                    row,
                )?);
            }
        }
        let row_count = batch.num_rows() as u64;
        batches.push(reconcile_record_batch(
            physical_schema,
            reconciled_schema.clone(),
            batch,
            format_name,
        )?);
        residual_candidates.push(candidates);
        source_row_ordinal += row_count;
    }
    Ok(ReconciledRecordBatches {
        batches,
        residual_candidates,
    })
}

fn reconcile_record_batch(
    physical_schema: &Schema,
    reconciled_schema: SchemaRef,
    batch: RecordBatch,
    format_name: &str,
) -> Result<RecordBatch> {
    let columns = reconciled_schema
        .fields()
        .iter()
        .map(|field| reconciled_column(physical_schema, &batch, field.as_ref(), format_name))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(reconciled_schema, columns).map_err(CdfError::from)
}

fn reconciled_column(
    physical_schema: &Schema,
    batch: &RecordBatch,
    output_field: &Field,
    format_name: &str,
) -> Result<ArrayRef> {
    let source = source_name(output_field).unwrap_or_else(|| output_field.name());
    let physical_index = physical_schema
        .fields()
        .iter()
        .position(|field| field_source_name(field.as_ref()) == source);
    let Some(physical_index) = physical_index else {
        if output_field.is_nullable() {
            return Ok(new_null_array(output_field.data_type(), batch.num_rows()));
        }
        return Err(CdfError::internal(format!(
            "reconciled {format_name} field {:?} has no matching physical source field {source:?}",
            output_field.name()
        )));
    };
    let column = batch.column(physical_index);
    if column.data_type() == output_field.data_type() {
        return Ok(column.clone());
    }
    if !can_cast_types(column.data_type(), output_field.data_type()) {
        return Err(CdfError::contract(format!(
            "{format_name} schema reconciliation selected unsupported materialized cast for field {:?}: observed type {}; declared type {}",
            source,
            column.data_type(),
            output_field.data_type()
        )));
    }
    cast(column.as_ref(), output_field.data_type()).map_err(CdfError::from)
}

fn with_observed_schema_hash(
    mut read: FormatRead,
    observed_schema_hash: SchemaHash,
) -> Result<FormatRead> {
    for batch in &mut read.batches {
        batch.header.observed_schema_hash = observed_schema_hash.clone();
    }
    Ok(read)
}

fn strict_source_type_policy() -> TypePolicy {
    let mut type_policy = ContractPolicy::default().types;
    type_policy.coerce_types = false;
    type_policy.allow_lossy_mapping = false;
    type_policy
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
    build_output_with_pre_contract_evidence(
        schema,
        record_batches,
        options,
        scope,
        position,
        PreContractReadEvidence {
            quarantine: Vec::new(),
            residual_candidates: Vec::new(),
            schema_coercion_plan: None,
        },
    )
}

struct PreContractReadEvidence<'a> {
    quarantine: Vec<PreContractQuarantineFact>,
    residual_candidates: Vec<Vec<PreContractResidualCandidate>>,
    schema_coercion_plan: Option<&'a SchemaCoercionPlan>,
}

fn build_output_with_pre_contract_evidence(
    schema: SchemaRef,
    mut record_batches: Vec<RecordBatch>,
    options: &ReadOptions,
    scope: ScopeKey,
    position: Option<SourcePosition>,
    evidence: PreContractReadEvidence<'_>,
) -> Result<FormatRead> {
    let PreContractReadEvidence {
        mut quarantine,
        mut residual_candidates,
        schema_coercion_plan,
    } = evidence;
    if record_batches.is_empty() {
        record_batches.push(RecordBatch::new_empty(Arc::clone(&schema)));
    }
    residual_candidates.resize_with(record_batches.len(), Vec::new);
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
        deduplication: None,
        contract: None,
        state_scope: scope,
        freshness: None,
        trust_level: TrustLevel::Experimental,
    };
    let schema_coercion_plan = schema_coercion_plan
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| CdfError::internal(format!("serialize schema coercion plan: {error}")))?;

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
        batch.header.schema_coercion_plan = schema_coercion_plan.clone();
        if index == 0 {
            batch.header.pre_contract_quarantine = std::mem::take(&mut quarantine);
        }
        batch
            .header
            .extend_residual_candidates(std::mem::take(&mut residual_candidates[index]));
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
    accepted_rows: usize,
    quarantine_facts: Vec<PreContractQuarantineFact>,
    residual_candidates: Vec<PendingResidualCandidate>,
}

struct PendingResidualCandidate {
    accepted_row_ordinal: usize,
    source_row_ordinal: u64,
    source_path: Vec<String>,
    observed_field: Field,
    expected_field: Option<Field>,
    value: ArrayRef,
    value_index: usize,
}

fn filter_declared_ndjson_rows(
    bytes: &[u8],
    schema: &arrow_schema::Schema,
    type_policy: &TypePolicy,
    _position: &Option<SourcePosition>,
) -> Result<DeclaredNdjsonFilter> {
    validate_declared_json_schema(schema)?;
    let declared_fields = schema
        .fields()
        .iter()
        .map(|field| (field_source_name(field.as_ref()), field.as_ref()))
        .collect::<BTreeMap<_, _>>();
    let text = std::str::from_utf8(bytes).map_err(|error| {
        CdfError::data(format!("NDJSON file source is not valid UTF-8: {error}"))
    })?;
    let mut accepted_ndjson = Vec::new();
    let mut accepted_rows = 0_usize;
    let quarantine_facts = Vec::new();
    let mut residual_candidates = Vec::new();
    let mut row_ordinal = 0_u64;

    for (line_index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            return Err(CdfError::data(format!(
                "NDJSON file source line {} is empty",
                line_index + 1
            )));
        }
        let mut row: Value = serde_json::from_str(line).map_err(json_error)?;
        let object = row.as_object_mut().ok_or_else(|| {
            CdfError::data(format!(
                "NDJSON file source line {} must be a JSON object",
                line_index + 1
            ))
        })?;
        let unknown = object
            .iter()
            .filter(|(source, _)| !declared_fields.contains_key(source.as_str()))
            .map(|(source, value)| (source.clone(), value.clone()))
            .collect::<Vec<_>>();
        for (source, value) in unknown {
            let (observed_field, value_array) = json_residual_array(&source, &value)?;
            residual_candidates.push(PendingResidualCandidate {
                accepted_row_ordinal: accepted_rows,
                source_row_ordinal: row_ordinal,
                source_path: vec![source.clone()],
                observed_field,
                expected_field: None,
                value: value_array,
                value_index: 0,
            });
            object.remove(&source);
        }
        for (source, field) in &declared_fields {
            let Some(value) = object.get(*source).cloned() else {
                continue;
            };
            if value.is_null() {
                continue;
            }
            if declared_json_type_mismatch(field, &value, type_policy)? {
                let (observed_field, value_array) = json_residual_array(source, &value)?;
                residual_candidates.push(PendingResidualCandidate {
                    accepted_row_ordinal: accepted_rows,
                    source_row_ordinal: row_ordinal,
                    source_path: vec![(*source).to_owned()],
                    observed_field,
                    expected_field: Some((*field).clone()),
                    value: value_array,
                    value_index: 0,
                });
                object.insert((*source).to_owned(), Value::Null);
            }
        }

        serde_json::to_writer(&mut accepted_ndjson, &row).map_err(json_error)?;
        accepted_ndjson.push(b'\n');
        accepted_rows += 1;
        row_ordinal += 1;
    }

    Ok(DeclaredNdjsonFilter {
        accepted_rows,
        accepted_ndjson,
        quarantine_facts,
        residual_candidates,
    })
}

fn repair_residual_null_inference(
    physical: Schema,
    declared: &Schema,
    _candidates: &[PendingResidualCandidate],
) -> Schema {
    let fields = physical
        .fields()
        .iter()
        .map(|field| {
            let source = field_source_name(field.as_ref());
            if field.data_type() != &DataType::Null {
                return field.as_ref().clone();
            }
            let Some(expected) = declared
                .fields()
                .iter()
                .find(|expected| field_source_name(expected.as_ref()) == source)
            else {
                return field.as_ref().clone();
            };
            Field::new(field.name(), expected.data_type().clone(), true)
                .with_metadata(field.metadata().clone())
        })
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, physical.metadata().clone())
}

fn nullable_residual_decode_schema(
    reconciled: Schema,
    candidates: &[PendingResidualCandidate],
) -> Schema {
    let residual_sources = candidates
        .iter()
        .filter(|candidate| candidate.expected_field.is_some())
        .filter_map(|candidate| candidate.source_path.first().map(String::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    let fields = reconciled
        .fields()
        .iter()
        .map(|field| {
            if residual_sources.contains(field_source_name(field.as_ref())) {
                field.as_ref().clone().with_nullable(true)
            } else {
                field.as_ref().clone()
            }
        })
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, reconciled.metadata().clone())
}

fn json_residual_array(source: &str, value: &Value) -> Result<(Field, ArrayRef)> {
    const VALUE_FIELD: &str = "__cdf_residual_value";
    let row = serde_json::json!({VALUE_FIELD: value});
    let mut encoded = serde_json::to_vec(&row).map_err(json_error)?;
    encoded.push(b'\n');
    let (schema, _) =
        infer_json_schema(Cursor::new(encoded.as_slice()), Some(1)).map_err(CdfError::from)?;
    let schema = Arc::new(schema);
    let mut reader = JsonReaderBuilder::new(schema.clone())
        .with_batch_size(1)
        .build(Cursor::new(encoded))
        .map_err(CdfError::from)?;
    let batch = reader
        .next()
        .transpose()
        .map_err(CdfError::from)?
        .ok_or_else(|| CdfError::internal("residual JSON scalar produced no Arrow batch"))?;
    let field = Field::new(source, schema.field(0).data_type().clone(), true);
    Ok((field, batch.column(0).clone()))
}

fn merge_pending_residual_candidates(
    target: &mut [Vec<PreContractResidualCandidate>],
    batches: &[RecordBatch],
    pending: Vec<PendingResidualCandidate>,
) -> Result<()> {
    let mut row_offset = 0_usize;
    for candidate in pending {
        let mut matched = false;
        for (batch_index, batch) in batches.iter().enumerate() {
            let row_end = row_offset + batch.num_rows();
            if candidate.accepted_row_ordinal < row_end {
                target[batch_index].push(PreContractResidualCandidate::new(
                    candidate.source_row_ordinal,
                    candidate.accepted_row_ordinal - row_offset,
                    candidate.source_path,
                    candidate.observed_field,
                    candidate.expected_field,
                    candidate.value,
                    candidate.value_index,
                )?);
                matched = true;
                break;
            }
            row_offset = row_end;
        }
        if !matched {
            return Err(CdfError::internal(
                "residual candidate row was not present in decoded Arrow batches",
            ));
        }
        row_offset = 0;
    }
    Ok(())
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
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Timestamp(_, _)
    )
}

fn declared_json_type_mismatch(
    field: &Field,
    value: &Value,
    type_policy: &TypePolicy,
) -> Result<bool> {
    Ok(match field.data_type() {
        DataType::Boolean => !(value.is_boolean() || value.is_string() && type_policy.coerce_types),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            !(json_signed_integer(value)
                || value.is_number() && type_policy.allow_lossy_mapping
                || value.is_string() && type_policy.coerce_types)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            !(json_unsigned_integer(value)
                || value.is_number() && type_policy.allow_lossy_mapping
                || value.is_string() && type_policy.coerce_types)
        }
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => {
            !(value.is_number() || value.is_string() && type_policy.coerce_types)
        }
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

fn json_signed_integer(value: &Value) -> bool {
    value.as_i64().is_some() || value.as_u64().is_some_and(|value| value <= i64::MAX as u64)
}

fn json_unsigned_integer(value: &Value) -> bool {
    value.as_u64().is_some()
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
