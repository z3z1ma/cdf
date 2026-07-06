#![doc = "External data format boundary for firn."]

use std::{
    fs,
    io::{Cursor, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_csv::reader::{Format as ArrowCsvFormat, ReaderBuilder as CsvReaderBuilder};
use arrow_ipc::reader::StreamReader;
use arrow_json::reader::{ReaderBuilder as JsonReaderBuilder, infer_json_schema};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use firn_contract::{
    ContractPolicy, ObservedSchema, ValidationProgram, compile_validation_program,
};
use firn_kernel::{
    Batch, BatchId, FileManifest, FilePosition, FirnError, PartitionId, ResourceDescriptor,
    ResourceId, Result, SchemaHash, SchemaSource, ScopeKey, SourcePosition, TrustLevel,
    WriteDisposition,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

pub const DEFAULT_BATCH_SIZE: usize = 1024;
pub const SCHEMA_HASH_PREFIX: &str = "sha256:";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadOptions {
    pub resource_id: ResourceId,
    pub partition_id: PartitionId,
    pub batch_id_prefix: String,
    pub batch_size: usize,
}

impl ReadOptions {
    pub fn new(resource_id: ResourceId, partition_id: PartitionId) -> Self {
        let batch_id_prefix = format!(
            "{}-{}",
            sanitize_id_part(resource_id.as_str()),
            sanitize_id_part(partition_id.as_str())
        );
        Self {
            resource_id,
            partition_id,
            batch_id_prefix,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn with_batch_id_prefix(mut self, prefix: impl Into<String>) -> Result<Self> {
        let prefix = prefix.into();
        if prefix.trim().is_empty() {
            return Err(FirnError::contract("batch id prefix cannot be empty"));
        }
        self.batch_id_prefix = prefix;
        Ok(self)
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Result<Self> {
        if batch_size == 0 {
            return Err(FirnError::contract("batch size must be greater than zero"));
        }
        self.batch_size = batch_size;
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    Csv(CsvOptions),
    Json(JsonOptions),
    Ndjson(JsonOptions),
    Parquet,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvOptions {
    pub has_header: bool,
    pub delimiter: u8,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonOptions {
    pub max_read_records: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSource {
    pub path: PathBuf,
    pub format: FileFormat,
    pub options: ReadOptions,
}

impl FileSource {
    pub fn new(path: impl Into<PathBuf>, format: FileFormat, options: ReadOptions) -> Self {
        Self {
            path: path.into(),
            format,
            options,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FormatRead {
    pub descriptor: ResourceDescriptor,
    pub observed_schema: ObservedSchema,
    pub schema_hash: SchemaHash,
    pub batches: Vec<Batch>,
}

pub fn read_file_source(source: &FileSource) -> Result<FormatRead> {
    if matches!(source.format, FileFormat::Parquet) {
        return Err(FirnError::contract(
            "Parquet file source support is blocked by supply-chain policy: arrow-rs parquet currently pulls RUSTSEC-2024-0436 through paste",
        ));
    }

    let bytes = fs::read(&source.path)
        .map_err(|error| io_data_error(format!("read {}", source.path.display()), error))?;
    let position = file_source_position(&source.path)?;
    let scope = ScopeKey::File {
        path: path_string(&source.path)?,
    };

    match &source.format {
        FileFormat::Csv(options) => {
            read_csv_bytes_with_scope(&bytes, &source.options, options, scope, Some(position))
        }
        FileFormat::Json(options) => {
            read_json_bytes_with_scope(&bytes, &source.options, options, scope, Some(position))
        }
        FileFormat::Ndjson(options) => {
            read_ndjson_bytes_with_scope(&bytes, &source.options, options, scope, Some(position))
        }
        FileFormat::Parquet => unreachable!("Parquet support returns before file reads"),
    }
}

pub fn read_arrow_ipc_stream<R: Read>(reader: R, options: &ReadOptions) -> Result<FormatRead> {
    let mut reader = StreamReader::try_new(reader, None).map_err(FirnError::from)?;
    let schema = reader.schema();
    let record_batches = collect_record_batches(&mut reader)?;
    build_output(
        schema,
        record_batches,
        options,
        ScopeKey::Stream {
            name: "arrow_ipc_stdout".to_owned(),
        },
        None,
    )
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
        .map_err(FirnError::from)?;
    Ok(ObservedSchema::from_arrow(&schema))
}

pub fn compile_observed_schema(
    policy: &ContractPolicy,
    observed_schema: &ObservedSchema,
) -> Result<ValidationProgram> {
    compile_validation_program(policy, observed_schema)
}

pub fn schema_hash(schema: &Schema) -> Result<SchemaHash> {
    let mut hasher = Sha256::new();
    hash_schema(&mut hasher, schema);
    SchemaHash::new(format!(
        "{SCHEMA_HASH_PREFIX}{}",
        hex::encode(hasher.finalize())
    ))
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
        .map_err(FirnError::from)?;
    let schema = Arc::new(schema);
    let mut reader = CsvReaderBuilder::new(schema.clone())
        .with_format(format)
        .with_batch_size(options.batch_size)
        .build(Cursor::new(bytes))
        .map_err(FirnError::from)?;
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
        .map_err(FirnError::from)?;
    let schema = Arc::new(schema);
    let mut reader = JsonReaderBuilder::new(schema.clone())
        .with_batch_size(options.batch_size)
        .build(Cursor::new(bytes))
        .map_err(FirnError::from)?;
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
            return Err(FirnError::data(
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
        .map(|batch| batch.map_err(FirnError::from))
        .collect::<Result<Vec<_>>>()
}

fn json_document_to_ndjson(bytes: &[u8]) -> Result<Vec<u8>> {
    let value: Value = serde_json::from_slice(bytes).map_err(json_error)?;
    let rows = match value {
        Value::Array(rows) => rows,
        Value::Object(_) => vec![value],
        _ => {
            return Err(FirnError::data(
                "JSON file source must be an object or an array of objects",
            ));
        }
    };

    let mut output = Vec::new();
    for row in rows {
        if !row.is_object() {
            return Err(FirnError::data(
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
        .ok_or_else(|| FirnError::data(format!("path is not valid UTF-8: {}", path.display())))
}

fn hash_schema(hasher: &mut Sha256, schema: &Schema) {
    hasher.update(b"schema");
    for field in schema.fields() {
        hash_field(hasher, field.as_ref());
    }
    hash_metadata(hasher, schema.metadata());
}

fn hash_field(hasher: &mut Sha256, field: &arrow_schema::Field) {
    hasher.update(b"field");
    hasher.update(field.name().as_bytes());
    hasher.update(field.data_type().to_string().as_bytes());
    hasher.update([u8::from(field.is_nullable())]);
    hash_metadata(hasher, field.metadata());
}

fn hash_metadata(hasher: &mut Sha256, metadata: &std::collections::HashMap<String, String>) {
    let mut entries = metadata.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(key, _)| key.as_str());
    for (key, value) in entries {
        hasher.update(key.as_bytes());
        hasher.update(b"=");
        hasher.update(value.as_bytes());
        hasher.update(b"\n");
    }
}

fn sanitize_id_part(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '-' || character == '_' {
                character
            } else {
                '-'
            }
        })
        .collect()
}

fn io_data_error(context: impl Into<String>, error: std::io::Error) -> FirnError {
    FirnError::data(format!("{}: {error}", context.into()))
}

fn json_error(error: serde_json::Error) -> FirnError {
    FirnError::data(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{collections::BTreeMap, collections::HashMap};

    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{DataType, Field};
    use firn_contract::NORMALIZER_NAMECASE_V1;
    use firn_kernel::{ErrorKind, SegmentId, with_source_name};

    fn options(resource: &str, partition: &str) -> ReadOptions {
        ReadOptions::new(
            ResourceId::new(resource).unwrap(),
            PartitionId::new(partition).unwrap(),
        )
    }

    fn sample_schema() -> SchemaRef {
        let mut metadata = HashMap::new();
        metadata.insert("source".to_owned(), "fixture".to_owned());
        Arc::new(Schema::new_with_metadata(
            vec![
                with_source_name(Field::new("id", DataType::Int64, false), "ID"),
                Field::new("name", DataType::Utf8, true),
            ],
            metadata,
        ))
    }

    fn sample_batch() -> RecordBatch {
        let schema = sample_schema();
        let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let name: ArrayRef = Arc::new(StringArray::from(vec![Some("ada"), None, Some("grace")]));
        RecordBatch::try_new(schema, vec![id, name]).unwrap()
    }

    fn record_batches(read: &FormatRead) -> Vec<RecordBatch> {
        read.batches
            .iter()
            .map(|batch| batch.record_batch().unwrap().clone())
            .collect()
    }

    #[test]
    fn arrow_ipc_stream_round_trips_kernel_batches_without_schema_loss() {
        let input = sample_batch();
        let mut bytes = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut bytes, input.schema().as_ref()).unwrap();
            writer.write(&input).unwrap();
            writer.finish().unwrap();
        }

        let read = read_arrow_ipc_stream(Cursor::new(bytes), &options("orders", "p0")).unwrap();

        assert_eq!(read.batches.len(), 1);
        let output = read.batches[0].record_batch().unwrap();
        assert_eq!(output.schema().as_ref(), input.schema().as_ref());
        assert_eq!(
            output.schema().field_with_name("id").unwrap().metadata(),
            input.schema().field_with_name("id").unwrap().metadata()
        );
        assert_eq!(
            read.descriptor.schema_source,
            SchemaSource::Discovered {
                schema_hash: Some(read.schema_hash.clone())
            }
        );
    }

    #[test]
    fn ndjson_inference_feeds_contract_observed_schema() {
        let ndjson = br#"{"Order ID":1,"amount":10.5,"tags":["new","vip"]}
{"Order ID":2,"amount":11.0,"tags":["repeat"]}
"#;
        let read = read_ndjson_bytes(
            ndjson,
            &options("orders", "p0").with_batch_size(16).unwrap(),
            &JsonOptions::default(),
        )
        .unwrap();
        let program =
            compile_observed_schema(&ContractPolicy::evolve(), &read.observed_schema).unwrap();

        assert_eq!(program.normalizer_version, NORMALIZER_NAMECASE_V1);
        assert!(program.column_programs.iter().any(|column| {
            column.source_name == "Order ID" && column.output_name == "order_id"
        }));
        assert_eq!(
            read.batches[0].header.observed_schema_hash,
            read.schema_hash
        );
    }

    #[test]
    fn csv_and_json_file_sources_produce_descriptors_and_batches() {
        let temp = tempfile::tempdir().unwrap();
        let csv_path = temp.path().join("orders.csv");
        fs::write(&csv_path, "id,name\n1,ada\n2,grace\n").unwrap();

        let csv = read_file_source(&FileSource::new(
            &csv_path,
            FileFormat::Csv(CsvOptions::default()),
            options("orders_csv", "file"),
        ))
        .unwrap();
        assert_eq!(
            csv.batches
                .iter()
                .map(|batch| batch.header.row_count)
                .sum::<u64>(),
            2
        );
        assert!(matches!(csv.descriptor.state_scope, ScopeKey::File { .. }));
        assert!(matches!(
            csv.batches[0].header.source_position,
            Some(SourcePosition::FileManifest(_))
        ));

        let json_path = temp.path().join("orders.json");
        fs::write(
            &json_path,
            r#"[{"id":1,"name":"ada"},{"id":2,"name":"grace"}]"#,
        )
        .unwrap();
        let json = read_file_source(&FileSource::new(
            &json_path,
            FileFormat::Json(JsonOptions::default()),
            options("orders_json", "file"),
        ))
        .unwrap();
        assert_eq!(json.batches[0].header.row_count, 2);
    }

    #[test]
    fn parquet_file_source_reports_supply_chain_blocker() {
        let error = read_file_source(&FileSource::new(
            "orders.parquet",
            FileFormat::Parquet,
            options("orders_parquet", "file"),
        ))
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Contract);
        assert!(error.message.contains("RUSTSEC-2024-0436"));
    }

    #[test]
    fn malformed_inputs_map_to_data_errors() {
        let error =
            read_arrow_ipc_stream(Cursor::new(b"not-ipc".as_slice()), &options("bad", "p0"))
                .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Data);

        let error = read_ndjson_bytes(
            br#"{"id":1}
{bad}
"#,
            &options("bad", "p0"),
            &JsonOptions::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Data);

        let error = read_json_bytes(
            br#"[1,2,3]"#,
            &options("bad", "p0"),
            &JsonOptions::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Data);
    }

    #[test]
    fn adapter_output_can_be_packaged_and_replayed_like_native_output() {
        let read = read_ndjson_bytes(
            br#"{"id":1,"name":"ada"}
{"id":2,"name":"grace"}
"#,
            &options("orders", "p0"),
            &JsonOptions::default(),
        )
        .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let mut builder = firn_package::PackageBuilder::create(temp.path(), "pkg-formats").unwrap();
        builder
            .write_json_artifact(
                "schema/observed.arrow.json",
                &BTreeMap::from([("schema_hash", read.schema_hash.as_str())]),
            )
            .unwrap();
        builder
            .write_segment(
                SegmentId::new("seg-formats").unwrap(),
                &record_batches(&read),
            )
            .unwrap();
        let manifest = builder.finish().unwrap();
        let reader = firn_package::PackageReader::open(temp.path()).unwrap();
        reader.verify().unwrap();
        let replayed = reader
            .read_segment(&SegmentId::new("seg-formats").unwrap())
            .unwrap();

        assert_eq!(manifest.identity.segments[0].row_count, 2);
        assert_eq!(
            replayed[0].schema().as_ref(),
            record_batches(&read)[0].schema().as_ref()
        );
    }
}
