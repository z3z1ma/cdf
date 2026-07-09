use super::*;

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::{Cursor, Write},
    path::Path,
    sync::Arc,
};

use arrow_array::{
    Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
use arrow_ipc::writer::{FileWriter, StreamWriter};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_conformance::resource::{
    ResourceExecutionConformanceCase, assert_resource_stream_conformance,
    assert_resource_stream_execution_conformance,
};
use cdf_contract::{ArrowType, ContractPolicy, NORMALIZER_NAMECASE_V1};
use cdf_kernel::{
    ErrorKind, PartitionId, PreContractObservedValue, ResourceId, ResourceStream, ScanRequest,
    SchemaHash, ScopeKey, SegmentId, SourcePosition, physical_type, source_name, with_semantic,
    with_source_name,
};

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

fn write_parquet_file(path: &Path, batches: &[RecordBatch]) {
    let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(batches).unwrap();
    fs::write(path, bytes).unwrap();
}

fn gzip_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

fn zstd_bytes(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(Cursor::new(bytes), 0).unwrap()
}

fn file_scan_request(resource: &FileResource) -> ScanRequest {
    ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: resource.descriptor().state_scope.clone(),
    }
}

fn assert_file_resource_conformance(
    source: &FileSource,
    expected_schema_hash: &SchemaHash,
    expected_rows: u64,
) {
    let resource = FileResource::new(source.clone()).unwrap();
    let request = file_scan_request(&resource);
    assert_resource_stream_conformance(&resource, [request.clone()]);
    futures_executor::block_on(assert_resource_stream_execution_conformance(
        &resource,
        [ResourceExecutionConformanceCase::new(
            request,
            expected_schema_hash.clone(),
            [source.options.partition_id.clone()],
            expected_rows,
        )
        .with_expected_partition_rows([(source.options.partition_id.clone(), expected_rows)])
        .require_file_manifest_positions()],
    ));
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
        read.descriptor
            .schema_source
            .pinned_snapshot()
            .map(|snapshot| &snapshot.schema_hash),
        Some(&read.schema_hash)
    );
}

#[test]
fn arrow_ipc_file_round_trips_kernel_batches_without_schema_loss() {
    let input = sample_batch();
    let mut bytes = Cursor::new(Vec::new());
    {
        let mut writer = FileWriter::try_new(&mut bytes, input.schema().as_ref()).unwrap();
        writer.write(&input).unwrap();
        writer.finish().unwrap();
    }

    let read =
        read_arrow_ipc_file(Cursor::new(bytes.into_inner()), &options("orders", "p0")).unwrap();

    assert_eq!(read.batches.len(), 1);
    let output = read.batches[0].record_batch().unwrap();
    assert_eq!(output.schema().as_ref(), input.schema().as_ref());
    assert_eq!(
        output.schema().field_with_name("id").unwrap().metadata(),
        input.schema().field_with_name("id").unwrap().metadata()
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
    assert!(
        program
            .column_programs
            .iter()
            .any(|column| { column.source_name == "Order ID" && column.output_name == "order_id" })
    );
    assert_eq!(
        read.batches[0].header.observed_schema_hash,
        read.schema_hash
    );
}

#[test]
fn declared_ndjson_scalar_type_mismatch_quarantines_row_and_preserves_accepted_order() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("event_type", DataType::Utf8, false),
    ]));
    let ndjson = br#"{"id":1,"event_type":"order.created"}
{"id":2,"event_type":42}
{"id":3,"event_type":"order.shipped"}
"#;

    let read = read_ndjson_bytes_with_declared_schema(
        ndjson,
        &options("events", "p0").with_batch_size(16).unwrap(),
        &JsonOptions::default(),
        schema,
    )
    .unwrap();

    assert_eq!(read.batches.len(), 1);
    let batch = read.batches[0].record_batch().unwrap();
    assert_eq!(batch.num_rows(), 2);
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let event_types = batch
        .column_by_name("event_type")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!([ids.value(0), ids.value(1)], [1, 3]);
    assert_eq!(event_types.value(0), "order.created");
    assert_eq!(event_types.value(1), "order.shipped");

    let facts = &read.batches[0].header.pre_contract_quarantine;
    assert_eq!(facts.len(), 1);
    assert_eq!(facts[0].source_row_ordinal, 1);
    assert_eq!(facts[0].rule_id, "source-decode:event_type:type-mismatch");
    assert_eq!(facts[0].error_code, "source_type_mismatch");
    assert_eq!(
        facts[0].observed_value_redacted,
        PreContractObservedValue::Preserved {
            value: "42".to_owned()
        }
    );
}

#[test]
fn declared_ndjson_malformed_json_still_fails_closed() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "event_type",
        DataType::Utf8,
        false,
    )]));
    let error = read_ndjson_bytes_with_declared_schema(
        br#"{"event_type":"order.created"}
{bad}
"#,
        &options("events", "p0"),
        &JsonOptions::default(),
        schema,
    )
    .unwrap_err();

    assert_eq!(error.kind, ErrorKind::Data);
}

#[test]
fn declared_ndjson_type_mismatch_hashes_pii_observed_value() {
    let schema = Arc::new(Schema::new(vec![with_semantic(
        Field::new("email", DataType::Int64, false),
        "pii:email",
    )]));

    let read = read_ndjson_bytes_with_declared_schema(
        br#"{"email":"alice@example.com"}
"#,
        &options("events", "p0"),
        &JsonOptions::default(),
        schema,
    )
    .unwrap();

    let facts = &read.batches[0].header.pre_contract_quarantine;
    assert_eq!(facts.len(), 1);
    assert_eq!(
        facts[0].observed_value_redacted,
        PreContractObservedValue::Hashed {
            algorithm: "sha256".to_owned(),
            value: "sha256:ff8d9819fc0e12bf0d24892e45987e249a28dce836a85cad60e28eaaa8c6d976"
                .to_owned()
        }
    );
}

#[test]
fn csv_json_and_ndjson_file_sources_produce_descriptors_and_batches() {
    let temp = tempfile::tempdir().unwrap();
    let csv_path = temp.path().join("orders.csv");
    fs::write(&csv_path, "id,name\n1,ada\n2,grace\n").unwrap();

    let csv_source = FileSource::new(
        &csv_path,
        FileFormat::Csv(CsvOptions::default()),
        options("orders_csv", "file"),
    );
    let csv = read_file_source(&csv_source).unwrap();
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
    assert_file_resource_conformance(&csv_source, &csv.schema_hash, 2);

    let json_path = temp.path().join("orders.json");
    fs::write(
        &json_path,
        r#"[{"id":1,"name":"ada"},{"id":2,"name":"grace"}]"#,
    )
    .unwrap();
    let json_source = FileSource::new(
        &json_path,
        FileFormat::Json(JsonOptions::default()),
        options("orders_json", "file"),
    );
    let json = read_file_source(&json_source).unwrap();
    assert_eq!(json.batches[0].header.row_count, 2);
    assert_file_resource_conformance(&json_source, &json.schema_hash, 2);

    let ndjson_path = temp.path().join("orders.ndjson");
    fs::write(
        &ndjson_path,
        r#"{"id":1,"name":"ada"}
{"id":2,"name":"grace"}
"#,
    )
    .unwrap();
    let ndjson_source = FileSource::new(
        &ndjson_path,
        FileFormat::Ndjson(JsonOptions::default()),
        options("orders_ndjson", "file"),
    );
    let ndjson = read_file_source(&ndjson_source).unwrap();
    assert_eq!(ndjson.batches[0].header.row_count, 2);
    assert_file_resource_conformance(&ndjson_source, &ndjson.schema_hash, 2);

    let json_object_path = temp.path().join("single-order.json");
    fs::write(&json_object_path, r#"{"id":3,"name":"alan"}"#).unwrap();
    let json_object = read_file_source(&FileSource::new(
        &json_object_path,
        FileFormat::Json(JsonOptions::default()),
        options("single_order_json", "file"),
    ))
    .unwrap();
    assert_eq!(json_object.batches[0].header.row_count, 1);
}

#[test]
fn compression_ndjson_file_sources_decode_and_preserve_compressed_identity() {
    let temp = tempfile::tempdir().unwrap();
    let ndjson = br#"{"id":1,"name":"ada"}
{"id":2,"name":"grace"}
"#;

    for (file_name, compression, bytes) in [
        (
            "orders.ndjson.gz",
            FileCompression::Gzip,
            gzip_bytes(ndjson),
        ),
        (
            "orders.ndjson.zst",
            FileCompression::Zstd,
            zstd_bytes(ndjson),
        ),
    ] {
        let path = temp.path().join(file_name);
        fs::write(&path, bytes).unwrap();
        let source = FileSource::new(
            &path,
            FileFormat::Ndjson(JsonOptions::default()),
            options(file_name, "file"),
        )
        .with_compression(compression);

        let read = read_file_source(&source).unwrap();

        assert_eq!(read.batches[0].header.row_count, 2);
        let Some(SourcePosition::FileManifest(manifest)) = &read.batches[0].header.source_position
        else {
            panic!("compressed NDJSON should preserve file manifest source position");
        };
        assert_eq!(manifest.files.len(), 1);
        assert!(manifest.files[0].path.ends_with(file_name));
        assert_eq!(
            manifest.files[0].size_bytes,
            fs::metadata(&path).unwrap().len()
        );
        assert!(!temp.path().join("orders.ndjson").exists());
    }
}

#[test]
fn parquet_file_source_produces_descriptor_batches_and_file_manifest() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("orders.parquet");
    write_parquet_file(&parquet_path, &[sample_batch()]);

    let source = FileSource::new(
        &parquet_path,
        FileFormat::Parquet,
        options("orders_parquet", "file")
            .with_batch_size(2)
            .unwrap(),
    );
    let read = read_file_source(&source).unwrap();

    assert_eq!(
        read.descriptor
            .schema_source
            .pinned_snapshot()
            .map(|snapshot| &snapshot.schema_hash),
        Some(&read.schema_hash)
    );
    assert!(matches!(read.descriptor.state_scope, ScopeKey::File { .. }));
    assert_eq!(
        read.batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        3
    );
    assert_eq!(read.batches.len(), 2);
    assert_eq!(read.batches[0].header.row_count, 2);
    assert_eq!(read.batches[1].header.row_count, 1);
    assert_eq!(
        read.batches[0].header.batch_id.as_str(),
        "orders_parquet-file-000001"
    );
    assert_eq!(
        read.batches[1].header.batch_id.as_str(),
        "orders_parquet-file-000002"
    );
    assert_eq!(
        read.batches[0].header.observed_schema_hash,
        read.schema_hash
    );

    let first_batch = read.batches[0].record_batch().unwrap();
    assert_eq!(first_batch.num_columns(), 2);
    assert_eq!(first_batch.schema().field(0).name(), "id");
    assert_eq!(first_batch.schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(first_batch.schema().field(1).name(), "name");
    assert_eq!(first_batch.schema().field(1).data_type(), &DataType::Utf8);
    let first_ids = first_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let first_names = first_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!([first_ids.value(0), first_ids.value(1)], [1, 2]);
    assert_eq!(first_names.value(0), "ada");
    assert!(first_names.is_null(1));

    let second_batch = read.batches[1].record_batch().unwrap();
    let second_ids = second_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let second_names = second_batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(second_ids.value(0), 3);
    assert_eq!(second_names.value(0), "grace");

    let SourcePosition::FileManifest(manifest) =
        read.batches[0].header.source_position.as_ref().unwrap()
    else {
        panic!("Parquet file source should set a file manifest position");
    };
    assert_eq!(manifest.files.len(), 1);
    assert_eq!(manifest.files[0].path, parquet_path.to_str().unwrap());
    assert_eq!(
        manifest.files[0].size_bytes,
        fs::metadata(&parquet_path).unwrap().len()
    );
    assert_eq!(manifest.files[0].sha256.as_ref().unwrap().len(), 64);

    assert_file_resource_conformance(&source, &read.schema_hash, 3);
}

#[test]
fn declared_parquet_int32_declared_int64_materializes_lossless_widening() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("events.parquet");
    let physical_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let physical_batch = RecordBatch::try_new(
        physical_schema,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    write_parquet_file(&parquet_path, &[physical_batch]);
    let declared_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let read = read_file_source_with_declared_schema(
        &FileSource::new(
            &parquet_path,
            FileFormat::Parquet,
            options("events", "file"),
        ),
        declared_schema,
    )
    .unwrap();

    let batch = read.batches[0].record_batch().unwrap();
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(
        read.observed_schema.fields[0].arrow_type,
        ArrowType::Int {
            signed: true,
            bits: 64
        }
    );
    assert_eq!(source_name(batch.schema().field(0)), Some("id"));
    assert_eq!(physical_type(batch.schema().field(0)), Some("Int32"));
    assert_eq!(
        read.descriptor
            .schema_source
            .pinned_snapshot()
            .map(|snapshot| &snapshot.schema_hash),
        Some(&read.schema_hash)
    );
    assert_eq!(
        read.batches[0].header.observed_schema_hash,
        read.schema_hash
    );
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!([ids.value(0), ids.value(1), ids.value(2)], [1, 2, 3]);
}

#[test]
fn declared_parquet_float32_declared_float64_materializes_lossless_widening() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("metrics.parquet");
    let physical_schema = Arc::new(Schema::new(vec![Field::new(
        "score",
        DataType::Float32,
        false,
    )]));
    let physical_batch = RecordBatch::try_new(
        physical_schema,
        vec![Arc::new(Float32Array::from(vec![1.5_f32, 2.25_f32]))],
    )
    .unwrap();
    write_parquet_file(&parquet_path, &[physical_batch]);
    let declared_schema = Arc::new(Schema::new(vec![Field::new(
        "score",
        DataType::Float64,
        false,
    )]));

    let read = read_file_source_with_declared_schema(
        &FileSource::new(
            &parquet_path,
            FileFormat::Parquet,
            options("metrics", "file"),
        ),
        declared_schema,
    )
    .unwrap();

    let batch = read.batches[0].record_batch().unwrap();
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Float64);
    assert_eq!(physical_type(batch.schema().field(0)), Some("Float32"));
    let scores = batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!([scores.value(0), scores.value(1)], [1.5, 2.25]);
}

#[test]
fn declared_parquet_projection_renames_by_source_name_and_drops_extra_fields() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("vendors.parquet");
    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("VendorID", DataType::Int32, false),
        Field::new("ignored_physical_column", DataType::Utf8, true),
    ]));
    let physical_batch = RecordBatch::try_new(
        physical_schema,
        vec![
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec![Some("drop"), Some("me")])),
        ],
    )
    .unwrap();
    write_parquet_file(&parquet_path, &[physical_batch]);
    let declared_schema = Arc::new(Schema::new(vec![with_source_name(
        with_semantic(Field::new("vendor_id", DataType::Int32, false), "id"),
        "VendorID",
    )]));

    let read = read_file_source_with_declared_schema(
        &FileSource::new(
            &parquet_path,
            FileFormat::Parquet,
            options("vendors", "file"),
        ),
        declared_schema,
    )
    .unwrap();

    let batch = read.batches[0].record_batch().unwrap();
    assert_eq!(batch.num_columns(), 1);
    assert!(batch.column_by_name("ignored_physical_column").is_none());
    let batch_schema = batch.schema();
    let field = batch_schema.field(0);
    assert_eq!(field.name(), "vendor_id");
    assert_eq!(source_name(field), Some("VendorID"));
    assert_eq!(physical_type(field), Some("Int32"));
    assert_eq!(field.metadata().get("cdf:semantic"), Some(&"id".to_owned()));
    assert_eq!(read.observed_schema.fields.len(), 1);
    assert_eq!(read.observed_schema.fields[0].name, "vendor_id");
    assert_eq!(read.observed_schema.fields[0].source_name, "VendorID");
    let ids = batch
        .column_by_name("vendor_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!([ids.value(0), ids.value(1)], [10, 20]);
}

#[test]
fn declared_parquet_lossy_narrowing_fails_before_batches_are_emitted() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("events.parquet");
    let physical_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let physical_batch = RecordBatch::try_new(
        physical_schema,
        vec![Arc::new(Int64Array::from(vec![1_i64, 2_i64]))],
    )
    .unwrap();
    write_parquet_file(&parquet_path, &[physical_batch]);
    let declared_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

    let error = read_file_source_with_declared_schema(
        &FileSource::new(
            &parquet_path,
            FileFormat::Parquet,
            options("events", "file"),
        ),
        declared_schema,
    )
    .unwrap_err();

    let message = error.to_string();
    assert_eq!(error.kind, ErrorKind::Contract);
    assert!(message.contains("observed type Int64"));
    assert!(message.contains("declared type Int32"));
    assert!(message.contains("enable allow_lossy_mapping"));
}

#[test]
fn undeclared_parquet_read_preserves_physical_schema_after_declared_path_added() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("metrics.parquet");
    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("score", DataType::Float32, false),
    ]));
    let physical_batch = RecordBatch::try_new(
        physical_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Float32Array::from(vec![1.5_f32, 2.25_f32])),
        ],
    )
    .unwrap();
    write_parquet_file(&parquet_path, &[physical_batch]);

    let read = read_file_source(&FileSource::new(
        &parquet_path,
        FileFormat::Parquet,
        options("metrics", "file"),
    ))
    .unwrap();

    let batch = read.batches[0].record_batch().unwrap();
    assert_eq!(batch.schema().as_ref(), physical_schema.as_ref());
    assert_eq!(read.observed_schema.fields.len(), 2);
    assert_eq!(
        read.observed_schema.fields[0].arrow_type,
        ArrowType::Int {
            signed: true,
            bits: 32
        }
    );
    assert_eq!(
        read.observed_schema.fields[1].arrow_type,
        ArrowType::Float { bits: 32 }
    );
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let scores = batch
        .column_by_name("score")
        .unwrap()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!([ids.value(0), ids.value(1)], [1, 2]);
    assert_eq!([scores.value(0), scores.value(1)], [1.5_f32, 2.25_f32]);
}

#[test]
fn local_parquet_schema_discovery_reads_footer_without_batches() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("orders.parquet");
    write_parquet_file(&parquet_path, &[sample_batch()]);

    let discovery = discover_local_parquet_schema(&parquet_path).unwrap();

    assert_eq!(discovery.schema.as_ref(), sample_schema().as_ref());
    assert_eq!(
        discovery.source_identity.size_bytes,
        fs::metadata(&parquet_path).unwrap().len()
    );
    assert_eq!(discovery.source_identity.row_count, 3);
    assert_eq!(discovery.source_identity.row_group_count, 1);
    assert!(
        discovery
            .source_identity
            .footer_sha256
            .starts_with("sha256:")
    );
    assert_eq!(discovery.source_identity.footer_sha256.len(), 71);
}

#[test]
fn local_parquet_schema_discovery_is_repeatable_for_unchanged_file() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("orders.parquet");
    write_parquet_file(&parquet_path, &[sample_batch()]);

    let first = discover_local_parquet_schema(&parquet_path).unwrap();
    let second = discover_local_parquet_schema(&parquet_path).unwrap();

    assert_eq!(first.schema.as_ref(), second.schema.as_ref());
    assert_eq!(first.source_identity, second.source_identity);
}

#[test]
fn local_parquet_schema_discovery_rejects_non_parquet_input() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("bad.parquet");
    fs::write(&parquet_path, b"not parquet").unwrap();

    let error = discover_local_parquet_schema(&parquet_path).unwrap_err();

    assert_eq!(error.kind, ErrorKind::Data);
    assert!(error.message.contains("Parquet metadata discovery"));
}

#[test]
fn malformed_inputs_map_to_data_errors() {
    let error = read_arrow_ipc_stream(Cursor::new(b"not-ipc".as_slice()), &options("bad", "p0"))
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

    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("bad.parquet");
    fs::write(&parquet_path, b"not parquet").unwrap();
    let error = read_file_source(&FileSource::new(
        &parquet_path,
        FileFormat::Parquet,
        options("bad_parquet", "file"),
    ))
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Data);
    assert!(error.message.contains("read Parquet file metadata"));
}

#[test]
fn parquet_source_output_can_be_packaged_and_replayed_like_native_output() {
    let parquet_source = tempfile::tempdir().unwrap();
    let parquet_path = parquet_source.path().join("orders.parquet");
    write_parquet_file(&parquet_path, &[sample_batch()]);
    let read = read_file_source(&FileSource::new(
        &parquet_path,
        FileFormat::Parquet,
        options("orders", "p0"),
    ))
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut builder = cdf_package::PackageBuilder::create(temp.path(), "pkg-formats").unwrap();
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
    let reader = cdf_package::PackageReader::open(temp.path()).unwrap();
    reader.verify().unwrap();
    let replayed = reader
        .read_segment(&SegmentId::new("seg-formats").unwrap())
        .unwrap();

    assert_eq!(manifest.identity.segments[0].row_count, 3);
    assert_eq!(
        replayed[0].schema().as_ref(),
        record_batches(&read)[0].schema().as_ref()
    );
}
