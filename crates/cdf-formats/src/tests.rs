use super::*;

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::Cursor,
    path::Path,
    sync::Arc,
};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_conformance::resource::{
    ResourceExecutionConformanceCase, assert_resource_stream_conformance,
    assert_resource_stream_execution_conformance,
};
use cdf_contract::{ContractPolicy, NORMALIZER_NAMECASE_V1};
use cdf_kernel::{
    ErrorKind, PartitionId, ResourceId, ResourceStream, ScanRequest, SchemaHash, SchemaSource,
    ScopeKey, SegmentId, SourcePosition, with_source_name,
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
        read.descriptor.schema_source,
        SchemaSource::Discovered {
            schema_hash: Some(read.schema_hash.clone())
        }
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
