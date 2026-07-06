use super::*;

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::Cursor,
    sync::Arc,
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use firn_contract::{ContractPolicy, NORMALIZER_NAMECASE_V1};
use firn_kernel::{
    ErrorKind, PartitionId, ResourceId, SchemaSource, ScopeKey, SegmentId, SourcePosition,
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
