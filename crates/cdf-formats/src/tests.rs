use super::*;

use std::{
    collections::{BTreeMap, HashMap},
    fs,
    io::{Cursor, Read, Seek, SeekFrom},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow_array::{
    Array, ArrayRef, Decimal128Array, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_ipc::writer::{FileWriter, StreamWriter};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use cdf_conformance::resource::{
    ResourceExecutionConformanceCase, assert_resource_stream_conformance,
    assert_resource_stream_execution_conformance,
};
use cdf_contract::{
    ArrowType, ContractPolicy, FieldCoercionDecision, NORMALIZER_NAMECASE_V1,
    reject_untrusted_schema_coercion_metadata, schema_coercion_plan_from_reconciled_schema,
    schema_coercion_plan_from_trusted_json,
};
use cdf_kernel::{
    ErrorKind, PartitionId, ResourceId, ResourceStream, ScanRequest, SchemaHash, ScopeKey,
    SegmentId, SourcePosition, physical_type, source_name, with_semantic, with_source_name,
};
use cdf_runtime::ReadOptions;
use futures_util::StreamExt;

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
fn arrow_ipc_file_discovery_reads_schema_block_without_decoding_record_batches() {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("Payload", DataType::Utf8, false)],
        HashMap::from([("owner".to_owned(), "source-system".to_owned())]),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec!["x".repeat(1_000_000)]))],
    )
    .unwrap();
    let mut bytes = Cursor::new(Vec::new());
    {
        let mut writer = FileWriter::try_new(&mut bytes, schema.as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let bytes = bytes.into_inner();
    let read_bytes = Arc::new(AtomicUsize::new(0));
    let reader = CountingCursor {
        inner: Cursor::new(bytes.clone()),
        read_bytes: Arc::clone(&read_bytes),
    };

    let discovered = discover_arrow_ipc_file_schema(reader).unwrap();

    assert_eq!(discovered.as_ref(), schema.as_ref());
    assert!(
        read_bytes.load(Ordering::Relaxed) < bytes.len() / 2,
        "schema discovery must not read the record-batch body"
    );
}

#[test]
fn bounded_local_arrow_ipc_discovery_enforces_total_metadata_budget() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("events.arrow");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Utf8,
        false,
    )]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec!["x".repeat(1_000_000)]))],
    )
    .unwrap();
    let mut file = fs::File::create(&path).unwrap();
    let mut writer = FileWriter::try_new(&mut file, schema.as_ref()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let size = fs::metadata(&path).unwrap().len();

    let discovery = discover_local_arrow_ipc_schema_bounded(&path, 8, size / 2).unwrap();
    assert_eq!(discovery.schema.as_ref(), schema.as_ref());
    assert!(discovery.probe_bytes_read < size / 2);
    let error = discover_local_arrow_ipc_schema_bounded(&path, 8, 8)
        .unwrap_err()
        .to_string();
    assert!(error.contains("metadata budget exceeded"));
    assert!(error.contains("allowed 8"));
}

#[test]
fn arrow_ipc_file_discovery_rejects_stream_framing_explicitly() {
    let input = sample_batch();
    let mut bytes = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut bytes, input.schema().as_ref()).unwrap();
        writer.write(&input).unwrap();
        writer.finish().unwrap();
    }

    let error = discover_arrow_ipc_file_schema(Cursor::new(bytes)).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("expected Arrow IPC file framing")
    );
    assert!(error.to_string().contains("stream framing is unsupported"));
}

struct CountingCursor {
    inner: Cursor<Vec<u8>>,
    read_bytes: Arc<AtomicUsize>,
}

impl Read for CountingCursor {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let count = self.inner.read(buffer)?;
        self.read_bytes.fetch_add(count, Ordering::Relaxed);
        Ok(count)
    }
}

impl Seek for CountingCursor {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(position)
    }
}

#[test]
fn parquet_and_arrow_carried_coercion_metadata_is_not_trusted_as_internal_evidence() {
    let source_plan = serde_json::json!({
        "fields": [{
            "source_name": "id",
            "observed_name": "id",
            "output_name": "id",
            "observed_type": "Int64",
            "constraint_type": "Int64",
            "decision": "preserved",
            "outcome": "pass",
            "reason": "observed type already satisfies the constraint"
        }]
    })
    .to_string();
    let schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new("id", DataType::Int64, false)],
        HashMap::from([("cdf:schema_coercion_plan".to_owned(), source_plan)]),
    ));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();

    let mut ipc = Cursor::new(Vec::new());
    {
        let mut writer = FileWriter::try_new(&mut ipc, schema.as_ref()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let ipc_read =
        read_arrow_ipc_file(Cursor::new(ipc.into_inner()), &options("events", "ipc")).unwrap();
    assert!(ipc_read.batches[0].header.schema_coercion_plan.is_none());
    let ipc_error = reject_untrusted_schema_coercion_metadata(
        ipc_read.batches[0]
            .record_batch()
            .unwrap()
            .schema()
            .as_ref(),
    )
    .unwrap_err();
    assert!(
        ipc_error
            .to_string()
            .contains("without trusted batch evidence")
    );

    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("injected.parquet");
    write_parquet_file(&parquet_path, &[batch]);
    let parquet_read = read_file_source(&FileSource::new(
        parquet_path,
        FileFormat::Parquet,
        options("events", "parquet"),
    ))
    .unwrap();
    assert!(
        parquet_read.batches[0]
            .header
            .schema_coercion_plan
            .is_none()
    );
    let parquet_schema = parquet_read.batches[0].record_batch().unwrap().schema();
    assert!(
        !parquet_schema
            .metadata()
            .contains_key("cdf:schema_coercion_plan")
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
fn declared_ndjson_scalar_type_mismatch_preserves_neutral_residual_candidate() {
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
    assert_eq!(batch.num_rows(), 3);
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
    assert_eq!([ids.value(0), ids.value(1), ids.value(2)], [1, 2, 3]);
    assert_eq!(event_types.value(0), "order.created");
    assert!(event_types.is_null(1));
    assert_eq!(event_types.value(2), "order.shipped");
    assert_eq!(physical_type(batch.schema().field(1)), Some("Utf8"));
    let coercion = schema_coercion_plan_from_reconciled_schema(batch.schema().as_ref())
        .unwrap()
        .unwrap();
    assert_eq!(
        coercion.fields[1].decision,
        FieldCoercionDecision::Preserved
    );

    assert!(read.batches[0].header.pre_contract_quarantine.is_empty());
    let candidates = read.batches[0].header.residual_candidates();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].source_row_ordinal(), 1);
    assert_eq!(candidates[0].source_path(), &["event_type".to_owned()]);
}

#[test]
fn declared_ndjson_source_name_override_scopes_residual_to_source_field() {
    let schema = Arc::new(Schema::new(vec![with_source_name(
        Field::new("event_type", DataType::Utf8, false),
        "Event Type",
    )]));
    let read = read_ndjson_bytes_with_declared_schema(
        b"{\"Event Type\":\"created\"}\n{\"Event Type\":42}\n{\"Event Type\":\"shipped\"}\n",
        &options("events", "p0"),
        &JsonOptions::default(),
        schema,
    )
    .unwrap();

    let batch = read.batches[0].record_batch().unwrap();
    let event_types = batch
        .column_by_name("event_type")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(event_types.value(0), "created");
    assert!(event_types.is_null(1));
    assert_eq!(event_types.value(2), "shipped");
    assert_eq!(source_name(batch.schema().field(0)), Some("Event Type"));

    let candidates = read.batches[0].header.residual_candidates();
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].source_row_ordinal(), 1);
    assert_eq!(candidates[0].source_path(), &["Event Type".to_owned()]);
}

#[test]
fn declared_ndjson_all_rows_mismatch_and_unknown_variance_decode_to_residual_candidates() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Int64,
        true,
    )]));
    let read = read_ndjson_bytes_with_declared_schema(
        br#"{"amount":"unknown","extra":1}
{"amount":"still-unknown","extra":{"nested":true}}
{"amount":"again","extra":null}
"#,
        &options("events", "p0"),
        &JsonOptions::default(),
        schema,
    )
    .unwrap();

    let batch = read.batches[0].record_batch().unwrap();
    assert_eq!(batch.num_rows(), 3);
    let amounts = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(amounts.null_count(), 3);
    let candidates = read.batches[0].header.residual_candidates();
    assert_eq!(candidates.len(), 6);
    assert_eq!(
        candidates
            .iter()
            .filter(|candidate| candidate.source_path() == ["extra".to_owned()])
            .count(),
        3
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
fn declared_ndjson_type_mismatch_defers_pii_value_without_debug_leak() {
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

    let candidates = read.batches[0].header.residual_candidates();
    assert_eq!(candidates.len(), 1);
    assert!(!format!("{candidates:?}").contains("alice@example.com"));
}

#[test]
fn declared_ndjson_observes_then_materializes_lossless_integer_to_decimal_widening() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(20, 0),
        false,
    )]));

    let read = read_ndjson_bytes_with_declared_schema(
        b"{\"amount\":9007199254740991}\n{\"amount\":-42}\n",
        &options("events", "p0"),
        &JsonOptions::default(),
        schema,
    )
    .unwrap();

    let batch = read.batches[0].record_batch().unwrap();
    let field = batch.schema().field(0).clone();
    assert_eq!(field.data_type(), &DataType::Decimal128(20, 0));
    assert_eq!(physical_type(&field), Some("Int64"));
    let amounts = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amounts.values(), &[9_007_199_254_740_991_i128, -42_i128]);

    let plan = schema_coercion_plan_from_reconciled_schema(batch.schema().as_ref())
        .unwrap()
        .unwrap();
    assert_eq!(plan.fields.len(), 1);
    assert_eq!(plan.fields[0].decision, FieldCoercionDecision::Widened);
    assert_eq!(plan.fields[0].observed_type.as_deref(), Some("Int64"));
    assert_eq!(
        plan.fields[0].constraint_type.as_deref(),
        Some("Decimal128(20, 0)")
    );
}

#[test]
fn declared_ndjson_projects_by_source_name_and_records_extra_observed_fields() {
    let schema = Arc::new(Schema::new(vec![with_source_name(
        Field::new("vendor_id", DataType::Int64, false),
        "VendorID",
    )]));

    let read = read_ndjson_bytes_with_declared_schema(
        b"{\"VendorID\":1,\"ignored\":\"first\"}\n{\"VendorID\":2,\"ignored\":\"second\"}\n",
        &options("events", "p0"),
        &JsonOptions::default(),
        schema,
    )
    .unwrap();

    let batch = read.batches[0].record_batch().unwrap();
    assert_eq!(batch.num_columns(), 1);
    assert!(batch.column_by_name("ignored").is_none());
    assert_eq!(source_name(batch.schema().field(0)), Some("VendorID"));
    assert_eq!(physical_type(batch.schema().field(0)), Some("Int64"));
    let vendors = batch
        .column_by_name("vendor_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(vendors.values(), &[1, 2]);

    let plan = schema_coercion_plan_from_reconciled_schema(batch.schema().as_ref())
        .unwrap()
        .unwrap();
    assert_eq!(plan.fields.len(), 1);
    assert_eq!(plan.fields[0].source_name, "VendorID");
    assert_eq!(plan.fields[0].decision, FieldCoercionDecision::Preserved);
    assert_eq!(read.batches[0].header.residual_candidates().len(), 2);
}

#[test]
fn declared_ndjson_parse_coercion_is_policy_gated_and_materialized_when_allowed() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "observed_at",
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        false,
    )]));
    let bytes = b"{\"observed_at\":\"2026-07-09T12:34:56.123456\"}\n";

    let error = read_ndjson_bytes_with_declared_schema(
        bytes,
        &options("events", "p0"),
        &JsonOptions::default(),
        schema.clone(),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("field \"observed_at\""));
    assert!(message.contains("observed type Utf8"));
    assert!(message.contains("declared type Timestamp"), "{message}");
    assert!(message.contains("change the declaration to Utf8"));
    assert!(message.contains("enable coerce_types"));

    let mut type_policy = ContractPolicy::default().types;
    type_policy.coerce_types = true;
    let read = read_ndjson_bytes_with_declared_schema_and_type_policy(
        bytes,
        &options("events", "p0"),
        &JsonOptions::default(),
        schema,
        &type_policy,
    )
    .unwrap();
    let batch = read.batches[0].record_batch().unwrap();
    assert_eq!(physical_type(batch.schema().field(0)), Some("Utf8"));
    assert!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .is_some()
    );
    let plan = schema_coercion_plan_from_reconciled_schema(batch.schema().as_ref())
        .unwrap()
        .unwrap();
    assert_eq!(
        plan.fields[0].decision,
        FieldCoercionDecision::CoercedByPolicy
    );
    assert_eq!(
        schema_coercion_plan_from_trusted_json(
            batch.schema().as_ref(),
            read.batches[0]
                .header
                .schema_coercion_plan
                .as_deref()
                .unwrap(),
        )
        .unwrap(),
        plan
    );
}

#[test]
fn declared_ndjson_lossy_numeric_mapping_is_policy_gated_and_recorded_exactly() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let bytes = b"{\"id\":1}\n{\"id\":2}\n";

    let error = read_ndjson_bytes_with_declared_schema(
        bytes,
        &options("events", "p0"),
        &JsonOptions::default(),
        schema.clone(),
    )
    .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("observed type Int64"));
    assert!(message.contains("declared type Int32"));
    assert!(message.contains("widen or change the declaration to Int64"));
    assert!(message.contains("enable allow_lossy_mapping"));

    let mut type_policy = ContractPolicy::default().types;
    type_policy.coerce_types = false;
    type_policy.allow_lossy_mapping = true;
    let read = read_ndjson_bytes_with_declared_schema_and_type_policy(
        bytes,
        &options("events", "p0"),
        &JsonOptions::default(),
        schema,
        &type_policy,
    )
    .unwrap();
    let batch = read.batches[0].record_batch().unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2]);
    let plan = schema_coercion_plan_from_reconciled_schema(batch.schema().as_ref())
        .unwrap()
        .unwrap();
    assert_eq!(plan.fields[0].decision, FieldCoercionDecision::LossyAllowed);
}

#[test]
fn declared_ndjson_string_decimal_parse_is_policy_enabled_and_materialized() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(10, 2),
        false,
    )]));
    let mut type_policy = ContractPolicy::default().types;
    type_policy.coerce_types = true;

    let read = read_ndjson_bytes_with_declared_schema_and_type_policy(
        b"{\"amount\":\"12.34\"}\n{\"amount\":\"-0.50\"}\n",
        &options("events", "p0"),
        &JsonOptions::default(),
        schema,
        &type_policy,
    )
    .unwrap();

    let batch = read.batches[0].record_batch().unwrap();
    let amounts = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();
    assert_eq!(amounts.values(), &[1_234_i128, -50_i128]);
    assert!(read.batches[0].header.pre_contract_quarantine.is_empty());
    let plan = schema_coercion_plan_from_reconciled_schema(batch.schema().as_ref())
        .unwrap()
        .unwrap();
    assert_eq!(
        plan.fields[0].decision,
        FieldCoercionDecision::CoercedByPolicy
    );
}

#[test]
fn declared_ndjson_string_decimal_without_policy_is_residual_candidate() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(10, 2),
        false,
    )]));

    let read = read_ndjson_bytes_with_declared_schema(
        b"{\"amount\":\"12.34\"}\n",
        &options("events", "p0"),
        &JsonOptions::default(),
        schema,
    )
    .unwrap();

    let batch = read.batches[0].record_batch().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(batch.column(0).is_null(0));
    assert_eq!(read.batches[0].header.residual_candidates().len(), 1);
}

#[test]
fn declared_ndjson_fractional_drift_in_integer_field_is_residual_candidate() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    let read = read_ndjson_bytes_with_declared_schema(
        b"{\"id\":1}\n{\"id\":2.5}\n{\"id\":3}\n",
        &options("events", "p0"),
        &JsonOptions::default(),
        schema,
    )
    .unwrap();

    let batch = read.batches[0].record_batch().unwrap();
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.len(), 3);
    assert_eq!(ids.value(0), 1);
    assert!(ids.is_null(1));
    assert_eq!(ids.value(2), 3);
    assert_eq!(read.batches[0].header.residual_candidates().len(), 1);
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
fn declared_json_document_and_ndjson_share_observation_reconciliation_front_end() {
    let temp = tempfile::tempdir().unwrap();
    let json_path = temp.path().join("events.json");
    let ndjson_path = temp.path().join("events.ndjson");
    fs::write(
        &json_path,
        r#"[{"VendorID":1,"ignored":"a"},{"VendorID":2,"ignored":"b"}]"#,
    )
    .unwrap();
    fs::write(
        &ndjson_path,
        "{\"VendorID\":1,\"ignored\":\"a\"}\n{\"VendorID\":2,\"ignored\":\"b\"}\n",
    )
    .unwrap();
    let declared = Arc::new(Schema::new(vec![with_source_name(
        Field::new("vendor_id", DataType::Int64, false),
        "VendorID",
    )]));

    let json = read_file_source_with_declared_schema(
        &FileSource::new(
            json_path,
            FileFormat::Json(JsonOptions::default()),
            options("events", "json"),
        ),
        declared.clone(),
    )
    .unwrap();
    let ndjson = read_file_source_with_declared_schema(
        &FileSource::new(
            ndjson_path,
            FileFormat::Ndjson(JsonOptions::default()),
            options("events", "ndjson"),
        ),
        declared,
    )
    .unwrap();

    let json_batch = json.batches[0].record_batch().unwrap();
    let ndjson_batch = ndjson.batches[0].record_batch().unwrap();
    assert_eq!(json_batch, ndjson_batch);
    assert_eq!(json.schema_hash, ndjson.schema_hash);
    assert_eq!(
        json.batches[0].header.schema_coercion_plan,
        ndjson.batches[0].header.schema_coercion_plan
    );
    assert_eq!(json.batches[0].header.residual_candidates().len(), 2);
    assert_eq!(ndjson.batches[0].header.residual_candidates().len(), 2);
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
    let physical_schema_hash = schema_hash(physical_schema.as_ref()).unwrap();
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
        physical_schema_hash
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
fn declared_parquet_stream_yields_reconciled_batches_incrementally() {
    let temp = tempfile::tempdir().unwrap();
    let parquet_path = temp.path().join("stream.parquet");
    let physical_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let physical_batch = RecordBatch::try_new(
        physical_schema,
        vec![Arc::new(Int32Array::from_iter_values(0..10))],
    )
    .unwrap();
    write_parquet_file(&parquet_path, &[physical_batch]);
    let declared_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let options = options("stream", "file").with_batch_size(3).unwrap();

    let mut stream = stream_parquet_file_with_declared_schema_and_type_policy(
        &parquet_path,
        &options,
        declared_schema,
        &ContractPolicy::default().types,
        None,
    )
    .unwrap();
    let first = futures_executor::block_on(stream.next()).unwrap().unwrap();
    let first = first.record_batch().unwrap();
    assert_eq!(first.num_rows(), 3);
    assert_eq!(first.schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(physical_type(first.schema().field(0)), Some("Int32"));
    let remaining_rows = futures_executor::block_on(async move {
        let mut rows = 0;
        while let Some(batch) = stream.next().await {
            rows += batch.unwrap().header.row_count;
        }
        rows
    });
    assert_eq!(remaining_rows, 7);
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
fn declared_parquet_projection_preserves_extra_fields_as_residual_candidates() {
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
    let candidates = read.batches[0].header.residual_candidates();
    assert_eq!(candidates.len(), 2);
    assert!(
        candidates
            .iter()
            .all(|candidate| { candidate.source_path() == ["ignored_physical_column".to_owned()] })
    );
}

#[test]
fn declared_arrow_ipc_projection_preserves_extra_fields_as_residual_candidates() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("events.arrow");
    let physical_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("nested_extra", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&physical_schema),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])),
            Arc::new(StringArray::from(vec![Some("one"), Some("two")])),
        ],
    )
    .unwrap();
    let mut file = fs::File::create(&path).unwrap();
    let mut writer = FileWriter::try_new(&mut file, physical_schema.as_ref()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    drop(file);

    let read = read_arrow_ipc_file_path_with_declared_schema(
        &path,
        &options("events", "file"),
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
    )
    .unwrap();
    let output = read.batches[0].record_batch().unwrap();
    assert_eq!(output.num_columns(), 1);
    let candidates = read.batches[0].header.residual_candidates();
    assert_eq!(candidates.len(), 2);
    assert!(
        candidates
            .iter()
            .all(|candidate| candidate.source_path() == ["nested_extra".to_owned()])
    );
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
    let builder = cdf_package::PackageBuilder::create(temp.path(), "pkg-formats").unwrap();
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
