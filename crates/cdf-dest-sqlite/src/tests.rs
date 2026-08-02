use std::sync::Arc;

use arrow_array::{
    ArrayRef, Float16Array, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit, UnionFields, UnionMode};
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CdfError, CheckpointId, CommitSegment, CommitSession, CursorPosition,
    CursorValue, DestinationProtocol, ErrorKind, IdempotencyToken, PackageHash, PipelineId,
    ResourceId, Result, ScanPlan, SchemaHash, ScopeKey, SegmentId, SourcePosition, StateDelta,
    StateSegment, TypeMappingFidelity, WriteDisposition,
};
use cdf_package_contract::{
    PackageReplayInputs, QuarantineObservedValue, QuarantineRecord, SegmentEntry,
    VerifiedPackageAccess,
};
use rusqlite::Connection;

use crate::{
    identifier::SqliteIdentifier,
    mapping::{columns_for_schema, sqlite_type_for_arrow, sqlite_value},
    models::{
        SqliteCommitRequest, SqliteExpectedSegment, SqliteLoadPlan, SqliteLoadPlanInput,
        SqliteSessionSegments,
    },
    plan::plan_sqlite_load,
    sheet::sqlite_destination_sheet,
    transaction::{SqliteCommitSession, install_progress_handler, verify_receipt},
};

#[test]
fn sheet_type_mappings_exactly_match_the_physical_scalar_mapper() {
    let supported = vec![
        (DataType::Boolean, "INTEGER"),
        (DataType::Int8, "INTEGER"),
        (DataType::Int16, "INTEGER"),
        (DataType::Int32, "INTEGER"),
        (DataType::Int64, "INTEGER"),
        (DataType::UInt8, "INTEGER"),
        (DataType::UInt16, "INTEGER"),
        (DataType::UInt32, "INTEGER"),
        (DataType::UInt64, "TEXT"),
        (DataType::Decimal32(9, 2), "TEXT"),
        (DataType::Decimal64(18, -2), "TEXT"),
        (DataType::Decimal128(38, 9), "TEXT"),
        (DataType::Decimal256(76, 18), "TEXT"),
        (DataType::Float16, "BLOB"),
        (DataType::Float32, "BLOB"),
        (DataType::Float64, "BLOB"),
        (DataType::Utf8, "TEXT"),
        (DataType::LargeUtf8, "TEXT"),
        (DataType::Utf8View, "TEXT"),
        (DataType::Binary, "BLOB"),
        (DataType::LargeBinary, "BLOB"),
        (DataType::BinaryView, "BLOB"),
        (DataType::FixedSizeBinary(16), "BLOB"),
        (DataType::Date32, "INTEGER"),
        (DataType::Date64, "INTEGER"),
        (DataType::Time32(TimeUnit::Second), "INTEGER"),
        (DataType::Time32(TimeUnit::Millisecond), "INTEGER"),
        (DataType::Time64(TimeUnit::Microsecond), "INTEGER"),
        (DataType::Time64(TimeUnit::Nanosecond), "INTEGER"),
        (DataType::Timestamp(TimeUnit::Second, None), "INTEGER"),
        (
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            "INTEGER",
        ),
        (DataType::Timestamp(TimeUnit::Microsecond, None), "INTEGER"),
        (
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            "INTEGER",
        ),
        (DataType::Duration(TimeUnit::Second), "INTEGER"),
        (DataType::Duration(TimeUnit::Millisecond), "INTEGER"),
        (DataType::Duration(TimeUnit::Microsecond), "INTEGER"),
        (DataType::Duration(TimeUnit::Nanosecond), "INTEGER"),
        (
            DataType::Interval(arrow_schema::IntervalUnit::YearMonth),
            "INTEGER",
        ),
        (
            DataType::Interval(arrow_schema::IntervalUnit::DayTime),
            "INTEGER",
        ),
        (
            DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            "TEXT",
        ),
    ];
    let item = Arc::new(Field::new("item", DataType::Int64, true));
    let map_entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ]
            .into(),
        ),
        false,
    ));
    let unsupported = vec![
        DataType::Null,
        DataType::Struct(vec![Field::new("value", DataType::Int64, true)].into()),
        DataType::List(Arc::clone(&item)),
        DataType::LargeList(Arc::clone(&item)),
        DataType::FixedSizeList(Arc::clone(&item), 2),
        DataType::ListView(Arc::clone(&item)),
        DataType::LargeListView(item),
        DataType::Map(map_entries, false),
        DataType::Union(
            UnionFields::try_new(
                vec![0],
                vec![Arc::new(Field::new("value", DataType::Int64, true))],
            )
            .unwrap(),
            UnionMode::Sparse,
        ),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        ),
    ];
    let sheet = sqlite_destination_sheet().unwrap();
    let declared_patterns = sheet
        .type_mappings
        .iter()
        .map(|mapping| mapping.arrow_type.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        declared_patterns,
        [
            "Null",
            "Boolean",
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "Decimal32(p,s)",
            "Decimal64(p,s)",
            "Decimal128(p,s)",
            "Decimal256(p,s)",
            "Float16",
            "Float32",
            "Float64",
            "Utf8",
            "LargeUtf8",
            "Utf8View",
            "Binary",
            "LargeBinary",
            "BinaryView",
            "FixedSizeBinary(*)",
            "Date32",
            "Date64",
            "Time32(second|millisecond)",
            "Time64(Microsecond)",
            "Time64(Nanosecond)",
            "Timestamp(*,*)",
            "Duration",
            "Interval(YearMonth|DayTime)",
            "Interval(MonthDayNano)",
            "Struct",
            "List*",
            "Map",
            "Union",
            "Dictionary",
            "RunEndEncoded",
        ]
    );
    for (index, (data_type, sqlite_type)) in supported.iter().enumerate() {
        assert_eq!(sqlite_type_for_arrow(data_type).unwrap(), *sqlite_type);
        let mapping =
            cdf_contract::resolve_destination_type_mapping(&sheet.type_mappings, data_type)
                .unwrap()
                .unwrap_or_else(|| panic!("sheet has no mapping for supported {data_type}"));
        assert_eq!(mapping.destination_type, *sqlite_type);
        assert_eq!(mapping.fidelity, TypeMappingFidelity::Lossless);
        for nullable in [false, true] {
            let schema = Schema::new(vec![Field::new(
                format!("field_{index}"),
                data_type.clone(),
                nullable,
            )]);
            let columns = columns_for_schema(&schema).unwrap();
            assert_eq!(columns[0].sqlite_type, *sqlite_type);
            assert_eq!(columns[0].nullable, nullable);
        }
    }
    for data_type in unsupported {
        assert!(sqlite_type_for_arrow(&data_type).is_err());
        let mapping =
            cdf_contract::resolve_destination_type_mapping(&sheet.type_mappings, &data_type)
                .unwrap()
                .unwrap_or_else(|| panic!("sheet has no unsupported mapping for {data_type}"));
        assert_eq!(mapping.fidelity, TypeMappingFidelity::Unsupported);
    }
    let destination = crate::SqliteDestination::connect("<sheet-parity>").unwrap();
    assert_eq!(
        cdf_runtime::artifact_hash(&destination.sheet_artifact().unwrap()).unwrap(),
        "sha256:752ca09c24e025ce0382d99760be03336b914b1df50cbd7230b203fd8e930fb7"
    );
}

#[test]
fn floats_preserve_exact_ieee_bits_in_canonical_big_endian_blobs() {
    let float16 = Float16Array::from(vec![half::f16::from_bits(0x7e55)]);
    let float32 = Float32Array::from(vec![
        f32::from_bits(0x8000_0000),
        f32::from_bits(0x7fc0_1234),
    ]);
    let float64 = Float64Array::from(vec![
        f64::from_bits(0x8000_0000_0000_0000),
        f64::from_bits(0x7ff8_0000_0000_1234),
    ]);
    assert_eq!(
        sqlite_value(&float16, &DataType::Float16, 0).unwrap(),
        rusqlite::types::Value::Blob(0x7e55_u16.to_be_bytes().to_vec())
    );
    for (row, bits) in [0x8000_0000_u32, 0x7fc0_1234].into_iter().enumerate() {
        assert_eq!(
            sqlite_value(&float32, &DataType::Float32, row).unwrap(),
            rusqlite::types::Value::Blob(bits.to_be_bytes().to_vec())
        );
    }
    for (row, bits) in [0x8000_0000_0000_0000_u64, 0x7ff8_0000_0000_1234]
        .into_iter()
        .enumerate()
    {
        assert_eq!(
            sqlite_value(&float64, &DataType::Float64, row).unwrap(),
            rusqlite::types::Value::Blob(bits.to_be_bytes().to_vec())
        );
    }
}

struct TestPackage {
    hash: String,
    entries: Vec<SegmentEntry>,
    schema: SchemaRef,
    quarantines: Vec<QuarantineRecord>,
}

impl VerifiedPackageAccess for TestPackage {
    fn package_hash(&self) -> &str {
        &self.hash
    }

    fn for_each_identity_segment(
        &self,
        visitor: &mut dyn FnMut(SegmentEntry) -> Result<()>,
    ) -> Result<()> {
        for entry in &self.entries {
            visitor(entry.clone())?;
        }
        Ok(())
    }

    fn recorded_scan_plan(&self) -> Result<ScanPlan> {
        Err(CdfError::internal("unused test scan plan"))
    }

    fn replay_inputs(&self) -> Result<PackageReplayInputs> {
        Err(CdfError::internal("unused test replay inputs"))
    }

    fn runtime_arrow_schema(&self) -> Result<SchemaRef> {
        Ok(Arc::clone(&self.schema))
    }

    fn for_each_quarantine_record(
        &self,
        visitor: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
    ) -> Result<()> {
        for record in &self.quarantines {
            visitor(record.clone())?;
        }
        Ok(())
    }
}

fn logical_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)),
        Arc::new(StringArray::from(names)),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

fn state_segment(rows: u64) -> StateSegment {
    StateSegment {
        segment_id: SegmentId::new("segment-1").unwrap(),
        scope: ScopeKey::Resource,
        output_position: SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "id".to_owned(),
            value: CursorValue::I64(i64::try_from(rows).unwrap()),
        }),
        row_count: rows,
        byte_count: rows * 8,
    }
}

fn test_plan(
    hash: &str,
    disposition: WriteDisposition,
    logical_schema: &Schema,
    merge_keys: &[&str],
    rows: u64,
) -> SqliteLoadPlan {
    let package_hash = PackageHash::new(hash).unwrap();
    let schema_hash = SchemaHash::new(format!("schema-{hash}")).unwrap();
    let suffix = hash.replace([':', '-'], "_");
    let segment = state_segment(rows);
    let state_delta = StateDelta {
        checkpoint_id: CheckpointId::new(format!("checkpoint-{suffix}")).unwrap(),
        pipeline_id: PipelineId::new(format!("pipeline-{suffix}")).unwrap(),
        resource_id: ResourceId::new(format!("resource-{suffix}")).unwrap(),
        scope: ScopeKey::Resource,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: segment.output_position.clone(),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        package_hash: package_hash.clone(),
        schema_hash: schema_hash.clone(),
        segments: vec![segment.clone()],
    };
    plan_sqlite_load(SqliteLoadPlanInput {
        package_hash: package_hash.clone(),
        idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
        target: SqliteIdentifier::user("events").unwrap(),
        disposition,
        schema_hash,
        segments: vec![segment],
        columns: columns_for_schema(logical_schema).unwrap(),
        merge_keys: merge_keys
            .iter()
            .map(|key| SqliteIdentifier::user(key).unwrap())
            .collect(),
        resource_id: Some(state_delta.resource_id.clone()),
        state_delta: Some(state_delta),
    })
    .unwrap()
}

#[test]
fn only_exact_governed_variant_field_enters_the_sqlite_system_namespace() {
    let variant = cdf_kernel::with_semantic(
        Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true),
        cdf_contract::VARIANT_SEMANTIC_TAG,
    );
    let mut exact_metadata = variant.metadata().clone();
    exact_metadata.insert(
        cdf_contract::RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
        cdf_contract::RESIDUAL_ENCODING_NAME.to_owned(),
    );
    let variant = variant.with_metadata(exact_metadata.clone());
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false), variant]);
    let columns = columns_for_schema(&schema).unwrap();
    assert!(!columns[0].framework_owned);
    assert_eq!(columns[1].name.as_str(), cdf_contract::VARIANT_COLUMN_NAME);
    assert_eq!(columns[1].sqlite_type, "TEXT");
    assert!(columns[1].nullable);
    assert!(columns[1].framework_owned);
    let plan = test_plan(
        "sha256:governed-variant",
        WriteDisposition::Append,
        &schema,
        &[],
        0,
    );
    assert_eq!(
        plan.columns[1].name.as_str(),
        cdf_contract::VARIANT_COLUMN_NAME
    );
    assert!(plan.columns[1].framework_owned);

    let impostors = [
        Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true),
        cdf_kernel::with_semantic(
            Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true),
            cdf_contract::VARIANT_SEMANTIC_TAG,
        ),
        Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([
                (
                    cdf_kernel::SEMANTIC_METADATA_KEY.to_owned(),
                    "wrong".to_owned(),
                ),
                (
                    cdf_contract::RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
                    cdf_contract::RESIDUAL_ENCODING_NAME.to_owned(),
                ),
            ]),
        ),
        cdf_kernel::with_semantic(
            Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Int64, true),
            cdf_contract::VARIANT_SEMANTIC_TAG,
        )
        .with_metadata(exact_metadata.clone()),
        cdf_kernel::with_semantic(
            Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, false),
            cdf_contract::VARIANT_SEMANTIC_TAG,
        )
        .with_metadata(exact_metadata.clone()),
        cdf_kernel::with_semantic(
            Field::new(cdf_contract::VARIANT_COLUMN_NAME, DataType::Utf8, true),
            cdf_contract::VARIANT_SEMANTIC_TAG,
        )
        .with_metadata(std::collections::HashMap::from([
            (
                cdf_kernel::SEMANTIC_METADATA_KEY.to_owned(),
                cdf_contract::VARIANT_SEMANTIC_TAG.to_owned(),
            ),
            (
                cdf_contract::RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
                "wrong".to_owned(),
            ),
        ])),
        cdf_kernel::with_semantic(
            Field::new("_cdf_other", DataType::Utf8, true),
            cdf_contract::VARIANT_SEMANTIC_TAG,
        )
        .with_metadata(exact_metadata),
    ];
    for impostor in impostors {
        let error = columns_for_schema(&Schema::new(vec![impostor])).unwrap_err();
        assert!(error.to_string().contains("reserved _cdf_ prefix"));
    }
}

fn start_session(
    path: &std::path::Path,
    hash: &str,
    disposition: WriteDisposition,
    merge_keys: &[&str],
    batch: RecordBatch,
) -> (SqliteCommitSession, StateSegment, RecordBatch) {
    start_session_with_quarantines(path, hash, disposition, merge_keys, batch, Vec::new())
}

fn start_session_with_quarantines(
    path: &std::path::Path,
    hash: &str,
    disposition: WriteDisposition,
    merge_keys: &[&str],
    batch: RecordBatch,
    quarantines: Vec<QuarantineRecord>,
) -> (SqliteCommitSession, StateSegment, RecordBatch) {
    let (_, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    start_session_with_execution(
        path,
        hash,
        disposition,
        merge_keys,
        batch,
        quarantines,
        execution,
    )
}

fn start_session_with_execution(
    path: &std::path::Path,
    hash: &str,
    disposition: WriteDisposition,
    merge_keys: &[&str],
    batch: RecordBatch,
    quarantines: Vec<QuarantineRecord>,
    execution: cdf_runtime::ExecutionServices,
) -> (SqliteCommitSession, StateSegment, RecordBatch) {
    let rows = batch.num_rows() as u64;
    let schema = batch.schema();
    let canonical = cdf_package_contract::append_package_row_ord(vec![batch], 0)
        .unwrap()
        .remove(0);
    let plan = test_plan(hash, disposition, schema.as_ref(), merge_keys, rows);
    let state = state_segment(rows);
    let expected = SqliteExpectedSegment {
        state: state.clone(),
        package_byte_count: rows * 16,
        package_row_ord_start: 0,
    };
    let package = Arc::new(TestPackage {
        hash: hash.to_owned(),
        entries: vec![SegmentEntry {
            segment_id: state.segment_id.clone(),
            path: "data/segment-1.arrow".to_owned(),
            package_row_ord_start: 0,
            row_count: rows,
            byte_count: rows * 16,
            sha256: "0".repeat(64),
        }],
        schema,
        quarantines,
    });
    let session = SqliteCommitSession::new(
        path.to_path_buf(),
        execution,
        SqliteCommitRequest {
            package,
            plan,
            segments: SqliteSessionSegments {
                expected: [(state.segment_id.clone(), expected)].into_iter().collect(),
            },
        },
    );
    (session, state, canonical)
}

fn try_commit(
    path: &std::path::Path,
    hash: &str,
    disposition: WriteDisposition,
    merge_keys: &[&str],
    batch: RecordBatch,
) -> Result<cdf_kernel::Receipt> {
    let rows = batch.num_rows() as u64;
    let (mut session, state, canonical) = start_session(path, hash, disposition, merge_keys, batch);
    session.apply_migrations()?;
    session.write_segments(Box::new(std::iter::once(Ok(CommitSegment::new(
        state,
        rows * 16,
        vec![canonical],
    )))))?;
    Box::new(session).finalize()
}

fn commit(
    path: &std::path::Path,
    hash: &str,
    disposition: WriteDisposition,
    merge_keys: &[&str],
    batch: RecordBatch,
) -> cdf_kernel::Receipt {
    try_commit(path, hash, disposition, merge_keys, batch).unwrap()
}

fn commit_empty(
    path: &std::path::Path,
    hash: &str,
    disposition: WriteDisposition,
) -> cdf_kernel::Receipt {
    let schema = logical_batch(Vec::new(), Vec::new()).schema();
    let mut plan = test_plan(hash, disposition, schema.as_ref(), &[], 0);
    plan.segments.clear();
    if let Some(delta) = &mut plan.state_delta {
        delta.segments.clear();
    }
    let package = Arc::new(TestPackage {
        hash: hash.to_owned(),
        entries: Vec::new(),
        schema,
        quarantines: Vec::new(),
    });
    let (_, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let mut session = SqliteCommitSession::new(
        path.to_path_buf(),
        execution,
        SqliteCommitRequest {
            package,
            plan,
            segments: SqliteSessionSegments {
                expected: Default::default(),
            },
        },
    );
    session.apply_migrations().unwrap();
    Box::new(session).finalize().unwrap()
}

#[test]
fn append_duplicate_and_fresh_verification_preserve_one_logical_receipt() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("append.sqlite");
    let first = commit(
        &path,
        "sha256:append",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1, 2], vec!["one", "two"]),
    );
    let duplicate = commit(
        &path,
        "sha256:append",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1, 2], vec!["one", "two"]),
    );
    assert_eq!(first, duplicate);
    verify_receipt(&path, &first).unwrap();
    let connection = Connection::open(path).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM _cdf_loads", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM _cdf_state", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn journal_and_synchronous_settings_are_preserved() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("durability.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .pragma_update(None, "journal_mode", "DELETE")
        .unwrap();
    connection
        .pragma_update(None, "synchronous", "FULL")
        .unwrap();
    let before_journal: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    let before_sync: i64 = connection
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .unwrap();
    drop(connection);
    let receipt = commit(
        &path,
        "sha256:durability",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1], vec!["one"]),
    );
    let connection = Connection::open(path).unwrap();
    let after_journal: String = connection
        .query_row("PRAGMA journal_mode", [], |row| row.get(0))
        .unwrap();
    let after_sync: i64 = connection
        .query_row("PRAGMA synchronous", [], |row| row.get(0))
        .unwrap();
    assert_eq!(after_journal, before_journal);
    assert_eq!(after_sync, before_sync);
    let transaction = receipt.transaction.unwrap();
    assert_eq!(transaction.values["journal_mode"], before_journal);
    assert_eq!(transaction.values["synchronous"], before_sync.to_string());
}

#[test]
fn incompatible_existing_schema_fails_without_mirror_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("incompatible.sqlite");
    Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE events(id TEXT NOT NULL, name TEXT NOT NULL) STRICT")
        .unwrap();
    let result = try_commit(
        &path,
        "sha256:incompatible",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1], vec!["one"]),
    );
    assert!(result.is_err());
    let connection = Connection::open(path).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name = '_cdf_loads'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn dropping_an_unfinalized_successful_session_rolls_back_payload_and_mirrors() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("crash-before-commit.sqlite");
    let batch = logical_batch(vec![1], vec!["one"]);
    let rows = batch.num_rows() as u64;
    let (mut session, state, canonical) = start_session(
        &path,
        "sha256:crash-before",
        WriteDisposition::Append,
        &[],
        batch,
    );
    session.apply_migrations().unwrap();
    session
        .write_segments(Box::new(std::iter::once(Ok(CommitSegment::new(
            state,
            rows * 16,
            vec![canonical],
        )))))
        .unwrap();
    drop(session);
    let connection = Connection::open(path).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('events', '_cdf_loads')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn runtime_capabilities_pin_one_bounded_writer_lane() {
    let capabilities = crate::runtime::sqlite_runtime_capabilities();
    capabilities.validate().unwrap();
    assert_eq!(capabilities.blocking_lanes.len(), 1);
    assert_eq!(capabilities.blocking_lanes[0].maximum_concurrency, 1);
    assert_eq!(capabilities.max_in_flight_segments, Some(1));
    assert_eq!(capabilities.max_in_flight_bytes, Some(64 * 1024 * 1024));
    assert_eq!(
        capabilities.commit_payload_mode,
        cdf_runtime::DestinationCommitPayloadMode::SegmentStreaming
    );
}

#[test]
fn zero_segment_package_commits_mirrors_without_creating_or_replacing_target() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("zero.sqlite");
    let schema = logical_batch(Vec::new(), Vec::new()).schema();
    let mut plan = test_plan(
        "sha256:zero",
        WriteDisposition::Replace,
        schema.as_ref(),
        &[],
        0,
    );
    plan.segments.clear();
    if let Some(delta) = &mut plan.state_delta {
        delta.segments.clear();
    }
    let package = Arc::new(TestPackage {
        hash: "sha256:zero".to_owned(),
        entries: Vec::new(),
        schema,
        quarantines: Vec::new(),
    });
    let (_, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let mut session = SqliteCommitSession::new(
        path.clone(),
        execution,
        SqliteCommitRequest {
            package,
            plan,
            segments: SqliteSessionSegments {
                expected: Default::default(),
            },
        },
    );
    session.apply_migrations().unwrap();
    let receipt = Box::new(session).finalize().unwrap();
    assert_eq!(receipt.counts, cdf_kernel::CommitCounts::default());
    verify_receipt(&path, &receipt).unwrap();
    let connection = Connection::open(path).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name = 'events'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM _cdf_loads", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
}

#[test]
fn zero_segment_replace_atomically_empties_an_existing_target() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("zero-replace.sqlite");
    let historical = commit(
        &path,
        "sha256:zero-replace-before",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1, 2], vec!["one", "two"]),
    );
    let replacement = commit_empty(
        &path,
        "sha256:zero-replace-after",
        WriteDisposition::Replace,
    );
    assert_eq!(replacement.counts.rows_written, 0);
    assert_eq!(replacement.counts.rows_deleted, Some(2));
    let connection = Connection::open(&path).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
    drop(connection);
    verify_receipt(&path, &historical).unwrap();
    verify_receipt(&path, &replacement).unwrap();
}

#[test]
fn replace_changes_the_target_atomically() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("replace.sqlite");
    commit(
        &path,
        "sha256:before",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1, 2], vec!["one", "two"]),
    );
    let receipt = commit(
        &path,
        "sha256:after",
        WriteDisposition::Replace,
        &[],
        logical_batch(vec![3], vec!["three"]),
    );
    assert_eq!(receipt.counts.rows_deleted, Some(2));
    let connection = Connection::open(path).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT group_concat(id) FROM events", [], |row| row
                .get::<_, String>(0))
            .unwrap(),
        "3"
    );
}

#[test]
fn merge_updates_matches_and_inserts_misses() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("merge.sqlite");
    commit(
        &path,
        "sha256:merge-before",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1, 2], vec!["one", "two"]),
    );
    let receipt = commit(
        &path,
        "sha256:merge-after",
        WriteDisposition::Merge,
        &["id"],
        logical_batch(vec![2, 3], vec!["TWO", "three"]),
    );
    assert_eq!(receipt.counts.rows_updated, Some(1));
    assert_eq!(receipt.counts.rows_inserted, Some(1));
    let connection = Connection::open(path).unwrap();
    let values = connection
        .prepare("SELECT id, name FROM events ORDER BY id")
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        values,
        vec![
            (1, "one".to_owned()),
            (2, "TWO".to_owned()),
            (3, "three".to_owned())
        ]
    );
}

#[test]
fn historical_receipts_remain_verifiable_after_replace_and_overlapping_merge() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("historical-verification.sqlite");
    let append = commit(
        &path,
        "sha256:history-append",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1, 2], vec!["one", "two"]),
    );
    let replace = commit(
        &path,
        "sha256:history-replace",
        WriteDisposition::Replace,
        &[],
        logical_batch(vec![2, 3], vec!["TWO", "three"]),
    );
    let merge = commit(
        &path,
        "sha256:history-merge",
        WriteDisposition::Merge,
        &["id"],
        logical_batch(vec![2, 4], vec!["two-again", "four"]),
    );
    for receipt in [&append, &replace, &merge] {
        verify_receipt(&path, receipt).unwrap();
    }
    let duplicate = commit(
        &path,
        "sha256:history-append",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1, 2], vec!["one", "two"]),
    );
    assert_eq!(duplicate, append);
    verify_receipt(&path, &duplicate).unwrap();
}

#[test]
fn merge_duplicate_keys_roll_back_without_target_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("merge-duplicate.sqlite");
    commit(
        &path,
        "sha256:merge-base",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1], vec!["one"]),
    );
    let result = try_commit(
        &path,
        "sha256:merge-duplicate",
        WriteDisposition::Merge,
        &["id"],
        logical_batch(vec![1, 1], vec!["first", "second"]),
    );
    assert!(result.is_err());
    let connection = Connection::open(path).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT name FROM events WHERE id = 1", [], |row| row
                .get::<_, String>(0))
            .unwrap(),
        "one"
    );
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM _cdf_loads", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
}

#[test]
fn quarantine_mirror_has_exact_transactional_readback_and_fresh_verification() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("quarantine.sqlite");
    let batch = logical_batch(vec![1], vec!["one"]);
    let rows = batch.num_rows() as u64;
    let record = QuarantineRecord {
        source_row_ordinal: 7,
        rule_id: "email-policy".to_owned(),
        error_code: "invalid-email".to_owned(),
        source_position: None,
        observed_value_redacted: QuarantineObservedValue::Masked {
            value: "***".to_owned(),
        },
    };
    let (mut session, state, canonical) = start_session_with_quarantines(
        &path,
        "sha256:quarantine",
        WriteDisposition::Append,
        &[],
        batch,
        vec![record],
    );
    session.apply_migrations().unwrap();
    session
        .write_segments(Box::new(std::iter::once(Ok(CommitSegment::new(
            state,
            rows * 16,
            vec![canonical],
        )))))
        .unwrap();
    let receipt = Box::new(session).finalize().unwrap();
    assert_eq!(
        receipt.transaction.as_ref().unwrap().values["quarantine_count"],
        "1"
    );
    verify_receipt(&path, &receipt).unwrap();
    let connection = Connection::open(&path).unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM _cdf_quarantine", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
    connection
        .execute(
            "UPDATE _cdf_quarantine
             SET quarantine_json = json_set(quarantine_json, '$.error_code', 'same-count-substitute')",
            [],
        )
        .unwrap();
    assert!(verify_receipt(&path, &receipt).is_err());
}

#[test]
fn verifier_falsifies_full_segment_state_evidence_and_provenance_corruption() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("verifier-corruption.sqlite");
    let receipt = commit(
        &path,
        "sha256:verifier-corruption",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1], vec!["one"]),
    );
    let connection = Connection::open(&path).unwrap();

    let segment_json: String = connection
        .query_row("SELECT segment_json FROM _cdf_segments", [], |row| {
            row.get(0)
        })
        .unwrap();
    connection
        .execute(
            "UPDATE _cdf_segments
             SET segment_json = json_set(segment_json, '$.row_count', 2)",
            [],
        )
        .unwrap();
    assert!(verify_receipt(&path, &receipt).is_err());
    connection
        .execute(
            "UPDATE _cdf_segments SET segment_json = ?1",
            [&segment_json],
        )
        .unwrap();
    verify_receipt(&path, &receipt).unwrap();

    let state_json: String = connection
        .query_row("SELECT state_json FROM _cdf_state_history", [], |row| {
            row.get(0)
        })
        .unwrap();
    connection
        .execute(
            "UPDATE _cdf_state_history
             SET state_json = json_set(state_json, '$.state_version', 77)",
            [],
        )
        .unwrap();
    assert!(verify_receipt(&path, &receipt).is_err());
    connection
        .execute(
            "UPDATE _cdf_state_history SET state_json = ?1",
            [&state_json],
        )
        .unwrap();
    verify_receipt(&path, &receipt).unwrap();

    let index_name: String = connection
        .query_row(
            "SELECT name FROM sqlite_schema
             WHERE type = 'index' AND tbl_name = 'events' AND name LIKE '_cdf_row_key_%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    connection
        .execute_batch(&format!(
            "DROP INDEX {}",
            crate::identifier::quote_identifier(&index_name)
        ))
        .unwrap();
    assert!(verify_receipt(&path, &receipt).is_err());
}

#[test]
fn injected_run_cancellation_stops_at_one_observation_and_rolls_back() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("cancel.sqlite");
    let batch = logical_batch(vec![1], vec!["one"]);
    let cancellation = cdf_runtime::RunCancellation::default();
    let (_, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let (mut session, _state, _canonical) = start_session_with_execution(
        &path,
        "sha256:cancel",
        WriteDisposition::Append,
        &[],
        batch,
        Vec::new(),
        execution.with_run_cancellation(cancellation.clone()),
    );
    session.apply_migrations().unwrap();
    cancellation.cancel();
    let observations = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = Arc::clone(&observations);
    let mut terminal = false;
    let iterator = std::iter::from_fn(move || {
        let prior = observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(prior, 0, "SQLite consumed more than one cancelled item");
        if terminal {
            return None;
        }
        terminal = true;
        Some(cancellation.check().and_then(|()| {
            Err(CdfError::internal(
                "cancelled iterator unexpectedly continued",
            ))
        }))
    });
    let error = session.write_segments(Box::new(iterator)).unwrap_err();
    assert!(error.message.contains("cancelled"));
    assert_eq!(observations.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(session);
    let connection = Connection::open(path).unwrap();
    assert_eq!(
        connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name IN ('events', '_cdf_loads')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
        0
    );
}

#[test]
fn sqlite_vm_progress_handler_interrupts_injected_cancellation() {
    let connection = Connection::open_in_memory().unwrap();
    let cancellation = cdf_runtime::RunCancellation::default();
    install_progress_handler(&connection, &cancellation).unwrap();
    cancellation.cancel();
    let raw = connection
        .query_row(
            "WITH RECURSIVE values_(value) AS (
                SELECT 1 UNION ALL SELECT value + 1 FROM values_ WHERE value < 1000000
             ) SELECT sum(value) FROM values_",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_err();
    let error = crate::error::classify_sqlite_execution_error(
        "run SQLite destination cancellation probe",
        raw,
        &cancellation,
    );
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("cancelled"));
}

#[test]
fn error_ownership_distinguishes_missing_durable_host_payload_and_target_failures() {
    let temp = tempfile::tempdir().unwrap();
    let durable_path = temp.path().join("durable.sqlite");
    let receipt = commit(
        &durable_path,
        "sha256:error-durable",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1], vec!["one"]),
    );
    let missing = temp.path().join("missing.sqlite");
    let error = verify_receipt(&missing, &receipt).unwrap_err();
    assert_eq!(error.kind, ErrorKind::Destination);

    let parent_file = temp.path().join("not-a-directory");
    std::fs::write(&parent_file, b"host obstacle").unwrap();
    let error = try_commit(
        &parent_file.join("destination.sqlite"),
        "sha256:error-host",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1], vec!["one"]),
    )
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Environment);

    let error = crate::error::classify_sqlite_payload_error(
        "insert SQLite payload row",
        rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_CONSTRAINT_NOTNULL),
            Some("NOT NULL constraint failed: events.name".to_owned()),
        ),
    );
    assert_eq!(error.kind, ErrorKind::Data);

    let target_path = temp.path().join("target.sqlite");
    commit(
        &target_path,
        "sha256:error-target-base",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1], vec!["one"]),
    );
    Connection::open(&target_path)
        .unwrap()
        .execute_batch("CREATE UNIQUE INDEX user_target_id_unique ON events(id)")
        .unwrap();
    let error = try_commit(
        &target_path,
        "sha256:error-target-conflict",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1], vec!["duplicate"]),
    )
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Destination);
}

const CRASH_HELPER_ENV: &str = "CDF_SQLITE_CRASH_HELPER";
const CRASH_HELPER_PATH_ENV: &str = "CDF_SQLITE_CRASH_HELPER_PATH";
const CRASH_HELPER_TEST: &str = "tests::sqlite_post_commit_crash_helper";

#[test]
fn sqlite_post_commit_crash_helper() {
    if std::env::var_os(CRASH_HELPER_ENV).is_none() {
        return;
    }
    let path = std::path::PathBuf::from(std::env::var_os(CRASH_HELPER_PATH_ENV).unwrap());
    let _ = commit(
        &path,
        "sha256:post-commit-crash",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1, 2], vec!["one", "two"]),
    );
    panic!("SQLite post-commit crash failpoint did not exit");
}

#[test]
fn subprocess_crash_after_commit_recovers_as_stable_duplicate_without_rewrite() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("post-commit-crash.sqlite");
    let executable = std::env::current_exe().unwrap(); // nosemgrep: rust.lang.security.current-exe.current-exe
    let output = std::process::Command::new(executable)
        .arg("--exact")
        .arg(CRASH_HELPER_TEST)
        .arg("--nocapture")
        .env(CRASH_HELPER_ENV, "1")
        .env(CRASH_HELPER_PATH_ENV, &path)
        .env(crate::transaction::TEST_EXIT_AFTER_COMMIT_ENV, "1")
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(crate::transaction::TEST_EXIT_AFTER_COMMIT_CODE),
        "helper failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let connection = Connection::open(&path).unwrap();
    let stored_json: String = connection
        .query_row("SELECT receipt_json FROM _cdf_loads", [], |row| row.get(0))
        .unwrap();
    let stored: cdf_kernel::Receipt = serde_json::from_str(&stored_json).unwrap();
    let before = connection
        .query_row(
            "SELECT COUNT(*), SUM(_cdf_row_key) FROM events",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .unwrap();
    drop(connection);
    let replay = commit(
        &path,
        "sha256:post-commit-crash",
        WriteDisposition::Append,
        &[],
        logical_batch(vec![1, 2], vec!["one", "two"]),
    );
    assert_eq!(replay, stored);
    verify_receipt(&path, &replay).unwrap();
    let connection = Connection::open(path).unwrap();
    let after = connection
        .query_row(
            "SELECT COUNT(*), SUM(_cdf_row_key) FROM events",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM _cdf_loads", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap(),
        1
    );
}

#[test]
fn subprocess_crashes_during_payload_and_mirror_mutation_roll_back_atomically() {
    for (phase, code) in [
        ("payload", crate::transaction::TEST_EXIT_DURING_PAYLOAD_CODE),
        ("mirrors", crate::transaction::TEST_EXIT_DURING_MIRRORS_CODE),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(format!("pre-commit-{phase}.sqlite"));
        let executable = std::env::current_exe().unwrap(); // nosemgrep: rust.lang.security.current-exe.current-exe
        let output = std::process::Command::new(executable)
            .arg("--exact")
            .arg(CRASH_HELPER_TEST)
            .arg("--nocapture")
            .env(CRASH_HELPER_ENV, "1")
            .env(CRASH_HELPER_PATH_ENV, &path)
            .env(crate::transaction::TEST_EXIT_BEFORE_COMMIT_ENV, phase)
            .output()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(code),
            "{phase} helper failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let connection = Connection::open(&path).unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_schema
                     WHERE name IN ('events', '_cdf_loads', '_cdf_commit_evidence')",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
            0,
            "{phase} crash leaked uncommitted destination state"
        );
        drop(connection);
        let replay = commit(
            &path,
            "sha256:post-commit-crash",
            WriteDisposition::Append,
            &[],
            logical_batch(vec![1, 2], vec!["one", "two"]),
        );
        verify_receipt(&path, &replay).unwrap();
    }
}
