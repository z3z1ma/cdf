use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use cdf_kernel::{
    Batch, CdfError, CursorOrderingClaim, CursorSpec, CursorValue, DeclarativeExpression,
    PushdownFidelity, ResourceDescriptor, ResourceId, ResourceStream, Result, ScanPredicate,
    ScanRequest, SchemaHash, SchemaSource, ScopeKey, SourcePosition, TrustLevel, WriteDisposition,
};
use cdf_runtime::RunCancellation;
use futures_util::StreamExt;
use rusqlite::{
    Connection,
    types::{Value, ValueRef},
};

use crate::{
    catalog::{SQLITE_STRICT_METADATA_KEY, SQLITE_UNIQUE_METADATA_KEY},
    identifier::SqliteIdentifier,
    native::{
        SQLITE_DEFAULT_OUTPUT_BATCH_ROWS, SqliteNativeOptions, SqliteSourceInput,
        discover_sqlite_query,
    },
};

use super::{
    SqliteSourceResource,
    execution::{
        ColumnBuilder, SQLITE_MAXIMUM_BATCH_BYTES, classify_execution_error,
        install_progress_handler,
    },
    plan_sqlite_source_partition,
    query::{build_query, scan_from_partition},
    schema::{SqliteTemporalEncoding, validate_sqlite_source_resource_shape},
    sqlite_source_predicate_fidelity,
    temporal::{decode_timestamp, encode_temporal_cursor},
};

fn table_input() -> SqliteSourceInput {
    SqliteSourceInput::Table {
        table: SqliteIdentifier::new("events").unwrap(),
    }
}

fn descriptor(cursor: bool) -> ResourceDescriptor {
    ResourceDescriptor {
        resource_id: ResourceId::new("local.events").unwrap(),
        schema_source: SchemaSource::Declared {
            schema_hash: SchemaHash::new("sha256:sqlite-source-test").unwrap(),
            source: "sqlite://fixtures/events.sqlite".to_owned(),
        },
        primary_key: vec!["id".to_owned()],
        merge_key: vec!["id".to_owned()],
        cursor: cursor.then(|| CursorSpec {
            field: "updated_at".to_owned(),
            ordering: CursorOrderingClaim::Exact,
            lag_tolerance_ms: 0,
        }),
        write_disposition: WriteDisposition::Merge,
        deduplication: None,
        contract: None,
        state_scope: ScopeKey::Resource,
        freshness: None,
        trust_level: TrustLevel::Governed,
    }
}
fn schema(strict: bool) -> SchemaRef {
    let metadata = std::collections::HashMap::from([(
        SQLITE_STRICT_METADATA_KEY.to_owned(),
        strict.to_string(),
    )]);
    let mut unique_metadata = metadata.clone();
    unique_metadata.insert(SQLITE_UNIQUE_METADATA_KEY.to_owned(), "true".to_owned());
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(unique_metadata),
        Field::new("name", DataType::Utf8, true).with_metadata(metadata.clone()),
        Field::new("updated_at", DataType::Int64, false).with_metadata(metadata),
    ]))
}

fn resource_with_execution(
    path: &Path,
    descriptor: ResourceDescriptor,
    schema: SchemaRef,
    stable_key: Option<&str>,
    temporal_encodings: BTreeMap<String, SqliteTemporalEncoding>,
) -> (
    Arc<cdf_engine::StandaloneExecutionHost>,
    SqliteSourceResource,
) {
    let (host, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let resource = SqliteSourceResource::new(
        path.to_owned(),
        descriptor,
        schema,
        table_input(),
        stable_key.map(|key| SqliteIdentifier::new(key).unwrap()),
        temporal_encodings,
        SqliteNativeOptions::default(),
    )
    .unwrap()
    .with_execution(execution)
    .unwrap();
    (host, resource)
}

fn full_scan_request(descriptor: &ResourceDescriptor) -> ScanRequest {
    ScanRequest {
        resource_id: descriptor.resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: descriptor.state_scope.clone(),
    }
}

fn read_all(
    host: &cdf_engine::StandaloneExecutionHost,
    resource: &SqliteSourceResource,
) -> Result<Vec<Batch>> {
    let partitions = resource.plan_partitions(&full_scan_request(resource.descriptor()))?;
    host.block_on_root(async {
        let mut stream = resource.open(partitions[0].clone()).await?;
        let mut batches = Vec::new();
        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }
        stream.completion().await?;
        Ok(batches)
    })
}

#[test]
fn declared_execution_observes_live_catalog_drift_without_changing_logical_output() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("declared-observation.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE events (
                id TEXT PRIMARY KEY,
                name TEXT,
                updated_at INTEGER NOT NULL
             ) STRICT;
             INSERT INTO events VALUES ('42', 'first', 1);",
        )
        .unwrap();
    drop(connection);

    let descriptor = descriptor(false);
    let (host, mut resource) =
        resource_with_execution(&path, descriptor, schema(true), None, BTreeMap::new());
    resource.type_policy_allowances = cdf_kernel::TypePolicyAllowances {
        coerce_types: true,
        allow_lossy_mapping: false,
    };
    let first = read_all(&host, &resource).unwrap().remove(0);
    let first_output = first.record_batch().unwrap();
    assert_eq!(first_output.schema().field(0).data_type(), &DataType::Int64);
    assert_eq!(
        first_output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    let first_physical = first.header.materialized_physical_schema().unwrap();
    assert_eq!(first_physical.field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        first.header.observed_schema_hash,
        cdf_kernel::canonical_arrow_schema_hash(&first_physical).unwrap()
    );
    assert_ne!(
        first.header.observed_schema_hash,
        cdf_kernel::canonical_arrow_schema_hash(first_output.schema().as_ref()).unwrap()
    );

    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "DROP TABLE events;
             CREATE TABLE events (
                id INTEGER PRIMARY KEY,
                name TEXT,
                updated_at INTEGER NOT NULL
             ) STRICT;
             INSERT INTO events VALUES (42, 'second', 1);",
        )
        .unwrap();
    drop(connection);

    let second = read_all(&host, &resource).unwrap().remove(0);
    let second_output = second.record_batch().unwrap();
    let second_physical = second.header.materialized_physical_schema().unwrap();
    assert_eq!(
        second_output.schema().field(0).data_type(),
        &DataType::Int64
    );
    assert_eq!(second_physical.field(0).data_type(), &DataType::Int64);
    assert_ne!(
        first.header.observed_schema_hash,
        second.header.observed_schema_hash
    );
}

#[test]
fn native_query_streams_complex_read_in_configured_batches() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("native-query.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE events(id INTEGER PRIMARY KEY, category TEXT, payload TEXT);\
             INSERT INTO events VALUES\
               (1, 'a', '{\"value\":2}'),\
               (2, 'a', '{\"value\":3}'),\
               (3, 'b', '{\"value\":5}');",
        )
        .unwrap();
    drop(connection);
    let input = SqliteSourceInput::from_authored(
        None,
        Some(
            "WITH ranked AS (SELECT id, category, json_extract(payload, '$.value') AS value, row_number() OVER (PARTITION BY category ORDER BY id) AS ordinal FROM events) SELECT id, category, value, ordinal FROM ranked"
                .to_owned(),
        ),
    )
    .unwrap();
    let options = SqliteNativeOptions {
        output_batch_rows: 2,
        ..SqliteNativeOptions::default()
    };
    let discovered = discover_sqlite_query(
        &path,
        &descriptor(false).resource_id,
        &input,
        &options,
        1_000,
        16 * 1024 * 1024,
    )
    .unwrap();
    let (host, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();
    let resource = SqliteSourceResource::new(
        path,
        descriptor(false),
        Arc::new(discovered.schema),
        input,
        None,
        BTreeMap::new(),
        options,
    )
    .unwrap()
    .with_execution(execution)
    .unwrap();
    let batches = read_all(&host, &resource).unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert_eq!(batches[0].record_batch().unwrap().num_columns(), 4);
}

#[test]
fn exact_and_inexact_pushdown_follow_strict_type_authority() {
    let expression = DeclarativeExpression::parse_comparison("updated_at >= 10").unwrap();
    assert_eq!(
        sqlite_source_predicate_fidelity(&schema(true), &expression),
        PushdownFidelity::Exact
    );
    assert_eq!(
        sqlite_source_predicate_fidelity(&schema(false), &expression),
        PushdownFidelity::Inexact
    );
    assert_eq!(
        sqlite_source_predicate_fidelity(
            &schema(true),
            &DeclarativeExpression::parse_comparison("name = 'ada'").unwrap()
        ),
        PushdownFidelity::Inexact
    );
}

#[test]
fn source_limit_is_never_applied_before_residual_filtering_or_cursor_group_close() {
    let input = table_input();
    let inexact_request = ScanRequest {
        resource_id: descriptor(false).resource_id.clone(),
        projection: None,
        filters: vec![
            ScanPredicate::new(
                cdf_kernel::PredicateId::new("name-filter").unwrap(),
                "name = 'qualified'",
            )
            .unwrap(),
        ],
        limit: Some(1),
        order_by: Vec::new(),
        scope: ScopeKey::Resource,
    };
    let partition = plan_sqlite_source_partition(
        &descriptor(false),
        &schema(false),
        &input,
        None,
        &BTreeMap::new(),
        None,
        &inexact_request,
    )
    .unwrap();
    assert_eq!(partition.scan_intent.limit, None);
    let scan = scan_from_partition(
        &descriptor(false),
        &schema(false),
        &input,
        None,
        &BTreeMap::new(),
        None,
        &partition,
    )
    .unwrap();
    let query = build_query(
        &descriptor(false),
        &schema(false),
        &input,
        None,
        &BTreeMap::new(),
        &partition,
        &scan,
    )
    .unwrap();
    assert!(!query.sql.contains(" LIMIT "));
    assert!(!query.sql.contains(" WHERE "));

    let cursor_request = ScanRequest {
        resource_id: descriptor(true).resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: Some(1),
        order_by: Vec::new(),
        scope: ScopeKey::Resource,
    };
    let partition = plan_sqlite_source_partition(
        &descriptor(true),
        &schema(true),
        &input,
        Some(&SqliteIdentifier::new("id").unwrap()),
        &BTreeMap::new(),
        None,
        &cursor_request,
    )
    .unwrap();
    assert_eq!(partition.scan_intent.limit, None);
    let scan = scan_from_partition(
        &descriptor(true),
        &schema(true),
        &input,
        Some(&SqliteIdentifier::new("id").unwrap()),
        &BTreeMap::new(),
        None,
        &partition,
    )
    .unwrap();
    let query = build_query(
        &descriptor(true),
        &schema(true),
        &input,
        Some(&SqliteIdentifier::new("id").unwrap()),
        &BTreeMap::new(),
        &partition,
        &scan,
    )
    .unwrap();
    assert!(!query.sql.contains(" LIMIT "));
}

#[test]
fn cursor_requires_stable_tie_breaker_and_canonical_order() {
    let input = table_input();
    let error = validate_sqlite_source_resource_shape(
        &descriptor(true),
        &schema(true),
        None,
        &BTreeMap::new(),
    )
    .unwrap_err();
    assert!(error.message.contains("stable_key"));
    let request = ScanRequest {
        resource_id: descriptor(true).resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: ScopeKey::Resource,
    };
    let partition = plan_sqlite_source_partition(
        &descriptor(true),
        &schema(true),
        &input,
        Some(&SqliteIdentifier::new("id").unwrap()),
        &BTreeMap::new(),
        None,
        &request,
    )
    .unwrap();
    let scan = scan_from_partition(
        &descriptor(true),
        &schema(true),
        &input,
        Some(&SqliteIdentifier::new("id").unwrap()),
        &BTreeMap::new(),
        None,
        &partition,
    )
    .unwrap();
    assert_eq!(scan.order_by[0].field, "updated_at");
    assert_eq!(scan.order_by[1].field, "id");

    let unproven_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("updated_at", DataType::Int64, false),
    ]));
    validate_sqlite_source_resource_shape(
        &descriptor(true),
        &unproven_schema,
        Some(&SqliteIdentifier::new("id").unwrap()),
        &BTreeMap::new(),
    )
    .unwrap();
}

#[test]
fn live_cursor_rejects_duplicate_unconstrained_stable_keys() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("duplicates.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
            .execute_batch(
                "CREATE TABLE events (id INTEGER NOT NULL, name TEXT, updated_at INTEGER NOT NULL) STRICT;
                 INSERT INTO events VALUES (1, 'first', 10), (1, 'duplicate', 10);",
            )
            .unwrap();
    drop(connection);
    let descriptor = descriptor(true);
    let (host, resource) =
        resource_with_execution(&path, descriptor, schema(true), Some("id"), BTreeMap::new());
    let error = read_all(&host, &resource).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("PRIMARY KEY or UNIQUE"));
}

#[test]
fn type_policy_controls_dynamic_storage_conversion() {
    let field = Field::new("id", DataType::Int64, false);
    let mut strict = ColumnBuilder::new(&field, 1).unwrap();
    assert!(
        strict
            .append(
                &field,
                ValueRef::Text(b"42"),
                None,
                cdf_kernel::TypePolicyAllowances::default(),
            )
            .is_err()
    );

    let mut coercing = ColumnBuilder::new(&field, 1).unwrap();
    coercing
        .append(
            &field,
            ValueRef::Text(b"42"),
            None,
            cdf_kernel::TypePolicyAllowances {
                coerce_types: true,
                allow_lossy_mapping: false,
            },
        )
        .unwrap();
    let values = coercing.finish();
    let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(values.values(), &[42]);

    let mut lossless_only = ColumnBuilder::new(&field, 1).unwrap();
    assert!(
        lossless_only
            .append(
                &field,
                ValueRef::Real(42.5),
                None,
                cdf_kernel::TypePolicyAllowances {
                    coerce_types: true,
                    allow_lossy_mapping: false,
                },
            )
            .is_err()
    );
    let mut lossy = ColumnBuilder::new(&field, 1).unwrap();
    lossy
        .append(
            &field,
            ValueRef::Real(42.5),
            None,
            cdf_kernel::TypePolicyAllowances {
                coerce_types: true,
                allow_lossy_mapping: true,
            },
        )
        .unwrap();
    let values = lossy.finish();
    assert_eq!(
        values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[42]
    );
}

#[test]
fn vm_progress_handler_interrupts_cancelled_sqlite_work() {
    let connection = Connection::open_in_memory().unwrap();
    let cancellation = RunCancellation::default();
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
    let error = classify_execution_error("run cancellation probe", raw, &cancellation);
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Internal);
    assert!(error.message.contains("cancelled"));
}

#[test]
fn oversized_variable_cell_is_rejected_before_arrow_copy() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("oversized.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, updated_at INTEGER) STRICT",
            [],
        )
        .unwrap();
    let oversized = "x".repeat(usize::try_from(SQLITE_MAXIMUM_BATCH_BYTES).unwrap() + 1);
    connection
        .execute("INSERT INTO events VALUES (1, ?1, 1)", [&oversized])
        .unwrap();
    drop(connection);
    let descriptor = descriptor(false);
    let (host, resource) =
        resource_with_execution(&path, descriptor, schema(true), None, BTreeMap::new());
    let error = read_all(&host, &resource).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("before Arrow allocation"));
}

#[test]
fn temporal_encodings_are_explicit_and_round_trip_cursor_units() {
    let field = Field::new(
        "observed_at",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    );
    assert_eq!(
        decode_timestamp(
            &field,
            ValueRef::Integer(1_700_000_000),
            Some(SqliteTemporalEncoding::UnixSeconds),
            TimeUnit::Microsecond
        )
        .unwrap(),
        1_700_000_000_000_000
    );
    assert!(
        decode_timestamp(
            &field,
            ValueRef::Text(b"2023-11-14T22:13:20Z"),
            None,
            TimeUnit::Microsecond
        )
        .is_err()
    );
    let encoded = encode_temporal_cursor(
        1_700_000_000_123_456,
        Some(SqliteTemporalEncoding::Iso8601Text),
        false,
    )
    .unwrap();
    assert_eq!(
        encoded,
        Value::Text("2023-11-14T22:13:20.123456Z".to_owned())
    );
}

#[test]
fn debug_redacts_database_path() {
    let resource = SqliteSourceResource::new(
        PathBuf::from("/private/operator/customer.sqlite"),
        descriptor(false),
        schema(true),
        table_input(),
        None,
        BTreeMap::new(),
        SqliteNativeOptions::default(),
    )
    .unwrap();
    let debug = format!("{resource:?}");
    assert!(debug.contains("<redacted-sqlite-database>"));
    assert!(!debug.contains("customer.sqlite"));
}

#[test]
fn production_stream_orders_equal_cursors_by_stable_key() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("events.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE events (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    updated_at INTEGER NOT NULL
                ) STRICT;
                INSERT INTO events VALUES (3, 'third', 10);
                INSERT INTO events VALUES (1, 'first', 10);
                INSERT INTO events VALUES (2, 'second', 10);",
        )
        .unwrap();
    drop(connection);

    let descriptor = descriptor(true);
    let (host, resource) =
        resource_with_execution(&path, descriptor, schema(true), Some("id"), BTreeMap::new());
    let batches = read_all(&host, &resource).unwrap();
    let ids = batches[0]
        .record_batch()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2, 3]);
    let SourcePosition::Cursor(position) = batches
        .last()
        .unwrap()
        .header
        .source_position
        .as_ref()
        .unwrap()
    else {
        panic!("cursor source must emit a cursor position");
    };
    assert_eq!(position.value, CursorValue::I64(10));
}

#[test]
fn production_stream_decodes_explicit_temporal_cursor_encoding() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("temporal.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE events (
                    id INTEGER PRIMARY KEY,
                    observed_at INTEGER NOT NULL
                ) STRICT;
                INSERT INTO events VALUES (1, 1700000000123);",
        )
        .unwrap();
    drop(connection);
    let metadata = std::collections::HashMap::from([(
        SQLITE_STRICT_METADATA_KEY.to_owned(),
        "true".to_owned(),
    )]);
    let mut unique_metadata = metadata.clone();
    unique_metadata.insert(SQLITE_UNIQUE_METADATA_KEY.to_owned(), "true".to_owned());
    let temporal_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false).with_metadata(unique_metadata),
        Field::new(
            "observed_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )
        .with_metadata(metadata),
    ]));
    let mut temporal_descriptor = descriptor(true);
    temporal_descriptor.cursor.as_mut().unwrap().field = "observed_at".to_owned();
    let encodings = BTreeMap::from([(
        "observed_at".to_owned(),
        SqliteTemporalEncoding::UnixMilliseconds,
    )]);
    let (host, resource) = resource_with_execution(
        &path,
        temporal_descriptor,
        temporal_schema,
        Some("id"),
        encodings,
    );
    let batches = read_all(&host, &resource).unwrap();
    let observed_at = batches[0]
        .record_batch()
        .unwrap()
        .column(1)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(observed_at.value(0), 1_700_000_000_123_000);
    let SourcePosition::Cursor(position) = batches[0].header.source_position.as_ref().unwrap()
    else {
        panic!("temporal cursor source must emit a cursor position");
    };
    assert_eq!(
        position.value,
        CursorValue::TimestampMicros {
            micros: 1_700_000_000_123_000,
            timezone: Some("UTC".to_owned()),
        }
    );
}

#[test]
fn production_stream_rejects_dynamic_storage_drift_as_data() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("drift.sqlite");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE events (id INTEGER UNIQUE, name TEXT, updated_at INTEGER);
                 INSERT INTO events VALUES ('not-an-integer', 'drifted', 1);",
        )
        .unwrap();
    drop(connection);
    let descriptor = descriptor(true);
    let (host, resource) = resource_with_execution(
        &path,
        descriptor,
        schema(false),
        Some("id"),
        BTreeMap::new(),
    );
    let partition = resource
        .plan_partitions(&full_scan_request(resource.descriptor()))
        .unwrap()
        .remove(0);
    let error = host
        .block_on_root(async {
            let mut stream = resource.open(partition).await?;
            let error = stream.next().await.unwrap().unwrap_err();
            stream.join_failed_attempt().await?;
            Ok::<_, CdfError>(error)
        })
        .unwrap();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("dynamic storage class"));
    assert!(!error.message.contains(path.to_str().unwrap()));
}

#[test]
fn production_stream_holds_one_snapshot_across_concurrent_wal_commit() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("snapshot.sqlite");
    let mut connection = Connection::open(&path).unwrap();
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .unwrap();
    let transaction = connection.transaction().unwrap();
    transaction
        .execute_batch(
            "CREATE TABLE events (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                ) STRICT;",
        )
        .unwrap();
    {
        let mut insert = transaction
            .prepare("INSERT INTO events VALUES (?1, ?2, 1)")
            .unwrap();
        let last_id = i64::try_from(SQLITE_DEFAULT_OUTPUT_BATCH_ROWS).unwrap() + 808;
        for id in 1_i64..=last_id {
            insert.execute((id, format!("original-{id}"))).unwrap();
        }
    }
    transaction.commit().unwrap();
    drop(connection);

    let descriptor = descriptor(true);
    let (host, resource) =
        resource_with_execution(&path, descriptor, schema(true), Some("id"), BTreeMap::new());
    let partition = resource
        .plan_partitions(&full_scan_request(resource.descriptor()))
        .unwrap()
        .remove(0);
    host.block_on_root(async {
        let mut stream = resource.open(partition).await.unwrap();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(
            first.header.row_count,
            SQLITE_DEFAULT_OUTPUT_BATCH_ROWS as u64
        );

        let last_id = i64::try_from(SQLITE_DEFAULT_OUTPUT_BATCH_ROWS).unwrap() + 808;
        let writer = Connection::open(&path).unwrap();
        writer
            .execute(
                "UPDATE events SET name = 'committed-after-first-batch' WHERE id = ?1",
                [last_id],
            )
            .unwrap();
        drop(writer);

        let second = stream.next().await.unwrap().unwrap();
        let names = second
            .record_batch()
            .unwrap()
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(names.len() - 1), format!("original-{last_id}"));
        assert!(stream.next().await.is_none());
        stream.completion().await.unwrap();
    });

    let connection = Connection::open(&path).unwrap();
    let committed: String = connection
        .query_row(
            "SELECT name FROM events WHERE id = ?1",
            [i64::try_from(SQLITE_DEFAULT_OUTPUT_BATCH_ROWS).unwrap() + 808],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(committed, "committed-after-first-batch");
}
