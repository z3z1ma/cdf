use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::Arc,
};

use arrow_array::{
    Array, BinaryArray, Date32Array, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampSecondArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use cdf_http::{SecretProvider, SecretUri, SecretValue};
use cdf_kernel::{
    CompiledScanIntent, CursorPosition, CursorSpec, CursorValue, PartitionId, PartitionPlan,
    PartitionRetrySafety, PredicateId, PushdownFidelity, PushedPredicate, QueryableResource,
    ResourceDescriptor, ResourceId, ScanPredicate, ScanRequest, SchemaHash, SchemaSource, ScopeKey,
    SourcePosition, TrustLevel, WriteDisposition, physical_type, with_physical_type,
    with_source_name,
};
use cdf_runtime::{
    RunCancellation, SourceCompileRequest, SourceDriver, SourceDriverId, SourceEgressScope,
    SourceExecutorClass, SourceResolutionContext,
};
use futures_util::StreamExt;

use crate::{
    ClickHouseSourceDriver,
    catalog::discover_clickhouse_table,
    client::ClickHouseConnection,
    execution::{
        CLICKHOUSE_MAXIMUM_POLL_BYTES, cursor_value, normalize_record_batch,
        project_physical_schema, validate_effective_physical_authority,
    },
    identifier::ClickHouseIdentifier,
    memory::{
        CLICKHOUSE_ARROW_BODY_BYTES, CLICKHOUSE_ARROW_CONTAINER_HEADROOM_BYTES,
        CLICKHOUSE_ARROW_MESSAGE_BYTES, CLICKHOUSE_ARROW_SCRATCH_CAPACITY_BYTES,
        CLICKHOUSE_CURSOR_STATE_BYTES, CLICKHOUSE_DECODE_LEASE_BYTES,
        CLICKHOUSE_HTTP1_TRANSPORT_BYTES, clickhouse_decode_envelope_bytes,
    },
    query::{build_query, predicate_fidelity, scan_from_partition, source_expression},
    resource::clickhouse_table_capabilities,
    types::{
        CLICKHOUSE_CURSOR_CAST_METADATA_KEY, CLICKHOUSE_MAXIMUM_TYPE_NESTING_DEPTH,
        CLICKHOUSE_MAXIMUM_TYPE_STRUCTURAL_TOKENS, CLICKHOUSE_MAXIMUM_TYPE_TEXT_BYTES,
        CLICKHOUSE_TARGET_ARROW_BODY_BYTES, ClickHouseCursorCast, bounded_block_rows,
        fixed_projection_body_bytes, projection_has_variable_width, validate_clickhouse_type,
        validate_resource_shape, with_cursor_cast,
    },
};

#[test]
fn resolved_connection_debug_redacts_credentials() {
    let connection = ClickHouseConnection::new(
        "https://warehouse.example:8443".to_owned(),
        ClickHouseIdentifier::new("analytics").unwrap(),
        Some("private-user".to_owned()),
        Some("private-password".to_owned()),
        4,
        65_536,
    );
    let debug = format!("{connection:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("private-user"));
    assert!(!debug.contains("private-password"));
}

fn descriptor(cursor: bool) -> ResourceDescriptor {
    ResourceDescriptor {
        resource_id: ResourceId::new("warehouse.events").unwrap(),
        schema_source: SchemaSource::Declared {
            schema_hash: SchemaHash::new("schema-clickhouse-tests").unwrap(),
            source: "clickhouse://warehouse/events".to_owned(),
        },
        primary_key: vec!["event_id".to_owned()],
        merge_key: Vec::new(),
        cursor: cursor.then(|| CursorSpec {
            field: "sequence".to_owned(),
            ordering: cdf_kernel::CursorOrderingClaim::Exact,
            lag_tolerance_ms: 0,
        }),
        write_disposition: WriteDisposition::Append,
        deduplication: None,
        contract: None,
        state_scope: ScopeKey::Resource,
        freshness: None,
        trust_level: TrustLevel::Governed,
    }
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("sequence", DataType::UInt64, false),
        Field::new("event_id", DataType::UInt64, false),
        Field::new("payload", DataType::Binary, true),
    ])
}

#[test]
fn identifiers_are_quoted_without_accepting_fragments() {
    assert_eq!(
        ClickHouseIdentifier::new("odd`name").unwrap().quoted(),
        "`odd\\`name`"
    );
    assert!(ClickHouseIdentifier::new("bad\nname").is_err());
}

#[test]
fn compile_is_contact_free_redacted_and_io_owned() {
    let driver = ClickHouseSourceDriver::new().unwrap();
    let plan = driver
        .compile(SourceCompileRequest {
            source_kind: "clickhouse".to_owned(),
            context: cdf_runtime::SourceCompileContext {
                source_name: "warehouse".to_owned(),
                project_root: None,
                cursor_pushdown: None,
            },
            source_options: BTreeMap::from([
                (
                    "endpoint".to_owned(),
                    serde_json::json!("clickhouses://warehouse.example:8443"),
                ),
                ("database".to_owned(), serde_json::json!("analytics")),
                (
                    "username".to_owned(),
                    serde_json::json!("secret://env/CLICKHOUSE_USER"),
                ),
                (
                    "password".to_owned(),
                    serde_json::json!("secret://env/CLICKHOUSE_PASSWORD"),
                ),
            ]),
            resource_options: BTreeMap::from([
                ("table".to_owned(), serde_json::json!("events")),
                ("stable_key".to_owned(), serde_json::json!("event_id")),
            ]),
            descriptor: descriptor(true),
            schema: schema(),
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        })
        .unwrap();

    assert_eq!(plan.driver.driver_id.as_str(), "clickhouse");
    assert_eq!(
        plan.execution_capabilities.executor_class,
        SourceExecutorClass::Io
    );
    assert!(plan.execution_capabilities.blocking_lane.is_none());
    assert_eq!(plan.execution_capabilities.maximum_concurrency, 1);
    let encoded = serde_json::to_string(&plan).unwrap();
    assert!(encoded.contains("secret://env/CLICKHOUSE_PASSWORD"));
    assert!(!encoded.contains("inline-password"));
    driver.validate_portable_plan(&plan).unwrap();

    let (_host, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let context = SourceResolutionContext::new(
        Path::new("."),
        Arc::new(NoSecrets),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let error = match driver.resolve(&plan, &context) {
        Ok(_) => panic!("declared schema resolved without physical catalog evidence"),
        Err(error) => error,
    };
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("catalog-backed physical schema"));
}

#[test]
fn compile_rejects_inline_credentials_and_cursor_without_tie_breaker() {
    let driver = ClickHouseSourceDriver::new().unwrap();
    let request = SourceCompileRequest {
        source_kind: "clickhouse".to_owned(),
        context: cdf_runtime::SourceCompileContext {
            source_name: "warehouse".to_owned(),
            project_root: None,
            cursor_pushdown: None,
        },
        source_options: BTreeMap::from([
            (
                "endpoint".to_owned(),
                serde_json::json!("clickhouse://user:password@localhost:8123"),
            ),
            ("database".to_owned(), serde_json::json!("analytics")),
        ]),
        resource_options: BTreeMap::from([("table".to_owned(), serde_json::json!("events"))]),
        descriptor: descriptor(true),
        schema: schema(),
        type_policy_allowances: Default::default(),
        effective_schema_runtime: None,
        baseline_observation_schema_catalog: Vec::new(),
    };
    let error = driver.compile(request).unwrap_err();
    assert!(error.to_string().contains("credential-free"));
}

#[test]
fn type_admission_is_recursive_and_rejects_dynamic_shapes_truthfully() {
    for supported in [
        "UInt64",
        "Decimal(38, 6)",
        "Decimal256(38)",
        "DateTime64(6, 'UTC')",
        "DateTime",
        "Nullable(LowCardinality(String))",
        "Array(Tuple(id UInt64, payload String))",
        "Map(String, Array(Nullable(Int32)))",
        "Enum8('ready' = 1, 'done' = 2)",
        "IPv6",
        "UUID",
    ] {
        validate_clickhouse_type("value", supported).unwrap();
    }
    for unsupported in [
        "Dynamic",
        "Variant(UInt64, String)",
        "AggregateFunction(sum, UInt64)",
        "Point",
    ] {
        let error = validate_clickhouse_type("value", unsupported).unwrap_err();
        assert!(error.to_string().contains("value"));
        assert!(error.to_string().contains(unsupported));
    }
    for wrapped in [
        "Nullable(UUID)",
        "Array(Date)",
        "LowCardinality(DateTime('UTC'))",
        "Map(String, DateTime)",
        "Tuple(id UInt64, observed Nullable(Date))",
    ] {
        let error = validate_clickhouse_type("value", wrapped).unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
        assert!(error.message.contains("wraps UUID, Date, or DateTime"));
    }
}

#[test]
fn type_parser_preflight_bounds_hostile_catalog_strings_before_recursive_matching() {
    let at_depth_limit = format!(
        "{}UInt64{}",
        "Array(".repeat(CLICKHOUSE_MAXIMUM_TYPE_NESTING_DEPTH),
        ")".repeat(CLICKHOUSE_MAXIMUM_TYPE_NESTING_DEPTH)
    );
    validate_clickhouse_type("value", &at_depth_limit).unwrap();

    let over_depth = format!("Array({at_depth_limit})");
    let error = validate_clickhouse_type("value", &over_depth).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("nesting limit"));

    let over_tokens = format!(
        "Tuple({})",
        vec!["UInt64"; CLICKHOUSE_MAXIMUM_TYPE_STRUCTURAL_TOKENS].join(", ")
    );
    let error = validate_clickhouse_type("value", &over_tokens).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("token parser limit"));

    let over_text = "X".repeat(CLICKHOUSE_MAXIMUM_TYPE_TEXT_BYTES + 1);
    let error = validate_clickhouse_type("value", &over_text).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("byte parser limit"));

    let error = validate_clickhouse_type("value", "Tuple(UInt64, Array(String)").unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("malformed"));
}

#[test]
fn decode_envelope_accounts_for_allocator_capacity_and_container_headroom() {
    let expected = u64::try_from(CLICKHOUSE_ARROW_SCRATCH_CAPACITY_BYTES).unwrap()
        + u64::try_from(CLICKHOUSE_ARROW_BODY_BYTES).unwrap()
        + u64::try_from(CLICKHOUSE_ARROW_MESSAGE_BYTES).unwrap()
        + CLICKHOUSE_HTTP1_TRANSPORT_BYTES
        + u64::try_from(CLICKHOUSE_ARROW_CONTAINER_HEADROOM_BYTES).unwrap();
    assert_eq!(clickhouse_decode_envelope_bytes().unwrap(), expected);
    assert!(expected < CLICKHOUSE_DECODE_LEASE_BYTES);
    assert_eq!(CLICKHOUSE_ARROW_SCRATCH_CAPACITY_BYTES, 32 * 1024 * 1024);
    assert_eq!(
        CLICKHOUSE_MAXIMUM_POLL_BYTES,
        CLICKHOUSE_DECODE_LEASE_BYTES
            + CLICKHOUSE_CURSOR_STATE_BYTES
            + CLICKHOUSE_HTTP1_TRANSPORT_BYTES
    );
}

#[test]
fn uuid_projection_and_physical_schema_binding_are_explicit_and_lossless() {
    let field = with_source_name(
        with_physical_type(Field::new("uuid", DataType::Utf8, false), "UUID"),
        "source_uuid",
    );
    let schema = Arc::new(Schema::new_with_metadata(
        vec![field],
        HashMap::from([("schema_layer".to_owned(), "effective".to_owned())]),
    ));
    let predicate = ScanPredicate::new(
        PredicateId::new("uuid-equality").unwrap(),
        "uuid = '550e8400-e29b-41d4-a716-446655440000'",
    )
    .unwrap();
    let partition = PartitionPlan {
        partition_id: PartitionId::new("clickhouse").unwrap(),
        scope: ScopeKey::Resource,
        planned_position: None,
        start_position: None,
        scan_intent: CompiledScanIntent {
            version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
            projection: None,
            predicates: vec![PushedPredicate {
                predicate,
                fidelity: PushdownFidelity::Exact,
            }],
            limit: None,
            order_by: vec![cdf_kernel::OrderBy {
                field: "uuid".to_owned(),
                direction: cdf_kernel::SortDirection::Asc,
            }],
        },
        retry_safety: PartitionRetrySafety::Forbidden,
        metadata: BTreeMap::from([
            ("kind".to_owned(), "clickhouse".to_owned()),
            ("dialect".to_owned(), "clickhouse".to_owned()),
            ("table".to_owned(), "events".to_owned()),
            (
                "resource_id".to_owned(),
                descriptor(false).resource_id.to_string(),
            ),
        ]),
    };
    let table = ClickHouseIdentifier::new("events").unwrap();
    let descriptor = descriptor(false);
    let scan = scan_from_partition(&descriptor, &schema, &table, None, &partition).unwrap();
    let query = build_query(
        &descriptor,
        &schema,
        &ClickHouseIdentifier::new("analytics").unwrap(),
        &table,
        &partition,
        &scan,
    )
    .unwrap();
    assert_eq!(
        query.sql,
        "SELECT toString(`source_uuid`) AS `uuid` FROM `analytics`.`events` WHERE throwIf(byteSize(tuple(toString(`source_uuid`))) > 16777216, 'CDF ClickHouse row exceeds bounded decode envelope') = 0 AND toString(`source_uuid`) = ? ORDER BY toString(`source_uuid`) ASC"
    );

    let physical = Schema::new_with_metadata(
        vec![with_physical_type(
            Field::new("source_uuid", DataType::Utf8, false).with_metadata(HashMap::from([(
                "catalog_marker".to_owned(),
                "physical".to_owned(),
            )])),
            "UUID",
        )],
        HashMap::from([("schema_layer".to_owned(), "physical".to_owned())]),
    );
    let projected_physical =
        project_physical_schema(&physical, &schema, &["uuid".to_owned()]).unwrap();
    assert_eq!(projected_physical.field(0).name(), "source_uuid");
    assert_eq!(
        projected_physical.field(0).metadata().get("catalog_marker"),
        Some(&"physical".to_owned())
    );
    assert_eq!(
        projected_physical.metadata().get("schema_layer"),
        Some(&"physical".to_owned())
    );
    assert_ne!(
        cdf_kernel::canonical_arrow_schema_hash(schema.as_ref()).unwrap(),
        cdf_kernel::canonical_arrow_schema_hash(&projected_physical).unwrap()
    );
    let projected_physical = Arc::new(projected_physical);

    let raw = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "uuid",
            DataType::Binary,
            false,
        )])),
        vec![Arc::new(BinaryArray::from(vec![
            b"550e8400-e29b-41d4-a716-446655440000".as_slice(),
        ]))],
    )
    .unwrap();
    let normalized = normalize_record_batch(&schema, &projected_physical, raw).unwrap();
    assert_eq!(normalized.schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        normalized
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "550e8400-e29b-41d4-a716-446655440000"
    );
}

#[test]
fn effective_physical_metadata_must_match_catalog_authority() {
    let effective = Schema::new(vec![with_physical_type(
        Field::new("observed_at", DataType::UInt64, false),
        "DateTime('UTC') + toDateTime(0) + ('UTC')",
    )]);
    let physical = Schema::new(vec![with_physical_type(
        Field::new("observed_at", DataType::UInt64, false),
        "DateTime('UTC')",
    )]);
    let error = validate_effective_physical_authority(&effective, &physical).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("differs from catalog authority"));
}

#[test]
fn query_uses_bound_values_and_canonical_cursor_tie_order() {
    let descriptor = descriptor(true);
    let schema = std::sync::Arc::new(schema());
    let predicate = ScanPredicate::new(
        PredicateId::new("minimum-sequence").unwrap(),
        "sequence >= 5",
    )
    .unwrap();
    assert_eq!(
        predicate_fidelity(&schema, &predicate.canonical_expression),
        PushdownFidelity::Exact
    );
    let mut metadata = BTreeMap::from([
        ("kind".to_owned(), "clickhouse".to_owned()),
        ("dialect".to_owned(), "clickhouse".to_owned()),
        ("table".to_owned(), "events".to_owned()),
        ("resource_id".to_owned(), descriptor.resource_id.to_string()),
        ("stable_key".to_owned(), "event_id".to_owned()),
    ]);
    metadata.insert("fixture".to_owned(), "query".to_owned());
    let partition = PartitionPlan {
        partition_id: PartitionId::new("clickhouse").unwrap(),
        scope: descriptor.state_scope.clone(),
        planned_position: None,
        start_position: Some(SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "sequence".to_owned(),
            value: CursorValue::U64(10),
        })),
        scan_intent: CompiledScanIntent {
            version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
            projection: Some(vec![
                "sequence".to_owned(),
                "event_id".to_owned(),
                "payload".to_owned(),
            ]),
            predicates: vec![PushedPredicate {
                predicate,
                fidelity: PushdownFidelity::Exact,
            }],
            limit: None,
            order_by: Vec::new(),
        },
        retry_safety: PartitionRetrySafety::Forbidden,
        metadata,
    };
    let table = ClickHouseIdentifier::new("events").unwrap();
    let stable_key = ClickHouseIdentifier::new("event_id").unwrap();
    let scan =
        scan_from_partition(&descriptor, &schema, &table, Some(&stable_key), &partition).unwrap();
    let query = build_query(
        &descriptor,
        &schema,
        &ClickHouseIdentifier::new("analytics").unwrap(),
        &table,
        &partition,
        &scan,
    )
    .unwrap();
    assert_eq!(
        query.sql,
        "SELECT `sequence` AS `sequence`, `event_id` AS `event_id`, `payload` AS `payload` FROM `analytics`.`events` WHERE throwIf(byteSize(tuple(`sequence`, `event_id`, `payload`)) > 16777216, 'CDF ClickHouse row exceeds bounded decode envelope') = 0 AND `sequence` >= ? AND `sequence` > ? ORDER BY `sequence` ASC, `event_id` ASC"
    );
    assert_eq!(query.parameters.len(), 2);
    assert!(!query.sql.contains('5'));
    assert!(!query.sql.contains("10"));
}

#[test]
fn cursor_limits_remain_engine_owned_and_decode_rows_are_schema_bounded() {
    assert_eq!(
        clickhouse_table_capabilities(&descriptor(true)).limits,
        cdf_kernel::CapabilitySupport::Unsupported
    );
    assert_eq!(
        clickhouse_table_capabilities(&descriptor(false)).limits,
        cdf_kernel::CapabilitySupport::Supported
    );
    assert_eq!(
        bounded_block_rows(&schema(), &["payload".to_owned()], 65_536).unwrap(),
        1
    );
    assert_eq!(
        bounded_block_rows(
            &schema(),
            &["sequence".to_owned(), "event_id".to_owned()],
            65_536,
        )
        .unwrap(),
        65_536
    );
    let nested = Schema::new(vec![Field::new(
        "nested",
        DataType::Struct(vec![Field::new("value", DataType::UInt8, true)].into()),
        true,
    )]);
    assert!(projection_has_variable_width(
        &nested,
        &["nested".to_owned()]
    ));
    assert_eq!(
        bounded_block_rows(&nested, &["nested".to_owned()], 65_536).unwrap(),
        1
    );
}

#[test]
fn fixed_width_block_rows_include_validity_alignment_and_reject_an_oversized_row() {
    let padded = Schema::new(vec![
        Field::new("flag", DataType::Boolean, true),
        Field::new("value", DataType::UInt64, true),
    ]);
    let rows =
        bounded_block_rows(&padded, &["flag".to_owned(), "value".to_owned()], u64::MAX).unwrap();
    assert!(rows > 0);
    assert!(rows < u64::MAX);
    let projection = ["flag".to_owned(), "value".to_owned()];
    assert!(
        fixed_projection_body_bytes(&padded, &projection, rows).unwrap()
            <= CLICKHOUSE_TARGET_ARROW_BODY_BYTES
    );
    assert!(
        fixed_projection_body_bytes(&padded, &projection, rows + 1).unwrap()
            > CLICKHOUSE_TARGET_ARROW_BODY_BYTES
    );

    let oversized = Schema::new(vec![Field::new(
        "payload",
        DataType::FixedSizeBinary(26 * 1024 * 1024),
        false,
    )]);
    let error = bounded_block_rows(&oversized, &["payload".to_owned()], 1).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("one ClickHouse Arrow row"));
}

#[test]
fn narrow_integer_cursor_widening_is_explicit_and_cursor_only() {
    let descriptor = descriptor(true);
    let widened_cursor = with_cursor_cast(
        with_physical_type(Field::new("sequence", DataType::UInt64, false), "UInt32"),
        ClickHouseCursorCast::Unsigned64,
    );
    let schema = Arc::new(Schema::new(vec![
        widened_cursor,
        Field::new("event_id", DataType::UInt64, false),
        with_physical_type(Field::new("narrow", DataType::UInt32, false), "UInt32"),
    ]));
    validate_resource_shape(
        &descriptor,
        &schema,
        &ClickHouseIdentifier::new("events").unwrap(),
        Some(&ClickHouseIdentifier::new("event_id").unwrap()),
    )
    .unwrap();

    let partition = PartitionPlan {
        partition_id: PartitionId::new("clickhouse").unwrap(),
        scope: descriptor.state_scope.clone(),
        planned_position: None,
        start_position: Some(SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "sequence".to_owned(),
            value: CursorValue::U64(10),
        })),
        scan_intent: CompiledScanIntent {
            version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
            projection: None,
            predicates: Vec::new(),
            limit: None,
            order_by: Vec::new(),
        },
        retry_safety: PartitionRetrySafety::Forbidden,
        metadata: BTreeMap::from([
            ("kind".to_owned(), "clickhouse".to_owned()),
            ("dialect".to_owned(), "clickhouse".to_owned()),
            ("table".to_owned(), "events".to_owned()),
            ("resource_id".to_owned(), descriptor.resource_id.to_string()),
            ("stable_key".to_owned(), "event_id".to_owned()),
        ]),
    };
    let table = ClickHouseIdentifier::new("events").unwrap();
    let stable_key = ClickHouseIdentifier::new("event_id").unwrap();
    let scan =
        scan_from_partition(&descriptor, &schema, &table, Some(&stable_key), &partition).unwrap();
    let query = build_query(
        &descriptor,
        &schema,
        &ClickHouseIdentifier::new("analytics").unwrap(),
        &table,
        &partition,
        &scan,
    )
    .unwrap();
    assert_eq!(
        query.sql,
        "SELECT toUInt64(`sequence`) AS `sequence`, `event_id` AS `event_id`, `narrow` AS `narrow` FROM `analytics`.`events` WHERE toUInt64(`sequence`) > ? ORDER BY toUInt64(`sequence`) ASC, `event_id` ASC"
    );

    let unmarked = Arc::new(Schema::new(vec![
        with_physical_type(Field::new("sequence", DataType::UInt64, false), "UInt32"),
        Field::new("event_id", DataType::UInt64, false),
    ]));
    let error =
        validate_resource_shape(&descriptor, &unmarked, &table, Some(&stable_key)).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("widening evidence"));
}

#[test]
fn nanosecond_cursor_is_rejected_instead_of_rounded() {
    let mut descriptor = descriptor(true);
    descriptor.cursor.as_mut().unwrap().field = "observed_at".to_owned();
    let schema = Arc::new(Schema::new(vec![
        with_physical_type(
            Field::new(
                "observed_at",
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            "DateTime64(9, 'UTC')",
        ),
        Field::new("event_id", DataType::UInt64, false),
    ]));
    let error = validate_resource_shape(
        &descriptor,
        &schema,
        &ClickHouseIdentifier::new("events").unwrap(),
        Some(&ClickHouseIdentifier::new("event_id").unwrap()),
    )
    .unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert!(error.message.contains("sub-microsecond"));
}

#[test]
fn temporal_cursor_values_preserve_clickhouse_units_exactly() {
    let identifier = ClickHouseIdentifier::new("observed_at").unwrap();
    assert_eq!(
        source_expression(&identifier, Some("Date")).unwrap(),
        "toDate32(`observed_at`)"
    );
    assert_eq!(
        source_expression(&identifier, Some("DateTime")).unwrap(),
        "toDateTime64(`observed_at`, 0)"
    );
    assert_eq!(
        source_expression(&identifier, Some("DateTime('UTC')")).unwrap(),
        "toDateTime64(`observed_at`, 0, 'UTC')"
    );
    let error = source_expression(
        &identifier,
        Some("DateTime('UTC') + toDateTime(0) + ('UTC')"),
    )
    .unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    let microsecond_field = Field::new(
        "observed_at",
        DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
        false,
    );
    let microseconds = TimestampMicrosecondArray::from(vec![-1_i64]).with_timezone("UTC");
    assert_eq!(
        cursor_value(&microsecond_field, &microseconds, 0).unwrap(),
        CursorValue::TimestampMicros {
            micros: -1,
            timezone: Some("UTC".to_owned()),
        }
    );
    let second_field = Field::new(
        "ordinary_datetime",
        DataType::Timestamp(arrow_schema::TimeUnit::Second, Some("UTC".into())),
        false,
    );
    let seconds = TimestampSecondArray::from(vec![1_785_632_523_i64]).with_timezone("UTC");
    assert_eq!(
        cursor_value(&second_field, &seconds, 0).unwrap(),
        CursorValue::TimestampMicros {
            micros: 1_785_632_523_000_000,
            timezone: Some("UTC".to_owned()),
        }
    );
    let date_field = Field::new("date", DataType::Date32, false);
    let dates = Date32Array::from(vec![20_667_i32, -3_652_i32]);
    assert_eq!(
        cursor_value(&date_field, &dates, 0).unwrap(),
        CursorValue::I64(20_667)
    );
    assert_eq!(
        cursor_value(&date_field, &dates, 1).unwrap(),
        CursorValue::I64(-3_652)
    );
}

#[test]
fn negative_date32_resume_stays_in_the_signed_date32_domain() {
    let mut descriptor = descriptor(true);
    descriptor.cursor.as_mut().unwrap().field = "observed_on".to_owned();
    let schema = Arc::new(Schema::new(vec![
        with_physical_type(Field::new("observed_on", DataType::Date32, false), "Date32"),
        Field::new("event_id", DataType::UInt64, false),
    ]));
    let partition = PartitionPlan {
        partition_id: PartitionId::new("clickhouse").unwrap(),
        scope: descriptor.state_scope.clone(),
        planned_position: None,
        start_position: Some(SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "observed_on".to_owned(),
            value: CursorValue::I64(-3_653),
        })),
        scan_intent: CompiledScanIntent {
            version: cdf_kernel::COMPILED_SCAN_INTENT_VERSION,
            projection: None,
            predicates: Vec::new(),
            limit: None,
            order_by: Vec::new(),
        },
        retry_safety: PartitionRetrySafety::Forbidden,
        metadata: BTreeMap::from([
            ("kind".to_owned(), "clickhouse".to_owned()),
            ("dialect".to_owned(), "clickhouse".to_owned()),
            ("table".to_owned(), "events".to_owned()),
            ("resource_id".to_owned(), descriptor.resource_id.to_string()),
            ("stable_key".to_owned(), "event_id".to_owned()),
        ]),
    };
    let table = ClickHouseIdentifier::new("events").unwrap();
    let stable_key = ClickHouseIdentifier::new("event_id").unwrap();
    let scan =
        scan_from_partition(&descriptor, &schema, &table, Some(&stable_key), &partition).unwrap();
    let query = build_query(
        &descriptor,
        &schema,
        &ClickHouseIdentifier::new("analytics").unwrap(),
        &table,
        &partition,
        &scan,
    )
    .unwrap();
    assert!(
        query
            .sql
            .contains("`observed_on` > addDays(toDate32('1970-01-01'), ?)")
    );
    assert!(!query.sql.contains("addDays(toDate('1970-01-01')"));
}

#[test]
fn bounded_host_stream_surfaces_failure_after_an_emitted_item() {
    let (_host, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let mut stream = execution
        .spawn_io_stream(
            "clickhouse-partial-stream-law",
            1,
            |mut sender, _| async move {
                sender.send(7_u64).await?;
                Err(cdf_kernel::CdfError::data(
                    "synthetic ClickHouse server failure after first batch",
                ))
            },
        )
        .unwrap();
    let first = futures_executor::block_on(stream.next()).unwrap().unwrap();
    assert_eq!(first, 7);
    let error = futures_executor::block_on(stream.next())
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.to_string().contains("after first batch"));
}

struct NoSecrets;

#[derive(Clone, Copy)]
struct LiveReadSettings {
    max_threads: u64,
    max_block_rows: u64,
}

impl SecretProvider for NoSecrets {
    fn resolve(&self, uri: &SecretUri) -> cdf_kernel::Result<SecretValue> {
        Err(cdf_kernel::CdfError::auth(format!(
            "live ClickHouse fixture has no secret for {uri}"
        )))
    }
}

#[test]
#[ignore = "requires digest-pinned ClickHouse via CDF_CLICKHOUSE_ENDPOINT"]
fn live_clickhouse_type_cursor_and_partial_stream_contract() {
    let endpoint = std::env::var("CDF_CLICKHOUSE_ENDPOINT")
        .expect("set CDF_CLICKHOUSE_ENDPOINT=clickhouse://host:port");
    let http_endpoint = endpoint
        .strip_prefix("clickhouse://")
        .map(|authority| format!("http://{authority}"))
        .or_else(|| {
            endpoint
                .strip_prefix("clickhouses://")
                .map(|authority| format!("https://{authority}"))
        })
        .expect("live ClickHouse endpoint must use clickhouse(s)://");
    let client = clickhouse::Client::default()
        .with_url(&http_endpoint)
        .with_database("default")
        .with_compression(clickhouse::Compression::None);
    let (host, execution) =
        cdf_engine::StandaloneExecutionHost::default_services(256 * 1024 * 1024).unwrap();

    let fixture_client = client.clone();
    execution
        .run_io(async move {
            seed_live_tables(fixture_client).await.map_err(|error| {
                cdf_kernel::CdfError::environment(format!(
                    "prepare live ClickHouse source fixture: {error}"
                ))
            })
        })
        .unwrap();

    let connection = ClickHouseConnection::new(
        http_endpoint,
        ClickHouseIdentifier::new("default").unwrap(),
        None,
        None,
        2,
        2,
    );
    let egress = SourceEgressScope::new(
        SourceDriverId::new("clickhouse").unwrap(),
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let discovered = execution
        .run_io(discover_clickhouse_table(
            connection.clone(),
            ResourceId::new("live.types").unwrap(),
            ClickHouseIdentifier::new("cdf_source_live_types").unwrap(),
            None,
            execution.memory(),
            egress.clone(),
            RunCancellation::default(),
        ))
        .unwrap();
    let expected_physical_types = [
        "Bool",
        "Int8",
        "Int16",
        "Int32",
        "Int64",
        "UInt8",
        "UInt16",
        "UInt32",
        "UInt64",
        "Float32",
        "Float64",
        "Decimal(9, 2)",
        "Decimal(18, 4)",
        "Decimal(38, 6)",
        "Decimal(76, 8)",
        "String",
        "FixedString(4)",
        "Date",
        "Date32",
        "DateTime",
        "DateTime('UTC')",
        "DateTime64(6, 'UTC')",
        "Array(Int32)",
        "Tuple(UInt64, String)",
        "Map(String, UInt64)",
        "Nullable(Int32)",
        "LowCardinality(String)",
        "Enum8('ready' = 1, 'done' = 2)",
        "IPv4",
        "IPv6",
        "UUID",
    ];
    assert_eq!(
        discovered.schema.fields().len(),
        expected_physical_types.len()
    );
    for (field, expected) in discovered
        .schema
        .fields()
        .iter()
        .zip(expected_physical_types)
    {
        assert_eq!(physical_type(field), Some(expected));
    }

    let unsupported = execution
        .run_io(discover_clickhouse_table(
            connection.clone(),
            ResourceId::new("live.dynamic").unwrap(),
            ClickHouseIdentifier::new("cdf_source_live_dynamic").unwrap(),
            None,
            execution.memory(),
            egress.clone(),
            RunCancellation::default(),
        ))
        .unwrap_err();
    assert_eq!(unsupported.kind, cdf_kernel::ErrorKind::Data);
    assert!(unsupported.message.contains("unstable"));
    assert!(unsupported.message.contains("Dynamic"));

    let wrapped = execution
        .run_io(discover_clickhouse_table(
            connection,
            ResourceId::new("live.wrapped").unwrap(),
            ClickHouseIdentifier::new("cdf_source_live_wrapped").unwrap(),
            None,
            execution.memory(),
            egress,
            RunCancellation::default(),
        ))
        .unwrap_err();
    assert_eq!(wrapped.kind, cdf_kernel::ErrorKind::Data);
    assert!(wrapped.message.contains("nested_uuid"));
    assert!(wrapped.message.contains("Array(UUID)"));
    assert!(wrapped.message.contains("cannot be normalized exactly"));

    let context = SourceResolutionContext::new(
        Path::new("."),
        Arc::new(NoSecrets),
        &execution,
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let type_resource = live_resource(
        &endpoint,
        "cdf_source_live_types",
        descriptor(false),
        discovered.schema.clone(),
        None,
        LiveReadSettings {
            max_threads: 2,
            max_block_rows: 128,
        },
        &context,
    );
    let type_batch = host
        .block_on_root(read_single_batch(type_resource.as_ref()))
        .unwrap();
    assert_eq!(type_batch.schema().as_ref(), &discovered.schema);
    let displayed = type_batch
        .columns()
        .iter()
        .map(|column| {
            if column.is_null(0) {
                Ok(None)
            } else {
                arrow_cast::display::array_value_to_string(column.as_ref(), 0).map(Some)
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(displayed[0].as_deref(), Some("true"));
    assert_eq!(
        &displayed[1..9],
        &["-1", "-2", "-3", "-4", "1", "2", "3", "4"].map(|value| Some(value.to_owned()))
    );
    assert_eq!(displayed[9].as_deref(), Some("1.5"));
    assert_eq!(displayed[10].as_deref(), Some("2.5"));
    assert_eq!(
        &displayed[11..15],
        &["12.34", "1234.5678", "123456.123456", "12345678.12345678"]
            .map(|value| Some(value.to_owned()))
    );
    assert_eq!(displayed[15].as_deref(), Some("6279746573"));
    assert_eq!(displayed[16].as_deref(), Some("61626364"));
    assert_eq!(type_batch.schema().field(17).data_type(), &DataType::Date32);
    let Some(date) = type_batch.column(17).as_any().downcast_ref::<Date32Array>() else {
        panic!("ClickHouse Date must use its promoted Arrow Date32 representation");
    };
    assert_eq!(date.value(0), 20_667);
    assert_eq!(type_batch.schema().field(18).data_type(), &DataType::Date32);
    let Some(date32) = type_batch.column(18).as_any().downcast_ref::<Date32Array>() else {
        panic!("ClickHouse Date32 must retain its Arrow Date32 representation");
    };
    assert_eq!(date32.value(0), -3_652);
    assert_eq!(
        type_batch.schema().field(19).data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Second, Some("UTC".into()))
    );
    let Some(datetime_bare) = type_batch
        .column(19)
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
    else {
        panic!("bare ClickHouse DateTime must use its promoted Arrow timestamp representation");
    };
    assert_eq!(datetime_bare.value(0), 1_785_632_523);
    assert_eq!(
        type_batch.schema().field(20).data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Second, Some("UTC".into()))
    );
    let Some(datetime) = type_batch
        .column(20)
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
    else {
        panic!("zoned ClickHouse DateTime must use its promoted Arrow timestamp representation");
    };
    assert_eq!(datetime.value(0), 1_785_632_523);
    assert_eq!(
        type_batch.schema().field(21).data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
    );
    let Some(datetime64) = type_batch
        .column(21)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
    else {
        panic!("ClickHouse DateTime64 must retain its Arrow timestamp representation");
    };
    assert_eq!(datetime64.value(0), 1_785_632_523_123_456);
    let expected_complex_types = [
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
        DataType::Struct(
            vec![
                Field::new("1", DataType::UInt64, false),
                Field::new("2", DataType::Binary, false),
            ]
            .into(),
        ),
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Binary, false),
                        Field::new("value", DataType::UInt64, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        DataType::Int32,
        DataType::Binary,
        DataType::Int8,
        DataType::UInt32,
        DataType::FixedSizeBinary(16),
        DataType::Utf8,
    ];
    for (index, expected) in (22..31).zip(expected_complex_types) {
        assert_eq!(type_batch.schema().field(index).data_type(), &expected);
        assert_eq!(type_batch.schema().field(index).is_nullable(), index == 25);
    }
    assert_eq!(
        &displayed[22..31],
        &[
            Some("[1, 2]".to_owned()),
            Some("{1: 7, 2: 7475706c65}".to_owned()),
            Some("{6b6579: 9}".to_owned()),
            None,
            Some("6c6f77".to_owned()),
            Some("1".to_owned()),
            Some("2130706433".to_owned()),
            Some("00000000000000000000000000000001".to_owned()),
            Some("550e8400-e29b-41d4-a716-446655440000".to_owned()),
        ]
    );
    let string = type_batch
        .column(15)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    assert_eq!(string.value(0), b"bytes");
    let uuid = type_batch
        .column(30)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(uuid.value(0), "550e8400-e29b-41d4-a716-446655440000");

    let cursor_resource = live_resource(
        &endpoint,
        "cdf_source_live_cursor",
        descriptor(true),
        Schema::new(vec![
            Field::new("sequence", DataType::UInt64, false),
            Field::new("event_id", DataType::UInt64, false),
            Field::new("narrow", DataType::UInt32, false),
            Field::new("payload", DataType::Binary, false),
        ]),
        Some("event_id"),
        LiveReadSettings {
            max_threads: 2,
            max_block_rows: 2,
        },
        &context,
    );
    let cursor_schema = cursor_resource.schema();
    assert_eq!(physical_type(cursor_schema.field(0)), Some("UInt32"));
    assert_eq!(
        cursor_schema
            .field(0)
            .metadata()
            .get(CLICKHOUSE_CURSOR_CAST_METADATA_KEY),
        Some(&"unsigned64".to_owned())
    );
    assert_eq!(cursor_schema.field(2).data_type(), &DataType::UInt32);
    assert_eq!(physical_type(cursor_schema.field(2)), Some("UInt32"));
    assert!(
        cursor_schema
            .field(2)
            .metadata()
            .get(CLICKHOUSE_CURSOR_CAST_METADATA_KEY)
            .is_none()
    );
    let (ids, position) = host
        .block_on_root(read_resource(cursor_resource.as_ref()))
        .unwrap();
    assert_eq!(ids, vec![1, 2, 3, 4]);
    assert_eq!(
        position,
        Some(SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "sequence".to_owned(),
            value: CursorValue::U64(20),
        }))
    );

    let mut date_descriptor = descriptor(true);
    date_descriptor.resource_id = ResourceId::new("live.date_cursor").unwrap();
    date_descriptor.cursor.as_mut().unwrap().field = "observed_on".to_owned();
    let date_resource = live_resource(
        &endpoint,
        "cdf_source_live_date_cursor",
        date_descriptor,
        Schema::new(vec![
            Field::new("observed_on", DataType::Date32, false),
            Field::new("event_id", DataType::UInt64, false),
        ]),
        Some("event_id"),
        LiveReadSettings {
            max_threads: 1,
            max_block_rows: 2,
        },
        &context,
    );
    let (date_ids, date_position) = host
        .block_on_root(read_resource_from(
            date_resource.as_ref(),
            Some(SourcePosition::Cursor(CursorPosition {
                version: 1,
                field: "observed_on".to_owned(),
                value: CursorValue::I64(-3_653),
            })),
        ))
        .unwrap();
    assert_eq!(date_ids, vec![2, 3]);
    assert_eq!(
        date_position,
        Some(SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "observed_on".to_owned(),
            value: CursorValue::I64(0),
        }))
    );

    let partial_schema = Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("payload", DataType::Binary, false),
        Field::new("fault", DataType::UInt8, false),
    ]);
    let partial_resource = live_resource(
        &endpoint,
        "cdf_source_live_partial",
        descriptor(false),
        partial_schema,
        None,
        LiveReadSettings {
            max_threads: 1,
            max_block_rows: 1,
        },
        &context,
    );
    let (rows_before_error, error) = host
        .block_on_root(read_until_error(partial_resource.as_ref()))
        .unwrap();
    assert!(rows_before_error > 0, "fixture must emit before failing");
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);

    let wide_resource = live_resource(
        &endpoint,
        "cdf_source_live_wide",
        descriptor(false),
        Schema::new(vec![Field::new("payload", DataType::Binary, false)]),
        None,
        LiveReadSettings {
            max_threads: 1,
            max_block_rows: 1,
        },
        &context,
    );
    let (rows_before_error, error) = host
        .block_on_root(read_until_error(wide_resource.as_ref()))
        .unwrap();
    assert_eq!(rows_before_error, 0);
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("server rejected"));
}

fn live_resource(
    endpoint: &str,
    table: &str,
    descriptor: ResourceDescriptor,
    expected_schema: Schema,
    stable_key: Option<&str>,
    settings: LiveReadSettings,
    context: &SourceResolutionContext<'_>,
) -> Arc<dyn QueryableResource> {
    let driver = ClickHouseSourceDriver::new().unwrap();
    let table_identifier = ClickHouseIdentifier::new(table).unwrap();
    let connection = ClickHouseConnection::new(
        endpoint
            .strip_prefix("clickhouse://")
            .map(|authority| format!("http://{authority}"))
            .or_else(|| {
                endpoint
                    .strip_prefix("clickhouses://")
                    .map(|authority| format!("https://{authority}"))
            })
            .unwrap(),
        ClickHouseIdentifier::new("default").unwrap(),
        None,
        None,
        settings.max_threads,
        settings.max_block_rows,
    );
    let egress = SourceEgressScope::new(
        SourceDriverId::new("clickhouse").unwrap(),
        Arc::new(cdf_http::EgressAllowlist::allow_any()),
    );
    let cursor_field = descriptor
        .cursor
        .as_ref()
        .map(|cursor| cursor.field.clone());
    let observed = context
        .execution()
        .run_io(discover_clickhouse_table(
            connection,
            descriptor.resource_id.clone(),
            table_identifier,
            cursor_field,
            context.execution().memory(),
            egress,
            RunCancellation::default(),
        ))
        .unwrap();
    assert_eq!(
        observed
            .schema
            .fields()
            .iter()
            .map(|field| (field.name(), field.data_type(), field.is_nullable()))
            .collect::<Vec<_>>(),
        expected_schema
            .fields()
            .iter()
            .map(|field| (field.name(), field.data_type(), field.is_nullable()))
            .collect::<Vec<_>>()
    );
    let schema = observed.schema;
    let physical_schema = Arc::new(schema.clone());
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let baseline_observation_schema_catalog = vec![cdf_kernel::EffectiveSchemaCatalogEntry::new(
        physical_hash.clone(),
        Arc::clone(&physical_schema),
    )];
    let effective_schema_runtime = cdf_kernel::EffectiveSchemaRuntime::new(
        cdf_kernel::EffectiveSchemaEvidence::new(
            descriptor.schema_source.baseline_reference().unwrap(),
            physical_hash.clone(),
            cdf_kernel::DiscoveryManifestReference {
                manifest_hash: cdf_kernel::DiscoveryManifestHash::new(
                    "test-clickhouse-live-discovery-manifest",
                )
                .unwrap(),
                path: ".cdf/discovery/test-clickhouse-live.json".to_owned(),
            },
            vec![cdf_kernel::EffectiveSchemaObservationEvidence::new(
                format!("default.{table}"),
                physical_hash.clone(),
                cdf_kernel::SchemaObservationBinding::new(format!("sha256:{}", "0".repeat(64)))
                    .unwrap(),
            )],
        )
        .unwrap(),
        baseline_observation_schema_catalog.clone(),
    )
    .unwrap();
    let mut resource_options = BTreeMap::from([("table".to_owned(), serde_json::json!(table))]);
    if let Some(stable_key) = stable_key {
        resource_options.insert("stable_key".to_owned(), serde_json::json!(stable_key));
    }
    let plan = driver
        .compile(SourceCompileRequest {
            source_kind: "clickhouse".to_owned(),
            context: cdf_runtime::SourceCompileContext {
                source_name: "live".to_owned(),
                project_root: None,
                cursor_pushdown: None,
            },
            source_options: BTreeMap::from([
                ("endpoint".to_owned(), serde_json::json!(endpoint)),
                ("database".to_owned(), serde_json::json!("default")),
                (
                    "max_threads".to_owned(),
                    serde_json::json!(settings.max_threads),
                ),
                (
                    "max_block_rows".to_owned(),
                    serde_json::json!(settings.max_block_rows),
                ),
                ("stream_buffer_batches".to_owned(), serde_json::json!(1)),
            ]),
            resource_options,
            descriptor,
            schema,
            type_policy_allowances: Default::default(),
            effective_schema_runtime: Some(effective_schema_runtime),
            baseline_observation_schema_catalog,
        })
        .unwrap();
    driver.resolve(&plan, context).unwrap()
}

async fn read_resource(
    resource: &dyn QueryableResource,
) -> cdf_kernel::Result<(Vec<u64>, Option<SourcePosition>)> {
    read_resource_from(resource, None).await
}

async fn read_resource_from(
    resource: &dyn QueryableResource,
    start_position: Option<SourcePosition>,
) -> cdf_kernel::Result<(Vec<u64>, Option<SourcePosition>)> {
    let scan = resource.negotiate(&ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: resource.descriptor().state_scope.clone(),
    })?;
    let mut partition = scan
        .inline_partitions()
        .and_then(|partitions| partitions.first())
        .cloned()
        .ok_or_else(|| cdf_kernel::CdfError::internal("live ClickHouse scan has no partition"))?;
    partition.start_position = start_position;
    bind_live_planned_physical_schema(resource, &mut partition)?;
    let mut stream = resource.open(partition).await?;
    let mut ids = Vec::new();
    let mut position = None;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let record_batch = batch.record_batch().ok_or_else(|| {
            cdf_kernel::CdfError::internal("live ClickHouse source emitted no Arrow batch")
        })?;
        let values = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| cdf_kernel::CdfError::data("live event_id was not UInt64"))?;
        ids.extend((0..values.len()).map(|row| values.value(row)));
        position = batch.header.source_position.clone();
    }
    stream.completion().await?;
    Ok((ids, position))
}

async fn read_single_batch(resource: &dyn QueryableResource) -> cdf_kernel::Result<RecordBatch> {
    let scan = resource.negotiate(&ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: resource.descriptor().state_scope.clone(),
    })?;
    let mut partition = scan
        .inline_partitions()
        .and_then(|partitions| partitions.first())
        .cloned()
        .ok_or_else(|| cdf_kernel::CdfError::internal("live type scan has no partition"))?;
    bind_live_planned_physical_schema(resource, &mut partition)?;
    let mut stream = resource.open(partition).await?;
    let batch = stream
        .next()
        .await
        .ok_or_else(|| cdf_kernel::CdfError::data("live type scan returned no batch"))??;
    if stream.next().await.is_some() {
        return Err(cdf_kernel::CdfError::data(
            "single-row live type scan returned more than one batch",
        ));
    }
    stream.completion().await?;
    batch
        .record_batch()
        .cloned()
        .ok_or_else(|| cdf_kernel::CdfError::internal("live type source emitted no Arrow batch"))
}

async fn read_until_error(
    resource: &dyn QueryableResource,
) -> cdf_kernel::Result<(usize, cdf_kernel::CdfError)> {
    let scan = resource.negotiate(&ScanRequest {
        resource_id: resource.descriptor().resource_id.clone(),
        projection: None,
        filters: Vec::new(),
        limit: None,
        order_by: Vec::new(),
        scope: resource.descriptor().state_scope.clone(),
    })?;
    let mut partition = scan
        .inline_partitions()
        .and_then(|partitions| partitions.first())
        .cloned()
        .ok_or_else(|| cdf_kernel::CdfError::internal("live partial scan has no partition"))?;
    bind_live_planned_physical_schema(resource, &mut partition)?;
    let mut stream = resource.open(partition).await?;
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        match batch {
            Ok(batch) => {
                rows += batch
                    .record_batch()
                    .ok_or_else(|| {
                        cdf_kernel::CdfError::internal(
                            "live partial ClickHouse source emitted no Arrow batch",
                        )
                    })?
                    .num_rows();
            }
            Err(error) => return Ok((rows, error)),
        }
    }
    Err(cdf_kernel::CdfError::data(
        "live partial ClickHouse fixture completed instead of failing",
    ))
}

fn bind_live_planned_physical_schema(
    resource: &dyn QueryableResource,
    partition: &mut PartitionPlan,
) -> cdf_kernel::Result<()> {
    let runtime = resource.effective_schema_runtime().ok_or_else(|| {
        cdf_kernel::CdfError::internal("live ClickHouse resource omitted effective-schema runtime")
    })?;
    let observation_id = cdf_kernel::partition_schema_observation_id(partition);
    let physical_hash = runtime
        .evidence
        .observation(observation_id)
        .ok_or_else(|| {
            cdf_kernel::CdfError::internal(
                "live ClickHouse partition omitted its effective-schema observation",
            )
        })?
        .physical_schema_hash
        .to_string();
    partition.metadata.insert(
        cdf_kernel::PLAN_PHYSICAL_SCHEMA_HASH_KEY.to_owned(),
        physical_hash,
    );
    Ok(())
}

async fn seed_live_tables(client: clickhouse::Client) -> clickhouse::error::Result<()> {
    for table in [
        "cdf_source_live_types",
        "cdf_source_live_dynamic",
        "cdf_source_live_cursor",
        "cdf_source_live_date_cursor",
        "cdf_source_live_wrapped",
        "cdf_source_live_partial",
        "cdf_source_live_wide",
    ] {
        client
            .query(&format!("DROP TABLE IF EXISTS `{table}`"))
            .execute()
            .await?;
    }
    client
        .query(concat!(
            "CREATE TABLE cdf_source_live_types (",
            "b Bool, i8 Int8, i16 Int16, i32 Int32, i64 Int64, ",
            "u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64, ",
            "f32 Float32, f64 Float64, d32 Decimal32(2), d64 Decimal64(4), ",
            "d128 Decimal128(6), d256 Decimal256(8), s String, fixed FixedString(4), ",
            "d Date, dwide Date32, dt_bare DateTime, dt DateTime('UTC'), ",
            "dt64 DateTime64(6, 'UTC'), ",
            "arr Array(Int32), tup Tuple(UInt64, String), mp Map(String, UInt64), ",
            "nullable Nullable(Int32), low LowCardinality(String), ",
            "state Enum8('ready' = 1, 'done' = 2), ip4 IPv4, ip6 IPv6, uuid UUID",
            ") ENGINE = Memory"
        ))
        .execute()
        .await?;
    client
        .query(concat!(
            "CREATE TABLE cdf_source_live_date_cursor (",
            "observed_on Date32, event_id UInt64",
            ") ENGINE = Memory"
        ))
        .execute()
        .await?;
    client
        .query(concat!(
            "INSERT INTO cdf_source_live_date_cursor VALUES ",
            "('1959-12-31', 1), ('1960-01-02', 2), ('1970-01-01', 3)"
        ))
        .execute()
        .await?;
    client
        .query(concat!(
            "INSERT INTO cdf_source_live_types VALUES (",
            "true, -1, -2, -3, -4, 1, 2, 3, 4, 1.5, 2.5, ",
            "12.34, 1234.5678, 123456.123456, 12345678.12345678, ",
            "'bytes', 'abcd', '2026-08-02', '1960-01-02', ",
            "'2026-08-02 01:02:03', '2026-08-02 01:02:03', ",
            "'2026-08-02 01:02:03.123456', ",
            "[1, 2], (7, 'tuple'), map('key', 9), NULL, 'low', 'ready', ",
            "'127.0.0.1', '::1', '550e8400-e29b-41d4-a716-446655440000')"
        ))
        .execute()
        .await?;
    client
        .query("CREATE TABLE cdf_source_live_dynamic (id UInt64, unstable Dynamic) ENGINE = Memory")
        .execute()
        .await?;
    client
        .query(
            "CREATE TABLE cdf_source_live_wrapped (id UInt64, nested_uuid Array(UUID)) ENGINE = Memory",
        )
        .execute()
        .await?;
    client
        .query(concat!(
            "CREATE TABLE cdf_source_live_cursor (",
            "sequence UInt32, event_id UInt64, narrow UInt32, payload String",
            ") ENGINE = Memory"
        ))
        .execute()
        .await?;
    client
        .query(concat!(
            "INSERT INTO cdf_source_live_cursor VALUES ",
            "(10, 1, 100, 'a'), (10, 2, 101, 'b'), ",
            "(20, 3, 102, 'c'), (20, 4, 103, 'd')"
        ))
        .execute()
        .await?;
    // This is the official clickhouse 0.15.1 deferred-response error law adapted to a table target:
    // one row per stored/read block, sleepEachRow(0.03), then fail on row four. Three separately
    // stored 4 MiB random binary payloads cross the HTTP and Arrow streaming buffers without relying
    // on their values. `wait_end_of_query` remains intentionally unset. If a server starts buffering
    // whole results, this law fails instead of weakening partial-stream propagation.
    client
        .query(concat!(
            "CREATE TABLE cdf_source_live_partial (",
            "event_id UInt64, payload String, ",
            "fault UInt8 ALIAS if(sleepEachRow(0.03) = 0, ",
            "throwIf(event_id >= 3, 'cdf live fixture'), 0)",
            ") ENGINE = TinyLog"
        ))
        .execute()
        .await?;
    for event_id in 0..3 {
        client
            .query(&format!(
                "INSERT INTO cdf_source_live_partial (event_id, payload) SELECT {event_id}, randomString(4194304)"
            ))
            .execute()
            .await?;
    }
    client
        .query("INSERT INTO cdf_source_live_partial (event_id, payload) SELECT 3, randomString(1)")
        .execute()
        .await?;
    client
        .query("CREATE TABLE cdf_source_live_wide (payload String) ENGINE = Memory")
        .execute()
        .await?;
    client
        .query("INSERT INTO cdf_source_live_wide SELECT randomString(20971520)")
        .execute()
        .await
}
