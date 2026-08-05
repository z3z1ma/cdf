use std::{collections::BTreeMap, str::FromStr, sync::Arc};

use arrow_array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_kernel::{
    CompiledScanIntent, CursorPosition, CursorSpec, CursorValue, PartitionId, PartitionPlan,
    PartitionRetrySafety, ResourceDescriptor, ResourceId, SchemaHash, SchemaSource, ScopeKey,
    SourcePosition, TrustLevel, WriteDisposition, with_semantic, with_source_name,
};
use cdf_runtime::{SourceCompileRequest, SourceDriver, SourceExecutorClass};
use mongodb::bson::{
    DateTime, Decimal128, doc,
    oid::ObjectId,
    raw::{RawDocumentBuf, cstr},
};

use crate::{
    MongoDbSourceDriver,
    error::classify_mongodb_error,
    identifier::{MongoDbIdentifier, validate_field_path},
    query::{build_query, scan_from_partition},
    schema::{
        MONGODB_DECIMAL_TEXT_SEMANTIC, MONGODB_OBJECT_ID_SEMANTIC, SchemaInference, decode_batch,
    },
};

fn descriptor(cursor: bool) -> ResourceDescriptor {
    ResourceDescriptor {
        resource_id: ResourceId::new("warehouse.events").unwrap(),
        schema_source: SchemaSource::Declared {
            schema_hash: SchemaHash::new("schema-mongodb-tests").unwrap(),
            source: "mongodb://warehouse/events".to_owned(),
        },
        primary_key: vec!["id".to_owned()],
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

fn semantic_field(field: Field, reference: &str) -> Field {
    with_semantic(field, &reference.parse().unwrap())
}

fn schema() -> Schema {
    Schema::new(vec![
        with_source_name(
            semantic_field(
                Field::new("id", DataType::FixedSizeBinary(12), false),
                MONGODB_OBJECT_ID_SEMANTIC,
            ),
            "_id",
        ),
        Field::new("sequence", DataType::Int64, false),
        semantic_field(
            Field::new("amount", DataType::Utf8, true),
            MONGODB_DECIMAL_TEXT_SEMANTIC,
        ),
        Field::new(
            "observed_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
    ])
}

#[test]
fn compile_is_contact_free_redacted_and_io_owned() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let plan = driver
        .compile(SourceCompileRequest {
            source_kind: "mongodb".to_owned(),
            context: cdf_runtime::SourceCompileContext {
                source_name: "warehouse".to_owned(),
                project_root: None,
                cursor_pushdown: None,
            },
            source_options: BTreeMap::from([
                (
                    "endpoint".to_owned(),
                    serde_json::json!("mongodb://warehouse.example:27017"),
                ),
                ("database".to_owned(), serde_json::json!("analytics")),
                (
                    "username".to_owned(),
                    serde_json::json!("secret://env/MONGODB_USER"),
                ),
                (
                    "password".to_owned(),
                    serde_json::json!("secret://env/MONGODB_PASSWORD"),
                ),
            ]),
            resource_options: BTreeMap::from([(
                "collection".to_owned(),
                serde_json::json!("events"),
            )]),
            descriptor: descriptor(true),
            schema: schema(),
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        })
        .unwrap();

    assert_eq!(plan.driver.driver_id.as_str(), "mongodb");
    assert_eq!(
        plan.execution_capabilities.executor_class,
        SourceExecutorClass::Io
    );
    assert!(plan.execution_capabilities.blocking_lane.is_none());
    assert_eq!(plan.execution_capabilities.maximum_concurrency, 1);
    let encoded = serde_json::to_string(&plan).unwrap();
    assert!(encoded.contains("secret://env/MONGODB_PASSWORD"));
    assert!(!encoded.contains("inline-password"));
    driver.validate_portable_plan(&plan).unwrap();
}

#[test]
fn compile_rejects_credentials_in_endpoint_and_unknown_options() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let request = |endpoint: &str, extra: bool| {
        let mut source_options = BTreeMap::from([
            ("endpoint".to_owned(), serde_json::json!(endpoint)),
            ("database".to_owned(), serde_json::json!("analytics")),
        ]);
        if extra {
            source_options.insert("legacy_mode".to_owned(), serde_json::json!(true));
        }
        SourceCompileRequest {
            source_kind: "mongodb".to_owned(),
            context: cdf_runtime::SourceCompileContext {
                source_name: "warehouse".to_owned(),
                project_root: None,
                cursor_pushdown: None,
            },
            source_options,
            resource_options: BTreeMap::from([(
                "collection".to_owned(),
                serde_json::json!("events"),
            )]),
            descriptor: descriptor(false),
            schema: schema(),
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        }
    };

    let error = driver
        .compile(request(
            "mongodb://user:inline-password@localhost:27017",
            false,
        ))
        .unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert!(error.message.contains("credential-free"));
    assert!(!error.message.contains("inline-password"));

    let error = driver
        .compile(request("mongodb://localhost:27017", true))
        .unwrap_err();
    assert!(error.message.contains("unknown field `legacy_mode`"));
}

#[test]
fn discovery_infers_exact_bson_shapes_and_nested_missing_fields() {
    let object_id = ObjectId::parse_str("64b64c27f6f1a00f92d66c6a").unwrap();
    let decimal = Decimal128::from_str("1234567890.0123456789").unwrap();
    let first = RawDocumentBuf::try_from(&doc! {
        "_id": object_id,
        "sequence": 1_i32,
        "amount": decimal,
        "observed_at": DateTime::from_millis(1_725_000_000_123),
        "nested": {"left": 1_i64},
        "tags": ["one", "two"],
    })
    .unwrap();
    let second = RawDocumentBuf::try_from(&doc! {
        "_id": object_id,
        "sequence": 2_i64,
        "amount": Decimal128::from_str("NaN").unwrap(),
        "observed_at": DateTime::from_millis(1_725_000_001_123),
        "nested": {"right": true},
        "tags": ["three"],
    })
    .unwrap();
    let mut inference = SchemaInference::default();
    inference.observe(&first).unwrap();
    inference.observe(&second).unwrap();
    let (schema, records, bytes) = inference.finish().unwrap();

    assert_eq!(records, 2);
    assert_eq!(
        bytes,
        (first.as_bytes().len() + second.as_bytes().len()) as u64
    );
    assert_eq!(
        schema.field_with_name("sequence").unwrap().data_type(),
        &DataType::Int64
    );
    assert_eq!(
        schema.field_with_name("_id").unwrap().metadata()["cdf:semantic"],
        MONGODB_OBJECT_ID_SEMANTIC
    );
    let amount = schema.field_with_name("amount").unwrap();
    assert_eq!(amount.data_type(), &DataType::Utf8);
    assert_eq!(
        amount.metadata()["cdf:semantic"],
        MONGODB_DECIMAL_TEXT_SEMANTIC
    );
    let DataType::Struct(fields) = schema.field_with_name("nested").unwrap().data_type() else {
        panic!("nested document did not infer as a struct");
    };
    assert!(fields.iter().all(|field| field.is_nullable()));
}

#[test]
fn raw_decoder_preserves_object_id_decimal_and_cursor_types() {
    let object_id = ObjectId::parse_str("64b64c27f6f1a00f92d66c6a").unwrap();
    let document = RawDocumentBuf::try_from(&doc! {
        "_id": object_id,
        "sequence": 42_i32,
        "amount": Decimal128::from_str("-12.3400").unwrap(),
        "observed_at": DateTime::from_millis(1_725_000_000_123),
    })
    .unwrap();
    let schema = Arc::new(schema());
    let batch = decode_batch(Arc::clone(&schema), &[document.as_ref()]).unwrap();

    assert_eq!(batch.num_rows(), 1);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap()
            .value(0),
        object_id.bytes()
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        42
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "-12.3400"
    );

    let drifted = RawDocumentBuf::try_from(&doc! {
        "_id": object_id,
        "sequence": "wrong",
        "amount": Decimal128::from_str("1").unwrap(),
        "observed_at": DateTime::from_millis(0),
    })
    .unwrap();
    let error = decode_batch(schema, &[drifted.as_ref()]).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("expected Int64"));
}

#[test]
fn duplicate_bson_keys_fail_instead_of_selecting_one_value() {
    let mut duplicate = RawDocumentBuf::new();
    duplicate.append(cstr!("sequence"), 1_i64);
    duplicate.append(cstr!("sequence"), 2_i64);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "sequence",
        DataType::Int64,
        false,
    )]));

    let error = decode_batch(schema, &[duplicate.as_ref()]).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("repeats field `sequence`"));
}

#[test]
fn cursor_query_uses_numeric_frontier_and_object_id_tie_breaker() {
    let descriptor = descriptor(true);
    let schema = Arc::new(schema());
    let partition = PartitionPlan {
        partition_id: PartitionId::new("mongodb").unwrap(),
        scope: descriptor.state_scope.clone(),
        planned_position: None,
        start_position: Some(SourcePosition::Cursor(CursorPosition {
            version: cdf_kernel::SOURCE_POSITION_VERSION,
            field: "sequence".to_owned(),
            value: CursorValue::I64(41),
        })),
        scan_intent: CompiledScanIntent::full_scan(),
        retry_safety: PartitionRetrySafety::Forbidden,
        metadata: BTreeMap::from([
            ("kind".to_owned(), "mongodb".to_owned()),
            ("resource_id".to_owned(), descriptor.resource_id.to_string()),
            ("collection".to_owned(), "events".to_owned()),
        ]),
    };
    let collection = MongoDbIdentifier::new("events").unwrap();
    let scan = scan_from_partition(&descriptor, &schema, &collection, &partition).unwrap();
    let query = build_query(&descriptor, &schema, &partition, &scan).unwrap();

    assert_eq!(query.filter, doc! {"sequence": {"$gt": 41_i64}});
    assert_eq!(query.sort, doc! {"sequence": 1_i32, "_id": 1_i32});
    assert_eq!(query.limit, None);
}

#[test]
fn identifiers_and_field_paths_reject_injection_fragments() {
    assert!(MongoDbIdentifier::new("events").is_ok());
    assert!(MongoDbIdentifier::new("system.users").is_err());
    assert!(MongoDbIdentifier::new("bad/name").is_err());
    assert!(validate_field_path("nested.value").is_ok());
    assert!(validate_field_path("$where").is_err());
    assert!(validate_field_path("nested..value").is_err());
}

#[test]
fn sdk_wrapper_preserves_typed_error_ownership_and_retry_delay() {
    let expected = cdf_kernel::CdfError::rate_limited("provider quota", Some(250));
    let direct = classify_mongodb_error(
        "read collection",
        mongodb::error::Error::custom(expected.clone()),
    );
    assert_eq!(direct.kind, cdf_kernel::ErrorKind::RateLimited);
    assert_eq!(direct.retry_after_ms, Some(250));
    assert!(direct.message.contains("provider quota"));

    let nested = std::io::Error::other(std::io::Error::other(expected));
    let nested = classify_mongodb_error("read collection", nested.into());
    assert_eq!(nested.kind, cdf_kernel::ErrorKind::RateLimited);
    assert_eq!(nested.retry_after_ms, Some(250));

    let truncated = classify_mongodb_error(
        "read collection",
        std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into(),
    );
    assert_eq!(truncated.kind, cdf_kernel::ErrorKind::Data);
    let denied = classify_mongodb_error(
        "read collection",
        std::io::Error::from(std::io::ErrorKind::PermissionDenied).into(),
    );
    assert_eq!(denied.kind, cdf_kernel::ErrorKind::Environment);
}
