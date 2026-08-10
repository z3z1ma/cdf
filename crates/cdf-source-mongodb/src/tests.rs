use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
};

use arrow_array::{
    Array, Decimal128Array, FixedSizeBinaryArray, Int64Array, ListArray, StringArray, StructArray,
    TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use cdf_contract::{
    CDF_VARIANT_SEMANTIC, RESIDUAL_ENCODING_METADATA_KEY, RESIDUAL_ENCODING_NAME,
    VARIANT_COLUMN_NAME,
};
use cdf_kernel::{
    CompiledScanIntent, CursorPosition, CursorSpec, CursorValue, PartitionId, PartitionPlan,
    PartitionRetrySafety, ResourceDescriptor, ResourceId, SEMANTIC_METADATA_KEY, SchemaHash,
    SchemaSource, ScopeKey, SourcePosition, TrustLevel, WriteDisposition, physical_type,
    with_physical_type, with_semantic, with_source_name,
};
use cdf_runtime::{SourceAddRequest, SourceCompileRequest, SourceDriver, SourceExecutorClass};
use mongodb::bson::{
    Binary, DateTime, Decimal128, Document, doc,
    oid::ObjectId,
    raw::{CString, RawDocumentBuf, RawJavaScriptCodeWithScope, cstr},
    spec::BinarySubtype,
};

use crate::{
    MongoDbSourceDriver,
    driver::{
        collection_metadata_from_response, compiled_database_inventory, validate_server_version,
        with_required_cdc_id,
    },
    error::classify_mongodb_error,
    execution::{
        MONGODB_FULL_SCAN_COMPLETION_PROTOCOL, cursor_value, full_scan_completion_position,
    },
    identifier::{MongoDbIdentifier, validate_field_path},
    native::{MongoDbNativeExtraction, MongoDbNativeResourceOptions, MongoDbReadCommand},
    query::{MongoDbQuery, build_query, scan_from_partition},
    resource::{
        MONGODB_COLLECTION_GENERATION_PROTOCOL, mongodb_collection_generation_position,
        rebind_mongodb_partition_for_resume,
    },
    schema::{
        MONGODB_ARRAY_EXTENDED_JSON_SEMANTIC, MONGODB_DECIMAL_TEXT_SEMANTIC,
        MONGODB_DOCUMENT_EXTENDED_JSON_SEMANTIC, MONGODB_OBJECT_ID_SEMANTIC,
        MONGODB_VALUE_EXTENDED_JSON_SEMANTIC, SchemaInference, attach_expected_physical_types,
        compile_source_materializations, decode_batch, decode_batch_with_evidence,
        decode_batch_with_physical_schema, maximum_safe_decode_prefix, parse_decimal128,
    },
};

#[test]
fn server_version_accepts_seven_and_rejects_older_servers() {
    let seven = doc! {
        "version": "7.0.40",
        "versionArray": [7_i32, 0_i32, 40_i32, 0_i32],
    };
    assert_eq!(validate_server_version(&seven).unwrap(), "7.0.40");

    let six = doc! {
        "version": "6.0.25",
        "versionArray": [6_i32, 0_i32, 25_i32, 0_i32],
    };
    let error = validate_server_version(&six).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert!(error.message.contains("requires server 7.0 or later"));
    assert!(error.message.contains("observed major version 6"));
}

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

fn cdc_descriptor(cursor: bool, key: &str) -> ResourceDescriptor {
    let mut descriptor = descriptor(cursor);
    descriptor.write_disposition = WriteDisposition::CdcApply;
    descriptor.primary_key = vec![key.to_owned()];
    descriptor.merge_key = vec![key.to_owned()];
    descriptor
}

#[test]
fn cdc_compilation_uses_native_resume_tokens_without_requiring_a_cursor() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let envelope = Schema::new(vec![
        Field::new("source_database", DataType::Utf8, false),
        Field::new("source_collection", DataType::Utf8, false),
        Field::new("document_key", DataType::Utf8, false),
        Field::new("document", DataType::Utf8, false),
    ]);
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
            ]),
            resource_options: BTreeMap::from([
                ("mode".to_owned(), serde_json::json!("cdc")),
                ("watch".to_owned(), serde_json::json!("database")),
                (
                    "representation".to_owned(),
                    serde_json::json!("envelope"),
                ),
                ("bootstrap".to_owned(), serde_json::json!("latest")),
                (
                    "include_collections".to_owned(),
                    serde_json::json!(["orders", "invoice_*"]),
                ),
                (
                    "exclude_collections".to_owned(),
                    serde_json::json!(["invoice_tmp_*"]),
                ),
                (
                    "change_pipeline".to_owned(),
                    serde_json::json!(r#"[{"$match":{"operationType":{"$in":["insert","update","replace","delete"]}}}]"#),
                ),
            ]),
            descriptor: cdc_descriptor(false, "document_key"),
            schema: envelope,
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        })
        .unwrap();

    assert!(!plan.execution_capabilities.bounded);
    assert!(plan.execution_capabilities.resumable);
    assert!(plan.stream_capabilities.is_some());
    assert!(plan.descriptor.cursor.is_none());
    assert_eq!(
        plan.resource_capabilities.incremental,
        cdf_kernel::IncrementalShape::Cdc
    );
    assert_eq!(plan.redacted_options["change_pipeline_stages"], 1);
    assert!(plan.redacted_options.get("change_pipeline").is_none());
}

#[test]
fn envelope_cdc_accepts_the_framework_residual_column() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let variant =
        Field::new(VARIANT_COLUMN_NAME, DataType::Utf8, true).with_metadata(HashMap::from([
            (
                SEMANTIC_METADATA_KEY.to_owned(),
                CDF_VARIANT_SEMANTIC.to_owned(),
            ),
            (
                RESIDUAL_ENCODING_METADATA_KEY.to_owned(),
                RESIDUAL_ENCODING_NAME.to_owned(),
            ),
        ]));
    let schema = Schema::new(vec![
        Field::new("source_database", DataType::Utf8, false),
        Field::new("source_collection", DataType::Utf8, false),
        Field::new("document_key", DataType::Utf8, false),
        Field::new("document", DataType::Utf8, false),
        variant,
    ]);
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
            ]),
            resource_options: BTreeMap::from([
                ("mode".to_owned(), serde_json::json!("cdc")),
                ("watch".to_owned(), serde_json::json!("database")),
                ("representation".to_owned(), serde_json::json!("envelope")),
                ("bootstrap".to_owned(), serde_json::json!("latest")),
                (
                    "include_collections".to_owned(),
                    serde_json::json!(["orders"]),
                ),
            ]),
            descriptor: cdc_descriptor(false, "document_key"),
            schema,
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        })
        .unwrap();

    assert_eq!(plan.schema.fields().len(), 5);
    assert!(cdf_contract::is_framework_variant_field(
        plan.schema.field(4)
    ));
}

#[test]
fn cdc_rejects_declared_cursor_and_snapshot_only_pipeline_options() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let mut options = BTreeMap::from([
        ("collection".to_owned(), serde_json::json!("orders")),
        ("mode".to_owned(), serde_json::json!("cdc")),
        ("bootstrap".to_owned(), serde_json::json!("latest")),
    ]);
    let request = |descriptor, options| SourceCompileRequest {
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
        ]),
        resource_options: options,
        descriptor,
        schema: schema(),
        type_policy_allowances: Default::default(),
        effective_schema_runtime: None,
        baseline_observation_schema_catalog: Vec::new(),
    };
    let error = driver
        .compile(request(cdc_descriptor(true, "id"), options.clone()))
        .unwrap_err();
    assert!(error.message.contains("must not declare a resource cursor"));

    options.insert("filter".to_owned(), serde_json::json!(r#"{"active":true}"#));
    let error = driver
        .compile(request(cdc_descriptor(false, "id"), options))
        .unwrap_err();
    assert!(
        error
            .message
            .contains("does not accept resource-level snapshot filter")
    );
}

#[test]
fn cdc_latest_ignores_source_snapshot_timeout_but_rejects_resource_override() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let request = |resource_options| SourceCompileRequest {
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
            ("max_time_ms".to_owned(), serde_json::json!(300_000)),
        ]),
        resource_options,
        descriptor: cdc_descriptor(false, "id"),
        schema: schema(),
        type_policy_allowances: Default::default(),
        effective_schema_runtime: None,
        baseline_observation_schema_catalog: Vec::new(),
    };

    let source_default = BTreeMap::from([
        ("collection".to_owned(), serde_json::json!("orders")),
        ("mode".to_owned(), serde_json::json!("cdc")),
        ("bootstrap".to_owned(), serde_json::json!("latest")),
    ]);
    let plan = driver.compile(request(source_default.clone())).unwrap();
    assert_eq!(
        plan.redacted_options["native"]["max_time_ms"],
        serde_json::Value::Null
    );

    let mut resource_override = source_default;
    resource_override.insert("max_time_ms".to_owned(), serde_json::json!(30_000));
    let error = driver.compile(request(resource_override)).unwrap_err();
    assert!(error.message.contains("resource-level snapshot"));
}

#[test]
fn cdc_snapshot_accepts_snapshot_native_options() {
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
                ("max_time_ms".to_owned(), serde_json::json!(300_000)),
            ]),
            resource_options: BTreeMap::from([
                ("collection".to_owned(), serde_json::json!("orders")),
                ("mode".to_owned(), serde_json::json!("cdc")),
                ("bootstrap".to_owned(), serde_json::json!("snapshot")),
                ("filter".to_owned(), serde_json::json!(r#"{"active":true}"#)),
            ]),
            descriptor: cdc_descriptor(false, "id"),
            schema: schema(),
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        })
        .unwrap();

    assert_eq!(plan.redacted_options["bootstrap"], "snapshot");
    assert_eq!(plan.redacted_options["native"]["max_time_ms"], 300_000);

    let error = driver
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
            ]),
            resource_options: BTreeMap::from([
                ("collection".to_owned(), serde_json::json!("orders")),
                ("mode".to_owned(), serde_json::json!("cdc")),
                ("bootstrap".to_owned(), serde_json::json!("snapshot")),
                (
                    "read_preference".to_owned(),
                    serde_json::json!(r#"{"mode":"secondaryPreferred"}"#),
                ),
            ]),
            descriptor: cdc_descriptor(false, "id"),
            schema: schema(),
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        })
        .unwrap_err();
    assert!(error.message.contains("requires primary read preference"));
}

#[test]
fn database_cdc_inventory_comes_from_bound_discovery_evidence() {
    let database = MongoDbIdentifier::new("analytics").unwrap();
    let descriptor = cdc_descriptor(false, "document_key");
    let physical_schema = Arc::new(schema());
    let physical_hash = cdf_kernel::canonical_arrow_schema_hash(physical_schema.as_ref()).unwrap();
    let manifest = cdf_kernel::DiscoveryManifestReference {
        manifest_hash: cdf_kernel::DiscoveryManifestHash::new("mongodb-inventory-manifest")
            .unwrap(),
        path: ".cdf/discovery/mongodb-inventory.json".to_owned(),
    };
    let runtime = cdf_kernel::EffectiveSchemaRuntime::new(
        cdf_kernel::EffectiveSchemaEvidence::new(
            descriptor.schema_source.baseline_reference().unwrap(),
            physical_hash.clone(),
            manifest,
            ["analytics.invoices", "analytics.orders"]
                .into_iter()
                .map(|observation_id| {
                    cdf_kernel::EffectiveSchemaObservationEvidence::new(
                        observation_id,
                        physical_hash.clone(),
                        cdf_kernel::SchemaObservationBinding::new(format!(
                            "sha256:{}",
                            "0".repeat(64)
                        ))
                        .unwrap(),
                    )
                })
                .collect(),
        )
        .unwrap(),
        vec![cdf_kernel::EffectiveSchemaCatalogEntry::new(
            physical_hash,
            physical_schema,
        )],
    )
    .unwrap();

    assert_eq!(
        compiled_database_inventory(&database, Some(&runtime)).unwrap(),
        vec!["invoices".to_owned(), "orders".to_owned()]
    );
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
    assert_eq!(
        plan.execution_capabilities.maximum_decode_bytes,
        128 * 1024 * 1024
    );
    assert_eq!(plan.redacted_options["cursor_batch_rows"], 8_192);
    assert_eq!(plan.redacted_options["output_batch_rows"], 65_536);
    assert_eq!(plan.redacted_options["discovery_records"], 1_000);
    assert_eq!(plan.redacted_options["discovery_bytes"], 16 * 1024 * 1024);
    assert_eq!(plan.redacted_options["max_pool_size"], 1);
    assert_eq!(plan.redacted_options["stream_buffer_batches"], 1);
    assert_eq!(plan.redacted_options["schema_depth"], 1);
    assert_eq!(plan.physical_plan["schema_depth"], 1);
    let encoded = serde_json::to_string(&plan).unwrap();
    assert!(encoded.contains("secret://env/MONGODB_PASSWORD"));
    assert!(!encoded.contains("inline-password"));
    driver.validate_portable_plan(&plan).unwrap();
}

#[test]
fn compile_binds_schema_depth_and_rejects_invalid_values() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let request = |schema_depth| SourceCompileRequest {
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
        ]),
        resource_options: BTreeMap::from([
            ("collection".to_owned(), serde_json::json!("events")),
            ("schema_depth".to_owned(), schema_depth),
        ]),
        descriptor: descriptor(false),
        schema: schema(),
        type_policy_allowances: Default::default(),
        effective_schema_runtime: None,
        baseline_observation_schema_catalog: Vec::new(),
    };

    let plan = driver.compile(request(serde_json::json!(2))).unwrap();
    assert_eq!(plan.redacted_options["schema_depth"], 2);
    assert_eq!(plan.physical_plan["schema_depth"], 2);
    driver.validate_portable_plan(&plan).unwrap();

    for invalid in [serde_json::json!(0), serde_json::json!(33)] {
        let error = driver.compile(request(invalid)).unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
        assert!(error.message.contains("schema_depth"), "{error}");
    }
}

#[test]
fn compile_keeps_measured_defaults_and_binds_resource_tuning_controls() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let request = |source_options, resource_options| SourceCompileRequest {
        source_kind: "mongodb".to_owned(),
        context: cdf_runtime::SourceCompileContext {
            source_name: "warehouse".to_owned(),
            project_root: None,
            cursor_pushdown: None,
        },
        source_options,
        resource_options,
        descriptor: descriptor(false),
        schema: schema(),
        type_policy_allowances: Default::default(),
        effective_schema_runtime: None,
        baseline_observation_schema_catalog: Vec::new(),
    };
    let source_options = BTreeMap::from([
        (
            "endpoint".to_owned(),
            serde_json::json!("mongodb://warehouse.example:27017"),
        ),
        ("database".to_owned(), serde_json::json!("analytics")),
        ("schema_depth".to_owned(), serde_json::json!(2)),
        ("discovery_records".to_owned(), serde_json::json!(1_500)),
        ("discovery_bytes".to_owned(), serde_json::json!(4_194_304)),
        ("cursor_batch_rows".to_owned(), serde_json::json!(2_048)),
        ("output_batch_rows".to_owned(), serde_json::json!(16_384)),
        ("max_time_ms".to_owned(), serde_json::json!(15_000)),
        ("read_concern".to_owned(), serde_json::json!("local")),
        (
            "read_preference".to_owned(),
            serde_json::json!(r#"{"mode":"primary"}"#),
        ),
    ]);
    let source_default_plan = driver
        .compile(request(
            source_options.clone(),
            BTreeMap::from([("collection".to_owned(), serde_json::json!("events"))]),
        ))
        .unwrap();
    assert_eq!(source_default_plan.redacted_options["schema_depth"], 2);
    assert_eq!(
        source_default_plan.redacted_options["cursor_batch_rows"],
        2_048
    );
    assert_eq!(
        source_default_plan.redacted_options["output_batch_rows"],
        16_384
    );
    assert_eq!(
        source_default_plan.redacted_options["discovery_records"],
        1_500
    );
    assert_eq!(
        source_default_plan.redacted_options["native"]["max_time_ms"],
        15_000
    );
    assert_eq!(
        source_default_plan.redacted_options["native"]["read_concern"],
        true
    );
    assert_eq!(
        source_default_plan.redacted_options["native"]["read_preference"],
        true
    );
    let plan = driver
        .compile(request(
            source_options.clone(),
            BTreeMap::from([
                ("collection".to_owned(), serde_json::json!("events")),
                ("discovery_records".to_owned(), serde_json::json!(2_500)),
                ("discovery_bytes".to_owned(), serde_json::json!(8_388_608)),
                ("cursor_batch_rows".to_owned(), serde_json::json!(4_096)),
                ("output_batch_rows".to_owned(), serde_json::json!(32_768)),
                ("schema_depth".to_owned(), serde_json::json!(3)),
                ("max_time_ms".to_owned(), serde_json::json!(30_000)),
                ("read_concern".to_owned(), serde_json::json!("majority")),
                (
                    "read_preference".to_owned(),
                    serde_json::json!(r#"{"mode":"secondaryPreferred"}"#),
                ),
            ]),
        ))
        .unwrap();

    assert_eq!(plan.driver.driver_version, "3.0.0");
    assert_eq!(plan.redacted_options["schema_depth"], 3);
    assert_eq!(plan.redacted_options["cursor_batch_rows"], 4_096);
    assert_eq!(plan.redacted_options["output_batch_rows"], 32_768);
    assert_eq!(plan.redacted_options["discovery_records"], 2_500);
    assert_eq!(plan.redacted_options["discovery_bytes"], 8_388_608);
    assert_eq!(plan.physical_plan["cursor_batch_rows"], 4_096);
    assert_eq!(plan.physical_plan["output_batch_rows"], 32_768);
    assert_eq!(plan.redacted_options["native"]["max_time_ms"], 30_000);

    let mut legacy_source = source_options;
    legacy_source.insert("batch_rows".to_owned(), serde_json::json!(1_000));
    let error = driver
        .compile(request(
            legacy_source,
            BTreeMap::from([("collection".to_owned(), serde_json::json!("events"))]),
        ))
        .unwrap_err();
    assert!(
        error.message.contains("unknown field `batch_rows`"),
        "{error}"
    );
}

#[test]
fn native_find_compilation_is_literal_safe_in_redacted_evidence() {
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
            ]),
            resource_options: BTreeMap::from([
                ("collection".to_owned(), serde_json::json!("events")),
                (
                    "filter".to_owned(),
                    serde_json::json!(
                        r#"{"tenant":{"$oid":"64b64c27f6f1a00f92d66c6a"},"status":"do-not-render"}"#
                    ),
                ),
                ("hint".to_owned(), serde_json::json!(r#""tenant_1""#)),
                (
                    "collation".to_owned(),
                    serde_json::json!(r#"{"locale":"en","strength":2}"#),
                ),
                ("max_time_ms".to_owned(), serde_json::json!(30_000)),
                ("read_concern".to_owned(), serde_json::json!("majority")),
                (
                    "read_preference".to_owned(),
                    serde_json::json!(
                        r#"{"mode":"secondaryPreferred","tagSets":[{"nodeType":"ANALYTICS"}]}"#
                    ),
                ),
            ]),
            descriptor: descriptor(false),
            schema: schema(),
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        })
        .unwrap();

    assert_eq!(plan.redacted_options["native"]["input_kind"], "find");
    assert_eq!(plan.redacted_options["native"]["max_time_ms"], 30_000);
    assert_eq!(plan.redacted_options["native"]["hint"], true);
    assert_eq!(plan.redacted_options["native"]["collation"], true);
    let redacted = serde_json::to_string(&plan.redacted_options).unwrap();
    assert!(!redacted.contains("do-not-render"));
    assert!(!redacted.contains("64b64c27f6f1a00f92d66c6a"));
    let physical = serde_json::to_string(&plan.physical_plan).unwrap();
    assert!(physical.contains("bson_base64"));
    assert!(!physical.contains("do-not-render"));
    driver.validate_portable_plan(&plan).unwrap();
}

#[test]
fn native_bson_artifact_round_trip_preserves_numeric_width_and_document_order() {
    let native = MongoDbNativeExtraction::compile(MongoDbNativeResourceOptions {
        filter: Some(
            r#"{"small":1,"wide":{"$numberLong":"1"},"profile.id":7,"nested":{"second":2,"first":1}}"#.to_owned(),
        ),
        hint: Some(r#"{"second":-1,"first":1}"#.to_owned()),
        collation: Some(r#"{"locale":"en","numericOrdering":true}"#.to_owned()),
        read_preference: Some(
            r#"{"mode":"secondaryPreferred","tagSets":[{"nodeType":"ANALYTICS","region":"west"}],"maxStalenessSeconds":120}"#.to_owned(),
        ),
        ..Default::default()
    })
    .unwrap();
    let identity = native.identity_hash().unwrap();
    let artifact = serde_json::to_value(&native).unwrap();
    let decoded: MongoDbNativeExtraction = serde_json::from_value(artifact).unwrap();
    assert_eq!(decoded.identity_hash().unwrap(), identity);

    let MongoDbReadCommand::Find { filter, options } = decoded.execution_command(
        MongoDbQuery {
            filter: Document::new(),
            projection: doc! {"small": 1_i32},
            sort: Document::new(),
            limit: None,
        },
        8_192,
    ) else {
        panic!("find input must produce a find command");
    };
    assert_eq!(filter.get_i32("small").unwrap(), 1);
    assert_eq!(filter.get_i64("wide").unwrap(), 1);
    assert_eq!(filter.get_i32("profile.id").unwrap(), 7);
    let nested = filter.get_document("nested").unwrap();
    assert_eq!(nested.keys().collect::<Vec<_>>(), vec!["second", "first"]);
    let mongodb::options::Hint::Keys(hint) = options.hint.unwrap() else {
        panic!("document hint must remain a key hint");
    };
    assert_eq!(hint.keys().collect::<Vec<_>>(), vec!["second", "first"]);
}

#[test]
fn native_pipeline_validation_rejects_unsafe_and_ambiguous_inputs() {
    let compile = |descriptor: ResourceDescriptor, resource_options| {
        MongoDbSourceDriver::new()
            .unwrap()
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
                ]),
                resource_options,
                descriptor,
                schema: schema(),
                type_policy_allowances: Default::default(),
                effective_schema_runtime: None,
                baseline_observation_schema_catalog: Vec::new(),
            })
    };

    let cases = [
        BTreeMap::from([
            ("collection".to_owned(), serde_json::json!("events")),
            ("filter".to_owned(), serde_json::json!(r#"{"x":1}"#)),
            ("pipeline".to_owned(), serde_json::json!("[]")),
        ]),
        BTreeMap::from([
            ("collection".to_owned(), serde_json::json!("events")),
            (
                "pipeline".to_owned(),
                serde_json::json!(r#"[{"$facet":{"sink":[{"$merge":"forbidden"}]}}]"#),
            ),
        ]),
        BTreeMap::from([
            ("collection".to_owned(), serde_json::json!("events")),
            (
                "pipeline".to_owned(),
                serde_json::json!(r#"[{"$match":{"x":1,"x":2}}]"#),
            ),
        ]),
        BTreeMap::from([
            ("collection".to_owned(), serde_json::json!("events")),
            ("filter".to_owned(), serde_json::json!(r#"{"x":1}"#)),
            ("allow_disk_use".to_owned(), serde_json::json!(true)),
        ]),
    ];
    for resource_options in cases {
        let error = compile(descriptor(false), resource_options).unwrap_err();
        assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract, "{error}");
    }

    let nondeterministic = compile(
        descriptor(true),
        BTreeMap::from([
            ("collection".to_owned(), serde_json::json!("events")),
            (
                "pipeline".to_owned(),
                serde_json::json!(r#"[{"$sample":{"size":10}}]"#),
            ),
        ]),
    )
    .unwrap_err();
    assert!(nondeterministic.message.contains("nondeterministic"));
}

#[test]
fn cross_collection_pipeline_is_local_only_until_each_dependency_is_attested() {
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
            ]),
            resource_options: BTreeMap::from([
                ("collection".to_owned(), serde_json::json!("events")),
                (
                    "pipeline".to_owned(),
                    serde_json::json!(r#"[{"$lookup":{"from":"accounts","localField":"tenant","foreignField":"_id","as":"account"}}]"#),
                ),
            ]),
            descriptor: descriptor(false),
            schema: schema(),
            type_policy_allowances: Default::default(),
            effective_schema_runtime: None,
            baseline_observation_schema_catalog: Vec::new(),
        })
        .unwrap();

    let error = driver.validate_portable_plan(&plan).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert!(error.message.contains("additional collections"));
}

#[test]
fn native_commands_preserve_authored_pipeline_and_apply_outer_cdf_stages() {
    let native = MongoDbNativeExtraction::compile(MongoDbNativeResourceOptions {
        pipeline: Some(r#"[{"$match":{"tenant":"t1"}},{"$group":{"_id":"$kind","sequence":{"$max":"$sequence"}}}]"#.to_owned()),
        allow_disk_use: true,
        let_vars: Some(r#"{"minimum":1}"#.to_owned()),
        max_time_ms: Some(90_000),
        ..Default::default()
    })
    .unwrap();
    let identity = native.identity_hash().unwrap();
    let native: MongoDbNativeExtraction =
        serde_json::from_value(serde_json::to_value(&native).unwrap()).unwrap();
    assert_eq!(native.identity_hash().unwrap(), identity);
    let query = MongoDbQuery {
        filter: doc! {"sequence": {"$gt": 10_i64}},
        projection: doc! {"_id": 1_i32, "sequence": 1_i32},
        sort: doc! {"sequence": 1_i32},
        limit: Some(25),
    };
    let MongoDbReadCommand::Aggregate { pipeline, options } =
        native.execution_command(query, 8_192)
    else {
        panic!("aggregation input must produce an aggregate command");
    };

    assert_eq!(pipeline.len(), 6);
    assert!(pipeline[0].contains_key("$match"));
    assert!(pipeline[1].contains_key("$group"));
    assert!(pipeline[2].contains_key("$match"));
    assert!(pipeline[3].contains_key("$sort"));
    assert!(pipeline[4].contains_key("$project"));
    assert!(pipeline[5].contains_key("$limit"));
    assert_eq!(options.batch_size, Some(8_192));
    assert_eq!(options.allow_disk_use, Some(true));
    assert_eq!(options.max_time, Some(std::time::Duration::from_secs(90)));
    assert_eq!(options.let_vars, Some(doc! {"minimum": 1_i32}));
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
fn add_planner_compiles_authority_collection_and_private_credentials() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let request = SourceAddRequest {
        source_name: "warehouse".to_owned(),
        resource_name: "events".to_owned(),
        location: "mongodb://reader:unprintable-password@mongo.example:27017/analytics/events"
            .to_owned(),
        project_root: "/project".into(),
        current_dir: "/project".into(),
        options: BTreeMap::from([
            ("cursor".to_owned(), "sequence".to_owned()),
            ("schema_depth".to_owned(), "2".to_owned()),
            ("discovery_records".to_owned(), "2500".to_owned()),
            ("discovery_bytes".to_owned(), "8388608".to_owned()),
            ("cursor_batch_rows".to_owned(), "4096".to_owned()),
            ("output_batch_rows".to_owned(), "32768".to_owned()),
            ("filter".to_owned(), r#"{"status":"active"}"#.to_owned()),
        ]),
        project_options: None,
    };
    let proposal = driver
        .add_planner()
        .unwrap()
        .propose_add(&request)
        .unwrap()
        .unwrap();

    assert_eq!(proposal.source_kind, "mongodb");
    assert_eq!(
        proposal.source_options["endpoint"],
        "mongodb://mongo.example:27017"
    );
    assert_eq!(proposal.source_options["database"], "analytics");
    assert_eq!(proposal.resource_options["collection"], "events");
    assert_eq!(proposal.resource_options["schema_depth"], 2);
    assert_eq!(proposal.resource_options["discovery_records"], 2_500);
    assert_eq!(proposal.resource_options["discovery_bytes"], 8_388_608);
    assert_eq!(proposal.resource_options["cursor_batch_rows"], 4_096);
    assert_eq!(proposal.resource_options["output_batch_rows"], 32_768);
    assert_eq!(
        proposal.resource_options["filter"],
        r#"{"status":"active"}"#
    );
    assert!(!proposal.source_options.contains_key("schema_depth"));
    assert!(!proposal.source_options.contains_key("discovery_records"));
    assert!(!proposal.source_options.contains_key("cursor_batch_rows"));
    assert_eq!(proposal.cursor.as_ref().unwrap().field, "sequence");
    assert_eq!(proposal.private_files.len(), 2);
    let rendered = format!("{proposal:?}");
    assert!(!rendered.contains("unprintable-password"));
}

#[test]
fn add_planner_rejects_invalid_schema_depth() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let error = driver
        .add_planner()
        .unwrap()
        .propose_add(&SourceAddRequest {
            source_name: "warehouse".to_owned(),
            resource_name: "events".to_owned(),
            location: "mongodb://mongo.example:27017/analytics/events".to_owned(),
            project_root: "/project".into(),
            current_dir: "/project".into(),
            options: BTreeMap::from([("schema_depth".to_owned(), "0".to_owned())]),
            project_options: None,
        })
        .unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Contract);
    assert!(error.message.contains("schema_depth"));
}

#[test]
fn add_planner_percent_decodes_credentials_and_resource_path() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let proposal = driver
        .add_planner()
        .unwrap()
        .propose_add(&SourceAddRequest {
            source_name: "warehouse".to_owned(),
            resource_name: "events".to_owned(),
            location: "mongodb://reader%40ops:p%40ss%3Aword@mongo.example:27017/analytics%2Dprod/events%2D2026".to_owned(),
            project_root: "/project".into(),
            current_dir: "/project".into(),
            options: BTreeMap::new(),
            project_options: None,
        })
        .unwrap()
        .unwrap();

    assert_eq!(proposal.source_options["database"], "analytics-prod");
    assert_eq!(proposal.resource_options["collection"], "events-2026");
    assert_eq!(proposal.private_files.len(), 2);
    assert_eq!(
        proposal.private_files[0].value.as_str().unwrap(),
        "reader@ops"
    );
    assert_eq!(
        proposal.private_files[1].value.as_str().unwrap(),
        "p@ss:word"
    );
}

#[test]
fn add_planner_splits_mongodb_aws_uri_into_secret_references() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let proposal = driver
        .add_planner()
        .unwrap()
        .propose_add(&SourceAddRequest {
            source_name: "atlas".to_owned(),
            resource_name: "events".to_owned(),
            location: "mongodb+srv://ACCESS:SECRET@cluster.example/analytics/events?ssl=true&authMechanism=MONGODB-AWS&authSource=%24external&authMechanismProperties=AWS_SESSION_TOKEN%3Asession-token".to_owned(),
            project_root: "/project".into(),
            current_dir: "/project".into(),
            options: BTreeMap::new(),
            project_options: None,
        })
        .unwrap()
        .unwrap();

    assert_eq!(
        proposal.source_options["endpoint"],
        "mongodb+srv://cluster.example"
    );
    assert_eq!(proposal.source_options["database"], "analytics");
    assert_eq!(proposal.source_options["auth_source"], "$external");
    assert_eq!(proposal.source_options["auth_mechanism"], "MONGODB-AWS");
    assert_eq!(
        proposal.source_options["aws_session_token"],
        "secret://file/.cdf/secrets/sources/atlas.aws_session_token"
    );
    assert_eq!(proposal.private_files.len(), 3);
    let rendered = format!("{proposal:?}");
    assert!(!rendered.contains("ACCESS"));
    assert!(!rendered.contains("SECRET"));
    assert!(!rendered.contains("session-token"));
}

#[test]
fn compile_rejects_incomplete_mongodb_aws_authority() {
    let driver = MongoDbSourceDriver::new().unwrap();
    let mut source_options = BTreeMap::from([
        (
            "endpoint".to_owned(),
            serde_json::json!("mongodb+srv://cluster.example"),
        ),
        ("database".to_owned(), serde_json::json!("analytics")),
        (
            "auth_mechanism".to_owned(),
            serde_json::json!("MONGODB-AWS"),
        ),
        (
            "username".to_owned(),
            serde_json::json!("secret://env/MONGODB_USER"),
        ),
        (
            "password".to_owned(),
            serde_json::json!("secret://env/MONGODB_PASSWORD"),
        ),
    ]);
    let request = |source_options| SourceCompileRequest {
        source_kind: "mongodb".to_owned(),
        context: cdf_runtime::SourceCompileContext {
            source_name: "atlas".to_owned(),
            project_root: None,
            cursor_pushdown: None,
        },
        source_options,
        resource_options: BTreeMap::from([("collection".to_owned(), serde_json::json!("events"))]),
        descriptor: descriptor(false),
        schema: schema(),
        type_policy_allowances: Default::default(),
        effective_schema_runtime: None,
        baseline_observation_schema_catalog: Vec::new(),
    };

    let error = driver.compile(request(source_options.clone())).unwrap_err();
    assert!(error.message.contains("auth_source `$external`"));

    source_options.insert("auth_source".to_owned(), serde_json::json!("$external"));
    source_options.insert(
        "aws_session_token".to_owned(),
        serde_json::json!("secret://env/MONGODB_AWS_SESSION_TOKEN"),
    );
    let plan = driver.compile(request(source_options)).unwrap();
    let encoded = serde_json::to_string(&plan).unwrap();
    assert!(encoded.contains("MONGODB-AWS"));
    assert!(encoded.contains("secret://env/MONGODB_AWS_SESSION_TOKEN"));
}

#[test]
fn default_discovery_types_primitives_and_keeps_complex_values_opaque() {
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
    assert!(schema.fields().iter().all(|field| field.is_nullable()));
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
    let nested = schema.field_with_name("nested").unwrap();
    assert_eq!(nested.data_type(), &DataType::Utf8);
    assert_eq!(physical_type(nested), Some("bson:document"));
    assert_eq!(
        nested.metadata()["cdf:semantic"],
        MONGODB_DOCUMENT_EXTENDED_JSON_SEMANTIC
    );
    let tags = schema.field_with_name("tags").unwrap();
    assert_eq!(tags.data_type(), &DataType::Utf8);
    assert_eq!(physical_type(tags), Some("bson:array"));
    assert_eq!(
        tags.metadata()["cdf:semantic"],
        MONGODB_ARRAY_EXTENDED_JSON_SEMANTIC
    );

    let schema = Arc::new(schema);
    let batch = decode_batch(Arc::clone(&schema), &[first.as_ref(), second.as_ref()]).unwrap();
    let nested = batch
        .column(schema.index_of("nested").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(nested.value(0), r#"{"left":{"$numberLong":"1"}}"#);
    assert_eq!(nested.value(1), r#"{"right":true}"#);
    let tags = batch
        .column(schema.index_of("tags").unwrap())
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(tags.value(0), r#"["one","two"]"#);
    assert_eq!(tags.value(1), r#"["three"]"#);
}

#[test]
fn cdc_discovery_requires_mongodb_id_without_claiming_other_fields_are_required() {
    let schema = Schema::new(vec![
        with_source_name(Field::new("_id", DataType::Utf8, true), "_id"),
        with_source_name(Field::new("status", DataType::Utf8, true), "status"),
    ]);

    let cdc = with_required_cdc_id(schema).unwrap();

    assert!(!cdc.field_with_name("_id").unwrap().is_nullable());
    assert!(cdc.field_with_name("status").unwrap().is_nullable());
    assert!(
        with_required_cdc_id(Schema::new(vec![Field::new(
            "status",
            DataType::Utf8,
            true,
        )]))
        .unwrap_err()
        .message
        .contains("required `_id`")
    );
}

#[test]
fn configured_depth_two_infers_direct_children_without_nested_key_explosion() {
    let first = RawDocumentBuf::try_from(&doc! {
        "nested": {"left": 1_i64, "map": {"uuid-a": 1_i64}},
        "tags": ["one", "two"],
    })
    .unwrap();
    let second = RawDocumentBuf::try_from(&doc! {
        "nested": {"right": true, "map": {"uuid-b": 2_i64}},
        "tags": ["three"],
    })
    .unwrap();
    let mut inference = SchemaInference::new(2).unwrap();
    inference.observe(&first).unwrap();
    inference.observe(&second).unwrap();
    let (schema, _, _) = inference.finish().unwrap();

    let DataType::Struct(fields) = schema.field_with_name("nested").unwrap().data_type() else {
        panic!("configured depth two did not infer direct document children");
    };
    assert_eq!(fields.len(), 3);
    assert!(fields.find("left").unwrap().1.is_nullable());
    assert!(fields.find("right").unwrap().1.is_nullable());
    let (_, map) = fields.find("map").unwrap();
    assert!(map.is_nullable());
    assert_eq!(map.data_type(), &DataType::Utf8);
    assert_eq!(physical_type(map), Some("bson:document"));
    assert_eq!(
        map.metadata()["cdf:semantic"],
        MONGODB_DOCUMENT_EXTENDED_JSON_SEMANTIC
    );
    assert!(matches!(
        schema.field_with_name("tags").unwrap().data_type(),
        DataType::List(_)
    ));

    let schema = Arc::new(schema);
    let batch = decode_batch(Arc::clone(&schema), &[first.as_ref(), second.as_ref()]).unwrap();
    let nested = batch
        .column(schema.index_of("nested").unwrap())
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let map = nested
        .column_by_name("map")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(map.value(0), r#"{"uuid-a":{"$numberLong":"1"}}"#);
    assert_eq!(map.value(1), r#"{"uuid-b":{"$numberLong":"2"}}"#);
}

#[test]
fn heterogeneous_sampled_values_use_lossless_mixed_extended_json() {
    let first = RawDocumentBuf::try_from(&doc! {"value": 1_i32}).unwrap();
    let second = RawDocumentBuf::try_from(&doc! {"value": "text"}).unwrap();
    let mut inference = SchemaInference::default();
    inference.observe(&first).unwrap();
    inference.observe(&second).unwrap();
    let (schema, _, _) = inference.finish().unwrap();
    let value = schema.field_with_name("value").unwrap();
    assert_eq!(value.data_type(), &DataType::Utf8);
    assert_eq!(physical_type(value), Some("bson:mixed"));
    assert_eq!(
        value.metadata()["cdf:semantic"],
        MONGODB_VALUE_EXTENDED_JSON_SEMANTIC
    );

    let schema = Arc::new(schema);
    let batch = decode_batch(Arc::clone(&schema), &[first.as_ref(), second.as_ref()]).unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), r#"{"$numberInt":"1"}"#);
    assert_eq!(values.value(1), r#""text""#);
}

#[test]
fn future_primitive_mismatch_is_residualized_under_governed_decode() {
    let sampled = RawDocumentBuf::try_from(&doc! {"sequence": 1_i64}).unwrap();
    let changed = RawDocumentBuf::try_from(&doc! {"sequence": {"nested": 1_i64}}).unwrap();
    let mut inference = SchemaInference::default();
    inference.observe(&sampled).unwrap();
    let (schema, _, _) = inference.finish().unwrap();
    let schema = Arc::new(schema);

    let decoded =
        decode_batch_with_evidence(Arc::clone(&schema), schema, &[changed.as_ref()], 0).unwrap();
    assert_eq!(decoded.residual_candidates.len(), 1);
    assert_eq!(decoded.residual_candidates[0].source_path(), ["sequence"]);
    assert!(decoded.record_batch.column(0).is_null(0));
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
    assert!(
        error
            .message
            .contains("contradicted the pinned Arrow schema")
    );
}

#[test]
fn governed_decoder_preserves_unknown_and_mismatched_values_as_residual_evidence() {
    let object_id = ObjectId::parse_str("64b64c27f6f1a00f92d66c6a").unwrap();
    let document = RawDocumentBuf::try_from(&doc! {
        "_id": object_id,
        "sequence": "wrong",
        "amount": Decimal128::from_str("1").unwrap(),
        "observed_at": DateTime::from_millis(0),
        "new_field": 42_i64,
    })
    .unwrap();
    let schema = Arc::new(schema());
    let decoded =
        decode_batch_with_evidence(Arc::clone(&schema), schema, &[document.as_ref()], 17).unwrap();

    assert_eq!(decoded.record_batch.num_rows(), 1);
    assert_eq!(decoded.residual_candidates.len(), 2);
    assert!(decoded.residual_candidates.iter().any(|candidate| {
        candidate.source_row_ordinal() == 17
            && candidate.source_path() == ["sequence"]
            && candidate.expected_field().is_some()
    }));
    assert!(decoded.residual_candidates.iter().any(|candidate| {
        candidate.source_path() == ["new_field"] && candidate.expected_field().is_none()
    }));
    assert!(
        decoded
            .physical_schema
            .fields()
            .iter()
            .all(|field| field.name() != "new_field")
    );
    assert!(decoded.residual_candidates.iter().any(|candidate| {
        candidate.observed_field().name() == "new_field"
            && physical_type(candidate.observed_field()) == Some("bson:int64")
    }));
}

#[test]
fn governed_decoder_rejects_structural_cardinality_before_residual_allocation() {
    let mut document = RawDocumentBuf::new();
    document.append(cstr!("known"), 1_i64);
    for index in 0..=65_536 {
        document.append(CString::try_from(format!("extra_{index}")).unwrap(), index);
    }
    let schema = Arc::new(Schema::new(vec![Field::new(
        "known",
        DataType::Int64,
        false,
    )]));

    let error = decode_batch_with_evidence(Arc::clone(&schema), schema, &[document.as_ref()], 0)
        .unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(
        error.message.contains("65536-element structural"),
        "{error}"
    );
}

#[test]
fn collection_metadata_binds_complete_collation_and_validator_without_plaintext() {
    let response = doc! {
        "cursor": {"firstBatch": [{
            "name": "events",
            "type": "collection",
            "info": {"uuid": Binary {
                subtype: BinarySubtype::Uuid,
                bytes: vec![7_u8; 16],
            }},
            "options": {
                "collation": {"locale": "en", "strength": 2_i32, "numericOrdering": true},
                "validator": {"sequence": {"$type": "long"}},
                "validationLevel": "strict",
                "validationAction": "error"
            }
        }]}
    };

    let metadata =
        collection_metadata_from_response(&response, &MongoDbIdentifier::new("events").unwrap())
            .unwrap();
    let identity = metadata.identity();

    assert_eq!(identity["collection_type"], "collection");
    assert!(identity["collection_uuid_sha256"].starts_with("sha256:"));
    assert!(identity["collection_generation_sha256"].starts_with("sha256:"));
    assert!(identity["collation_identity"].starts_with("sha256:"));
    assert!(identity["validator_sha256"].starts_with("sha256:"));
    assert_eq!(identity["validation_level"], "strict");
    assert_eq!(identity["validation_action"], "error");
    let rendered = format!("{identity:?}");
    assert!(!rendered.contains("numericOrdering"));
    assert!(!rendered.contains("$type"));
}

#[test]
fn collection_generation_changes_when_collection_uuid_changes() {
    let response = |uuid_byte| {
        doc! {"cursor": {"firstBatch": [{
            "name": "events",
            "type": "collection",
            "info": {"uuid": Binary {
                subtype: BinarySubtype::Uuid,
                bytes: vec![uuid_byte; 16],
            }},
            "options": {}
        }]}}
    };
    let collection = MongoDbIdentifier::new("events").unwrap();
    let first = collection_metadata_from_response(&response(1), &collection).unwrap();
    let second = collection_metadata_from_response(&response(2), &collection).unwrap();

    assert_ne!(
        first.identity()["collection_generation_sha256"],
        second.identity()["collection_generation_sha256"]
    );
}

#[test]
fn collection_generation_position_binds_resource_and_collection_identity() {
    let descriptor = descriptor(false);
    let database = MongoDbIdentifier::new("ledger").unwrap();
    let collection = MongoDbIdentifier::new("events").unwrap();
    let first = mongodb_collection_generation_position(
        &descriptor,
        &database,
        &collection,
        "sha256:1111111111111111111111111111111111111111111111111111111111111111",
    )
    .unwrap();
    let same = mongodb_collection_generation_position(
        &descriptor,
        &database,
        &collection,
        "sha256:1111111111111111111111111111111111111111111111111111111111111111",
    )
    .unwrap();
    let changed = mongodb_collection_generation_position(
        &descriptor,
        &database,
        &collection,
        "sha256:2222222222222222222222222222222222222222222222222222222222222222",
    )
    .unwrap();

    assert_eq!(first, same);
    assert_ne!(first, changed);
    let SourcePosition::ForeignState(state) = first else {
        panic!("MongoDB collection generation must use foreign-state authority");
    };
    assert_eq!(state.protocol, MONGODB_COLLECTION_GENERATION_PROTOCOL);

    let error =
        mongodb_collection_generation_position(&descriptor, &database, &collection, "not-a-hash")
            .unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
}

#[test]
fn collection_metadata_rejects_views_and_malformed_options() {
    let collection = MongoDbIdentifier::new("events").unwrap();
    let view = doc! {
        "cursor": {"firstBatch": [{
            "name": "events",
            "type": "view",
            "options": {}
        }]}
    };
    assert!(collection_metadata_from_response(&view, &collection).is_err());

    for malformed in [
        doc! {"cursor": {"firstBatch": [{"name": "events", "options": {}}]}},
        doc! {"cursor": {"firstBatch": [{"name": "events", "type": "collection", "options": "wrong"}]}},
        doc! {"cursor": {"firstBatch": [{"name": "events", "type": "collection", "options": {"collation": "wrong"}}]}},
        doc! {"cursor": {"firstBatch": [{"name": "events", "type": "collection", "options": {"validator": "wrong"}}]}},
    ] {
        assert!(collection_metadata_from_response(&malformed, &collection).is_err());
    }
}

#[test]
fn governed_decoder_captures_nested_unknown_field_at_exact_path() {
    let nested_field = Field::new(
        "nested",
        DataType::Struct(
            vec![Arc::new(with_source_name(
                Field::new("known", DataType::Int64, false),
                "known",
            ))]
            .into(),
        ),
        false,
    );
    let schema = Arc::new(Schema::new(vec![with_source_name(nested_field, "nested")]));
    let document = RawDocumentBuf::try_from(&doc! {
        "nested": {"known": 1_i64, "extra": "preserve me"}
    })
    .unwrap();

    let decoded =
        decode_batch_with_evidence(Arc::clone(&schema), schema, &[document.as_ref()], 0).unwrap();

    assert_eq!(decoded.residual_candidates.len(), 1);
    assert!(decoded.physical_reconciliations.is_empty());
    assert_eq!(
        decoded.residual_candidates[0].source_path(),
        ["nested", "extra"]
    );
    assert!(decoded.residual_candidates[0].expected_field().is_none());
}

#[test]
fn compatible_bson_integer_keeps_catalog_physical_observation_domain() {
    let logical = Arc::new(Schema::new(vec![with_source_name(
        Field::new("sequence", DataType::Int64, false),
        "sequence",
    )]));
    let physical = Arc::new(Schema::new(vec![with_physical_type(
        with_source_name(Field::new("sequence", DataType::Int32, false), "sequence"),
        "bson:int32",
    )]));
    let decoder = attach_expected_physical_types(logical.as_ref(), physical.as_ref()).unwrap();
    let document = RawDocumentBuf::try_from(&doc! {"sequence": 7_i32}).unwrap();

    let decoded = decode_batch_with_physical_schema(
        Arc::clone(&decoder),
        decoder,
        logical,
        physical,
        &[document.as_ref()],
        0,
    )
    .unwrap();

    assert!(decoded.residual_candidates.is_empty());
    assert!(decoded.physical_reconciliations.is_empty());
    assert_eq!(decoded.record_batch.column(0).data_type(), &DataType::Int64);
    assert!(decoded.record_batch.schema().field(0).is_nullable());
    assert_eq!(
        decoded.physical_schema.field(0).data_type(),
        &DataType::Int32
    );
    assert_eq!(
        physical_type(decoded.physical_schema.field(0)),
        Some("bson:int32")
    );
}

#[test]
fn mixed_pin_accepts_a_later_homogeneous_physical_observation() {
    let logical = Arc::new(Schema::new(vec![with_physical_type(
        semantic_field(
            Field::new("value", DataType::Utf8, false),
            MONGODB_VALUE_EXTENDED_JSON_SEMANTIC,
        ),
        "bson:mixed",
    )]));
    let physical = Arc::new(Schema::new(vec![with_physical_type(
        Field::new("value", DataType::Int32, false),
        "bson:int32",
    )]));
    let decoder = attach_expected_physical_types(logical.as_ref(), physical.as_ref()).unwrap();
    assert_eq!(decoder.field(0).data_type(), &DataType::Utf8);
    assert_eq!(physical_type(decoder.field(0)), Some("bson:int32"));
    let document = RawDocumentBuf::try_from(&doc! {"value": 7_i32}).unwrap();

    let decoded = decode_batch_with_physical_schema(
        Arc::clone(&decoder),
        decoder,
        logical,
        physical,
        &[document.as_ref()],
        0,
    )
    .unwrap();

    assert!(decoded.residual_candidates.is_empty());
    assert_eq!(
        decoded
            .record_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        r#"{"$numberInt":"7"}"#
    );
}

#[test]
fn compatible_physical_reconciliation_is_vectorized_beyond_residual_cardinality() {
    const ROWS: usize = 65_537;
    let pinned = Arc::new(Schema::new(vec![with_physical_type(
        with_source_name(Field::new("sequence", DataType::Int64, false), "sequence"),
        "bson:int64",
    )]));
    let documents = (0..ROWS)
        .map(|row| RawDocumentBuf::try_from(&doc! {"sequence": i32::try_from(row).unwrap()}))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let references = documents
        .iter()
        .map(RawDocumentBuf::as_ref)
        .collect::<Vec<_>>();

    let decoded = decode_batch_with_evidence(Arc::clone(&pinned), pinned, &references, 0).unwrap();

    assert!(decoded.residual_candidates.is_empty());
    assert_eq!(decoded.physical_reconciliations.len(), 1);
    assert_eq!(
        decoded.physical_reconciliations[0]
            .batch_row_ordinals()
            .len(),
        ROWS
    );
    assert_eq!(decoded.record_batch.num_rows(), ROWS);
}

#[test]
fn nested_leaf_reconciliation_composes_with_missing_and_unknown_fields() {
    let nested = with_physical_type(
        with_source_name(
            Field::new(
                "nested",
                DataType::Struct(
                    vec![
                        Arc::new(with_physical_type(
                            with_source_name(Field::new("known", DataType::Int64, false), "known"),
                            "bson:int64",
                        )),
                        Arc::new(with_physical_type(
                            with_source_name(
                                Field::new("optional", DataType::Int64, true),
                                "optional",
                            ),
                            "bson:int64",
                        )),
                    ]
                    .into(),
                ),
                false,
            ),
            "nested",
        ),
        "bson:document",
    );
    let schema = Arc::new(Schema::new(vec![nested]));
    let document = RawDocumentBuf::try_from(&doc! {
        "nested": {"known": 7_i32, "extra": "preserve"}
    })
    .unwrap();

    let decoded =
        decode_batch_with_evidence(Arc::clone(&schema), schema, &[document.as_ref()], 0).unwrap();

    assert_eq!(decoded.physical_reconciliations.len(), 1);
    assert_eq!(
        decoded.physical_reconciliations[0].source_path(),
        ["nested", "known"]
    );
    assert_eq!(decoded.residual_candidates.len(), 1);
    assert_eq!(
        decoded.residual_candidates[0].source_path(),
        ["nested", "extra"]
    );
}

#[test]
fn mixed_array_reconciliation_retains_exact_element_path() {
    let child = Arc::new(with_physical_type(
        Field::new("item", DataType::Int64, true),
        "bson:int64",
    ));
    let schema = Arc::new(Schema::new(vec![with_physical_type(
        with_source_name(Field::new("values", DataType::List(child), false), "values"),
        "bson:array",
    )]));
    let document = RawDocumentBuf::try_from(&doc! {"values": [1_i32, 2_i64]}).unwrap();

    let decoded =
        decode_batch_with_evidence(Arc::clone(&schema), schema, &[document.as_ref()], 0).unwrap();

    assert_eq!(decoded.physical_reconciliations.len(), 1);
    assert_eq!(
        decoded.physical_reconciliations[0].source_path(),
        ["values", "0"]
    );
    assert_eq!(
        physical_type(decoded.physical_reconciliations[0].observed_field()),
        Some("bson:int32")
    );
}

#[test]
fn non_finite_bson_doubles_preserve_exact_arrow_values() {
    let schema = Arc::new(Schema::new(vec![with_physical_type(
        Field::new("value", DataType::Float64, false),
        "bson:double",
    )]));
    let documents = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY]
        .into_iter()
        .map(|value| RawDocumentBuf::try_from(&doc! {"value": value}).unwrap())
        .collect::<Vec<_>>();
    let references = documents
        .iter()
        .map(RawDocumentBuf::as_ref)
        .collect::<Vec<_>>();

    let decoded = decode_batch_with_evidence(Arc::clone(&schema), schema, &references, 0).unwrap();
    let values = decoded
        .record_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Float64Array>()
        .unwrap();
    assert!(values.value(0).is_nan());
    assert_eq!(values.value(1), f64::INFINITY);
    assert_eq!(values.value(2), f64::NEG_INFINITY);
}

#[test]
fn sparse_wide_batches_fail_before_column_accumulator_growth() {
    let schema = Arc::new(Schema::new(
        (0..4_096)
            .map(|index| Field::new(format!("field_{index}"), DataType::Int64, true))
            .collect::<Vec<_>>(),
    ));
    let document = RawDocumentBuf::try_from(&doc! {"field_0": 1_i64}).unwrap();
    let documents = vec![document.as_ref(); 1_025];

    let error = decode_batch_with_evidence(Arc::clone(&schema), schema, &documents, 0).unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(
        error.message.contains("progressive decode bound"),
        "{error}"
    );
}

#[test]
fn wire_batches_are_partitioned_at_the_decode_allocation_boundary() {
    let schema = Schema::new(
        (0..4_096)
            .map(|index| Field::new(format!("field_{index}"), DataType::Int64, true))
            .collect::<Vec<_>>(),
    );
    let document = RawDocumentBuf::try_from(&doc! {"field_0": 1_i64}).unwrap();
    let documents = vec![document.as_ref(); 1_025];

    let admitted = maximum_safe_decode_prefix(&schema, &documents, documents.len()).unwrap();

    assert!(admitted > 0);
    assert!(admitted < documents.len());
    decode_batch_with_evidence(
        Arc::new(schema.clone()),
        Arc::new(schema),
        &documents[..admitted],
        0,
    )
    .unwrap();
}

#[test]
fn overlapping_source_paths_use_exact_payload_preflight() {
    let nested = with_source_name(
        Field::new(
            "nested_object",
            DataType::Struct(vec![Arc::new(Field::new("payload", DataType::Utf8, false))].into()),
            false,
        ),
        "nested",
    );
    let flat = with_source_name(
        Field::new("flat_payload", DataType::Utf8, false),
        "nested.payload",
    );
    let schema = Arc::new(Schema::new(vec![nested, flat]));
    let payload = "x".repeat(9 * 1024 * 1024);
    let document = RawDocumentBuf::try_from(&doc! {
        "nested": {"payload": payload}
    })
    .unwrap();
    let documents = [document.as_ref(), document.as_ref()];

    let error = decode_batch_with_evidence(Arc::clone(&schema), schema, &documents, 0).unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(
        error.message.contains("progressive decode bound"),
        "{error}"
    );
}

#[test]
fn discovery_caps_retained_nested_shape_across_documents() {
    let mut inference = SchemaInference::new(2).unwrap();
    let mut terminal = None;
    for index in 0..4_096 {
        let document = RawDocumentBuf::try_from(&doc! {
            "nested": {format!("field_{index}"): 1_i64}
        })
        .unwrap();
        if let Err(error) = inference.observe(document.as_ref()) {
            terminal = Some(error);
            break;
        }
    }
    let error = terminal.expect("retained nested discovery shape must be bounded");
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("retained schema bound"), "{error}");
}

#[test]
fn physical_catalog_attachment_enforces_decimal_semantics() {
    let observed = Schema::new(vec![with_physical_type(
        semantic_field(
            Field::new("amount", DataType::Utf8, true),
            MONGODB_DECIMAL_TEXT_SEMANTIC,
        ),
        "bson:decimal128",
    )]);
    let decimal = Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(18, 2),
        true,
    )]);
    assert_eq!(
        attach_expected_physical_types(&decimal, &observed)
            .unwrap()
            .field(0)
            .data_type(),
        &DataType::Decimal128(18, 2)
    );

    let plain_text = Schema::new(vec![Field::new("amount", DataType::Utf8, true)]);
    let error = attach_expected_physical_types(&plain_text, &observed).unwrap_err();
    assert!(error.message.contains("tagged-text semantic"), "{error}");
}

#[test]
fn decimal_materialization_rules_cover_logical_semantics_structs_and_lists() {
    let schema = Schema::new(vec![
        semantic_field(
            Field::new("amount", DataType::Decimal128(18, 2), false),
            "cdf.pii@1(class=\"financial\")",
        ),
        Field::new_struct(
            "profile",
            vec![Field::new(
                "nested_amount",
                DataType::Decimal128(20, 4),
                true,
            )],
            true,
        ),
        Field::new_list(
            "amounts",
            Field::new("item", DataType::Decimal128(12, 2), true),
            true,
        ),
    ]);

    let rules = compile_source_materializations(&schema).unwrap();
    cdf_kernel::validate_source_materializations(&rules, &schema).unwrap();
    assert_eq!(
        rules
            .iter()
            .map(|rule| rule.field_path.clone())
            .collect::<Vec<_>>(),
        vec![
            vec!["amount".to_owned()],
            vec!["amounts".to_owned(), "item".to_owned()],
            vec!["profile".to_owned(), "nested_amount".to_owned()],
        ]
    );
    for rule in &rules {
        assert_eq!(
            rule.required_observed_metadata
                .get(cdf_kernel::PHYSICAL_TYPE_METADATA_KEY)
                .map(String::as_str),
            Some("bson:decimal128")
        );
        assert_eq!(
            rule.required_observed_metadata
                .get(cdf_kernel::SEMANTIC_METADATA_KEY)
                .map(String::as_str),
            Some(MONGODB_DECIMAL_TEXT_SEMANTIC)
        );
    }
}

#[test]
fn nested_decimal_decode_publishes_logical_schema_and_physical_evidence() {
    let logical = Arc::new(Schema::new(vec![
        semantic_field(
            Field::new("amount", DataType::Decimal128(18, 2), false),
            "cdf.pii@1(class=\"financial\")",
        ),
        Field::new_struct(
            "profile",
            vec![Field::new(
                "nested_amount",
                DataType::Decimal128(18, 2),
                true,
            )],
            true,
        ),
        Field::new_list(
            "amounts",
            Field::new("item", DataType::Decimal128(18, 2), true),
            true,
        ),
    ]));
    let physical_decimal = |name: &str, nullable: bool| {
        with_physical_type(
            semantic_field(
                Field::new(name, DataType::Utf8, nullable),
                MONGODB_DECIMAL_TEXT_SEMANTIC,
            ),
            "bson:decimal128",
        )
    };
    let physical = Arc::new(Schema::new(vec![
        physical_decimal("amount", false),
        with_physical_type(
            Field::new_struct(
                "profile",
                vec![physical_decimal("nested_amount", true)],
                true,
            ),
            "bson:document",
        ),
        with_physical_type(
            Field::new_list("amounts", physical_decimal("item", true), true),
            "bson:array",
        ),
    ]));
    let decoder = attach_expected_physical_types(logical.as_ref(), physical.as_ref()).unwrap();
    let document = RawDocumentBuf::try_from(&doc! {
        "amount": Decimal128::from_str("12.34").unwrap(),
        "profile": {"nested_amount": Decimal128::from_str("56.78").unwrap()},
        "amounts": [
            Decimal128::from_str("9.00").unwrap(),
            Decimal128::from_str("10.01").unwrap(),
        ],
    })
    .unwrap();

    let decoded = decode_batch_with_physical_schema(
        Arc::clone(&decoder),
        decoder,
        Arc::clone(&logical),
        Arc::clone(&physical),
        &[document.as_ref()],
        0,
    )
    .unwrap();

    for index in 0..logical.fields().len() {
        assert_eq!(
            decoded.record_batch.schema().field(index).data_type(),
            logical.field(index).data_type()
        );
    }
    assert_eq!(decoded.physical_schema, physical.as_ref().clone());
    assert_eq!(
        decoded
            .record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        1_234
    );
    let profile = decoded
        .record_batch
        .column(1)
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    assert_eq!(
        profile
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value(0),
        5_678
    );
    let amounts = decoded
        .record_batch
        .column(2)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = amounts.values();
    let values = values.as_any().downcast_ref::<Decimal128Array>().unwrap();
    assert_eq!([values.value(0), values.value(1)], [900, 1_001]);
}

#[test]
fn decimal_zero_is_exact_at_every_pinned_scale() {
    for value in ["0", "-0", "+0.0", "0E-10", "-0.000E+20"] {
        assert_eq!(parse_decimal128(value, 38, 18).unwrap(), 0, "{value}");
    }
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
fn duplicate_nested_bson_keys_fail_before_residual_materialization() {
    let mut nested = RawDocumentBuf::new();
    nested.append(cstr!("known"), 1_i64);
    nested.append(cstr!("known"), 2_i64);
    let mut document = RawDocumentBuf::new();
    document.append(cstr!("nested"), nested);
    let schema = Arc::new(Schema::new(vec![with_source_name(
        Field::new(
            "nested",
            DataType::Struct(
                vec![Arc::new(with_source_name(
                    Field::new("known", DataType::Int64, false),
                    "known",
                ))]
                .into(),
            ),
            false,
        ),
        "nested",
    )]));

    let error = decode_batch_with_evidence(Arc::clone(&schema), schema, &[document.as_ref()], 0)
        .unwrap_err();

    assert!(error.message.contains("repeats field `known`"), "{error}");
}

#[test]
fn literal_dotted_bson_keys_fail_discovery_and_decode() {
    let mut document = RawDocumentBuf::new();
    document.append(cstr!("a.b"), 1_i64);

    let discovery_error = SchemaInference::default()
        .observe(document.as_ref())
        .unwrap_err();
    assert_eq!(discovery_error.kind, cdf_kernel::ErrorKind::Data);
    assert!(discovery_error.message.contains("literal dot"));

    let schema = Arc::new(Schema::new(vec![Field::new(
        "known",
        DataType::Int64,
        true,
    )]));
    let decode_error =
        decode_batch_with_evidence(Arc::clone(&schema), schema, &[document.as_ref()], 0)
            .unwrap_err();
    assert_eq!(decode_error.kind, cdf_kernel::ErrorKind::Data);
    assert!(decode_error.message.contains("literal dot"));
}

#[test]
fn duplicate_javascript_scope_keys_fail_before_residual_materialization() {
    let mut scope = RawDocumentBuf::new();
    scope.append(cstr!("binding"), 1_i64);
    scope.append(cstr!("binding"), 2_i64);
    let mut document = RawDocumentBuf::new();
    document.append(
        cstr!("script"),
        RawJavaScriptCodeWithScope {
            code: "return binding".to_owned(),
            scope,
        },
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "known",
        DataType::Int64,
        true,
    )]));

    let error = decode_batch_with_evidence(Arc::clone(&schema), schema, &[document.as_ref()], 0)
        .unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("repeats field `binding`"), "{error}");
}

#[test]
fn cursor_query_uses_numeric_frontier_and_object_id_tie_breaker() {
    let descriptor = descriptor(true);
    let schema = Arc::new(schema());
    let mut partition = PartitionPlan {
        partition_id: PartitionId::new("mongodb").unwrap(),
        scope: descriptor.state_scope.clone(),
        planned_position: None,
        start_position: None,
        scan_intent: CompiledScanIntent::full_scan(),
        retry_safety: PartitionRetrySafety::Forbidden,
        metadata: BTreeMap::from([
            ("kind".to_owned(), "mongodb".to_owned()),
            ("resource_id".to_owned(), descriptor.resource_id.to_string()),
            ("collection".to_owned(), "events".to_owned()),
        ]),
    };
    let committed = SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "sequence".to_owned(),
        value: CursorValue::I64(41),
    });
    rebind_mongodb_partition_for_resume(&descriptor, &mut partition, &committed).unwrap();
    let collection = MongoDbIdentifier::new("events").unwrap();
    let scan = scan_from_partition(&descriptor, &schema, &collection, &partition).unwrap();
    let query = build_query(&descriptor, &schema, &partition, &scan).unwrap();

    assert_eq!(query.filter, doc! {"sequence": {"$gt": 41_i64}});
    assert_eq!(
        query.projection,
        doc! {"_id": 1_i32, "sequence": 1_i32, "amount": 1_i32, "observed_at": 1_i32}
    );
    assert_eq!(query.sort, doc! {"sequence": 1_i32, "_id": 1_i32});
    assert_eq!(query.limit, None);
}

#[test]
fn cursorless_snapshot_query_uses_stable_object_id_order() {
    let descriptor = descriptor(false);
    let schema = Arc::new(schema());
    let partition = PartitionPlan {
        partition_id: PartitionId::new("mongodb").unwrap(),
        scope: descriptor.state_scope.clone(),
        planned_position: None,
        start_position: None,
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

    assert_eq!(
        query.projection,
        doc! {"_id": 1_i32, "sequence": 1_i32, "amount": 1_i32, "observed_at": 1_i32}
    );
    assert_eq!(query.sort, doc! {"_id": 1_i32});
}

#[test]
fn cursorless_snapshot_has_deterministic_full_scan_completion_authority() {
    let descriptor = descriptor(false);
    let database = MongoDbIdentifier::new("warehouse").unwrap();
    let collection = MongoDbIdentifier::new("events").unwrap();
    let mut partition = PartitionPlan {
        partition_id: PartitionId::new("mongodb").unwrap(),
        scope: descriptor.state_scope.clone(),
        planned_position: None,
        start_position: None,
        scan_intent: CompiledScanIntent::full_scan(),
        retry_safety: PartitionRetrySafety::Forbidden,
        metadata: BTreeMap::new(),
    };
    let first = full_scan_completion_position(
        &descriptor,
        &database,
        &collection,
        &partition,
        "sha256:native-a",
    )
    .unwrap();
    let repeated = full_scan_completion_position(
        &descriptor,
        &database,
        &collection,
        &partition,
        "sha256:native-a",
    )
    .unwrap();

    first.validate().unwrap();
    assert_eq!(first, repeated);
    partition.start_position = Some(first.clone());
    rebind_mongodb_partition_for_resume(&descriptor, &mut partition, &first).unwrap();
    assert!(partition.start_position.is_none());

    let SourcePosition::ForeignState(first_state) = first.clone() else {
        panic!("full scan must use explicit foreign-state completion authority");
    };
    assert_eq!(first_state.protocol, MONGODB_FULL_SCAN_COMPLETION_PROTOCOL);

    partition.scan_intent.limit = Some(1);
    let limited = full_scan_completion_position(
        &descriptor,
        &database,
        &collection,
        &partition,
        "sha256:native-b",
    )
    .unwrap();
    assert_ne!(first, limited);
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

    for (code, expected_kind) in [
        (2, cdf_kernel::ErrorKind::Contract),
        (14, cdf_kernel::ErrorKind::Contract),
        (20, cdf_kernel::ErrorKind::Contract),
        (13, cdf_kernel::ErrorKind::Auth),
        (18, cdf_kernel::ErrorKind::Auth),
        (26, cdf_kernel::ErrorKind::Data),
        (6, cdf_kernel::ErrorKind::Transient),
        (7, cdf_kernel::ErrorKind::Transient),
        (89, cdf_kernel::ErrorKind::Transient),
        (91, cdf_kernel::ErrorKind::Transient),
        (189, cdf_kernel::ErrorKind::Transient),
        (262, cdf_kernel::ErrorKind::Transient),
        (9001, cdf_kernel::ErrorKind::Transient),
        (50, cdf_kernel::ErrorKind::Transient),
        (16500, cdf_kernel::ErrorKind::RateLimited),
    ] {
        let command: mongodb::error::CommandError = serde_json::from_value(serde_json::json!({
            "code": code,
            "codeName": "fixture",
            "errmsg": "must remain redacted",
        }))
        .unwrap();
        let classified = classify_mongodb_error(
            "read collection",
            mongodb::error::ErrorKind::Command(command).into(),
        );
        assert_eq!(classified.kind, expected_kind, "command code {code}");
        assert!(
            classified
                .message
                .contains(&format!("code {code} (fixture)"))
        );
        assert!(!classified.message.contains("must remain redacted"));
    }

    let unsafe_code_name: mongodb::error::CommandError =
        serde_json::from_value(serde_json::json!({
            "code": 2,
            "codeName": "unsafe pipeline $secret",
            "errmsg": "must remain redacted",
        }))
        .unwrap();
    let classified = classify_mongodb_error(
        "open change stream",
        mongodb::error::ErrorKind::Command(unsafe_code_name).into(),
    );
    assert!(classified.message.contains("code 2"));
    assert!(!classified.message.contains("$secret"));
    assert!(!classified.message.contains("must remain redacted"));

    let shutdown = classify_mongodb_error(
        "read collection",
        mongodb::error::ErrorKind::Shutdown.into(),
    );
    assert_eq!(shutdown.kind, cdf_kernel::ErrorKind::Internal);
    let sessions = classify_mongodb_error(
        "read collection",
        mongodb::error::ErrorKind::SessionsNotSupported.into(),
    );
    assert_eq!(sessions.kind, cdf_kernel::ErrorKind::Contract);
}

#[test]
fn timestamp_cursor_overflow_fails_without_saturating() {
    let field = Field::new(
        "observed_at",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        false,
    );
    let values = TimestampMillisecondArray::from(vec![i64::MAX]);
    let error = cursor_value(&field, &values, 0).unwrap_err();
    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(
        error
            .message
            .contains("durable microsecond position domain")
    );
}
