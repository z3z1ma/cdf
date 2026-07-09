use super::*;
use crate::internal::*;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{
    DataType, Field, Fields, IntervalUnit, Schema, TimeUnit, UnionFields, UnionMode,
};
use cdf_contract::{ContractPolicy, ObservedSchema, compile_validation_program};
use cdf_declarative::{
    AuthDeclaration, CompiledResourcePlan, FileRuntimeDependencies, FileTransportFacade,
    HttpFileRequest, HttpFileResponse, HttpFileTransport, SourceDeclaration,
};
use cdf_engine::{EnginePlan, EnginePlanInput, PlanBoundedness, Planner};
use cdf_http::{
    HttpMethod, HttpRequest, HttpResponse, HttpTransport, SecretProvider, SecretUri, SecretValue,
};
use cdf_kernel::{
    CapabilitySupport, CdfError, CheckpointId, ConcurrencyLimit, DestinationId, DestinationSheet,
    IdempotencySupport, IdentifierRules, PipelineId, QueryableResource, ResourceId, ResourceStream,
    RunId, ScanRequest, SchemaSource, SourcePosition, TargetName, TransactionSupport, TypeMapping,
    TypeMappingFidelity, WriteDisposition, source_name,
};

const BOOK_PROJECT: &str = r#"
[project]
name = "acme_data"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"
retention = { default = "5 runs" }

[environments.prod]
destination = "postgres://secret://env/PROD_DWH"
retention = { default = "90d", financial = "400d" }

[environments.prod.destination_policy.postgres]
merge_dedup = "fail"

[python]
interpreter = ".venv/bin/python"

[defaults]
contract = "governed"

[resources."github.*"]
source = "resources/github.toml"

[resources."events.raw"]
source = "python://src/events.py#raw_events"
trust = "serving"
freshness = { expect_every = "15m", alert_after = "45m" }
"#;

const GITHUB_RESOURCE: &str = r#"
[source.github]
kind = "rest"
base_url = "https://api.github.com"
auth = { kind = "bearer", token = "secret://env/GITHUB_TOKEN" }

[resource.issues]
path = "/repos/{owner}/{repo}/issues"
records = "$"
primary_key = ["id"]
merge_key = ["id"]
cursor = { field = "updated_at", param = "since", ordering = "best_effort", lag = "5m" }
write_disposition = "merge"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "timestamp_micros", nullable = false, timezone = "UTC" },
] }
"#;

#[test]
fn book_project_shape_parses_into_typed_models() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();

    assert_eq!(config.project.name, "acme_data");
    assert_eq!(
        config.python.interpreter.as_deref(),
        Some(".venv/bin/python")
    );
    assert_eq!(config.defaults.contract.as_deref(), Some("governed"));
    assert_eq!(
        config.resources["events.raw"]
            .freshness
            .as_ref()
            .unwrap()
            .alert_after
            .unwrap()
            .millis(),
        2_700_000
    );
    assert_eq!(
        config.environments["dev"]
            .retention
            .as_ref()
            .unwrap()
            .default,
        Some(RetentionRule::Runs(5))
    );
}

#[test]
fn environment_overlays_inherit_unspecified_settings() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let prod = config.effective_environment("prod").unwrap();

    assert_eq!(prod.state, "sqlite://.cdf/state.db");
    assert_eq!(prod.packages, ".cdf/packages");
    assert_eq!(prod.destination, "postgres://secret://env/PROD_DWH");
    assert_eq!(
        prod.retention.as_ref().unwrap().default,
        Some(RetentionRule::Duration(DurationSpec::from_millis(
            90 * 86_400_000
        )))
    );
    assert_eq!(
        prod.retention.as_ref().unwrap().financial,
        Some(RetentionRule::Duration(DurationSpec::from_millis(
            400 * 86_400_000
        )))
    );
    assert_eq!(
        prod.destination_policy
            .postgres
            .as_ref()
            .unwrap()
            .merge_dedup,
        PostgresMergeDedupPolicy::Fail
    );
}

#[test]
fn destination_policy_overlays_from_default_environment() {
    let project = BOOK_PROJECT
        .replace(
            "[environments.prod.destination_policy.postgres]\nmerge_dedup = \"fail\"\n\n",
            "",
        )
        .replace(
            "retention = { default = \"5 runs\" }\n\n",
            "retention = { default = \"5 runs\" }\n\n[environments.dev.destination_policy.postgres]\nmerge_dedup = \"fail\"\n\n",
        );
    let config = parse_cdf_toml(&project).unwrap();
    let prod = config.effective_environment("prod").unwrap();

    assert_eq!(
        prod.destination_policy
            .postgres
            .as_ref()
            .unwrap()
            .merge_dedup,
        PostgresMergeDedupPolicy::Fail
    );
}

#[test]
fn validation_resolves_declarative_sources_and_redacts_secret_values() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let provider = DefaultSecretProvider::new(
        EnvSecretProvider::from_map([
            ("GITHUB_TOKEN", "github-token-value"),
            ("PROD_DWH", "postgres-dsn-value"),
        ]),
        FileSecretProvider::without_root(),
    );

    let report = validate_project(&config, Some("prod"), &resolver, &provider).unwrap();

    assert_eq!(report.declarative_resources, 1);
    assert_eq!(report.external_resources, 1);
    assert_eq!(report.checked_secrets.len(), 2);
    let debug = format!("{report:?}");
    assert!(!debug.contains("github-token-value"));
    assert!(!debug.contains("postgres-dsn-value"));
    assert!(debug.contains("secret://env/GITHUB_TOKEN"));
}

#[test]
fn validation_checks_missing_secret_without_printing_values() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let provider = EnvSecretProvider::from_map([("GITHUB_TOKEN", "github-token-value")]);

    let error = validate_project(&config, Some("prod"), &resolver, &provider).unwrap_err();

    assert!(error.to_string().contains("secret://env/PROD_DWH"));
    assert!(!error.to_string().contains("github-token-value"));
}

#[test]
fn plaintext_secret_values_are_rejected_where_references_are_required() {
    let bad_resource = GITHUB_RESOURCE.replace("secret://env/GITHUB_TOKEN", "plain-token-value");
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", bad_resource);
    let provider = EnvSecretProvider::from_map([("PROD_DWH", "postgres-dsn-value")]);

    let error = validate_project(&config, Some("prod"), &resolver, &provider).unwrap_err();

    assert!(error.to_string().contains("secret://"));
    assert!(!error.to_string().contains("plain-token-value"));
}

#[test]
fn file_secret_provider_resolves_without_exposing_contents() {
    let root = env::temp_dir().join(format!("cdf-project-secret-test-{}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("api-token"), "file-secret-value\n").unwrap();
    let provider = FileSecretProvider::new(&root);
    let uri = SecretUri::new("secret://file/api-token").unwrap();

    let value = provider.resolve(&uri).unwrap();

    assert_eq!(value.as_str().unwrap(), "file-secret-value");
    assert_eq!(format!("{value:?}"), "[REDACTED]");
    assert_eq!(format!("{value}"), "[REDACTED]");
    let _ = fs::remove_file(root.join("api-token"));
    let _ = fs::remove_dir(root);
}

#[test]
fn lockfile_generation_round_trips_and_diffs_semantic_changes() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let resources = compile_project_declarative_resources(&config, &resolver).unwrap();
    let sheet = destination_sheet("duckdb", TypeMappingFidelity::Lossless);
    let dependency_tuple = DependencyTuple {
        cdf: "0.1.0".to_owned(),
        arrow_rs: "59.1.0".to_owned(),
        datafusion: Some("54.0.0".to_owned()),
        object_store: None,
        duckdb_rs: None,
        rust: None,
    };

    let lock = generate_lockfile(
        &config,
        &resources,
        dependency_tuple.clone(),
        std::slice::from_ref(&sheet),
        BTreeMap::new(),
    )
    .unwrap();
    let encoded = lock_to_toml(&lock).unwrap();
    let decoded = parse_lock(&encoded).unwrap();
    assert_eq!(decoded, lock);
    assert_eq!(lock.normalizer, NORMALIZER_NAMECASE_V1);
    let resource = lock.resources.get("github.issues").unwrap();
    assert!(resource.capability_sheet_hash.starts_with("sha256:"));
    assert!(
        resource
            .schema_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    let contract = resource.contract.as_ref().unwrap();
    assert!(
        contract
            .policy_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        contract
            .validation_program_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(
        lock.destinations["duckdb"].sheet.type_mappings[0].fidelity,
        TypeMappingFidelity::Lossless
    );

    let changed = generate_lockfile(
        &config,
        &resources,
        dependency_tuple,
        &[destination_sheet(
            "duckdb",
            TypeMappingFidelity::LossyRequiresContractAllowance,
        )],
        BTreeMap::new(),
    )
    .unwrap();
    let diffs = diff_lockfiles(&lock, &changed).unwrap();

    assert!(diffs.iter().any(|diff| diff.path.contains("sheet_hash")));
    assert!(diffs.iter().any(|diff| {
        diff.path
            .contains("destinations.duckdb.sheet.type_mappings")
    }));
}

#[test]
fn schema_snapshot_artifact_uses_deterministic_hash_and_project_path() {
    let resource_id = ResourceId::new("github.issues").unwrap();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new(
            "payload",
            DataType::Struct(Fields::from(vec![Field::new(
                "source",
                DataType::Utf8,
                true,
            )])),
            true,
        ),
    ]);
    let metadata = BTreeMap::from([
        (
            "cdf:normalizer".to_owned(),
            NORMALIZER_NAMECASE_V1.to_owned(),
        ),
        ("probe".to_owned(), "parquet-footer".to_owned()),
    ]);

    let artifact = SchemaSnapshotArtifact::new(&resource_id, &schema, metadata.clone()).unwrap();
    let repeated = SchemaSnapshotArtifact::new(&resource_id, &schema, metadata).unwrap();

    assert_eq!(artifact.schema_hash, repeated.schema_hash);
    assert_eq!(artifact.schema.to_arrow().unwrap(), schema);
    assert_eq!(
        artifact.path,
        format!(".cdf/schemas/github.issues@{}.json", artifact.schema_hash)
    );
    assert_eq!(artifact.hash_input["resource_id"], "github.issues");
    assert_eq!(artifact.hash_input["metadata"]["probe"], "parquet-footer");
    assert_eq!(
        artifact.hash_input["schema"]["fields"][2]["data_type"]["kind"],
        "struct"
    );
    assert_eq!(
        artifact.hash_input["schema"]["fields"][2]["data_type"]["fields"][0]["name"],
        "source"
    );

    let temp = tempfile::tempdir().unwrap();
    let store = SchemaSnapshotStore::new(temp.path());
    let path = store.write(&artifact).unwrap();
    assert_eq!(path, temp.path().join(&artifact.path));
    assert_eq!(store.read(&artifact.reference()).unwrap(), artifact);

    let mut tampered = artifact.clone();
    tampered
        .metadata
        .insert("probe".to_owned(), "changed".to_owned());
    assert!(tampered.validate_hash_input().is_err());

    let mut escaped = artifact.reference();
    escaped.path = "../outside.json".to_owned();
    let error = store.read(&escaped).unwrap_err().to_string();
    assert!(error.contains("schema snapshot reference path"));
}

#[test]
fn schema_snapshot_arrow_round_trip_covers_closed_type_vocabulary() {
    let union = UnionFields::try_new(
        [1, 3],
        [
            Field::new("integer", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
        ],
    )
    .unwrap();
    let fields = vec![
        Field::new("decimal", DataType::Decimal256(76, 9), true),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "interval",
            DataType::Interval(IntervalUnit::MonthDayNano),
            true,
        ),
        Field::new("binary_view", DataType::BinaryView, true),
        Field::new("utf8_view", DataType::Utf8View, true),
        Field::new(
            "large_list_view",
            DataType::LargeListView(Field::new("item", DataType::UInt16, true).into()),
            true,
        ),
        Field::new(
            "map",
            DataType::Map(
                Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Float32, true),
                    ])),
                    false,
                )
                .into(),
                false,
            ),
            true,
        ),
        Field::new("union", DataType::Union(union, UnionMode::Dense), true),
        Field::new(
            "dictionary",
            DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::LargeUtf8)),
            true,
        ),
        Field::new(
            "run_end_encoded",
            DataType::RunEndEncoded(
                Field::new("run_ends", DataType::Int32, false).into(),
                Field::new("values", DataType::Utf8, true).into(),
            ),
            true,
        ),
    ];
    let schema = Schema::new(fields);

    assert_eq!(
        SchemaSnapshotSchema::from_arrow(&schema)
            .to_arrow()
            .unwrap(),
        schema
    );
}

#[test]
fn local_parquet_discovery_handoff_builds_deterministic_snapshot() {
    let resource_id = ResourceId::new("tlc.yellow").unwrap();
    let schema = Schema::new(vec![
        Field::new("VendorID", DataType::Int32, true),
        Field::new(
            "tpep_pickup_datetime",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
    ]);
    let source_identity = BTreeMap::from([
        ("footer_sha256".to_owned(), "sha256:footer".to_owned()),
        ("size_bytes".to_owned(), "123".to_owned()),
        (
            "local_path".to_owned(),
            "/tmp/private/orders.parquet".to_owned(),
        ),
    ]);

    let handoff =
        schema_snapshot_from_parquet_footer_schema(&resource_id, &schema, source_identity.clone())
            .unwrap();
    let repeated =
        schema_snapshot_from_parquet_footer_schema(&resource_id, &schema, source_identity).unwrap();

    assert_eq!(handoff.artifact, repeated.artifact);
    assert_eq!(handoff.reference, handoff.artifact.reference());
    assert_eq!(
        handoff.source_identity["local_path"],
        "/tmp/private/orders.parquet"
    );
    assert_eq!(
        handoff.artifact.metadata["probe"],
        SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER
    );
    assert_eq!(
        handoff.artifact.metadata["format"],
        SCHEMA_DISCOVERY_FORMAT_PARQUET
    );
    assert_eq!(
        handoff.artifact.path,
        format!(
            ".cdf/schemas/tlc.yellow@{}.json",
            handoff.artifact.schema_hash
        )
    );
    assert_eq!(
        handoff.artifact.hash_input["metadata"]["format"],
        SCHEMA_DISCOVERY_FORMAT_PARQUET
    );

    let hash_input = serde_json::to_string(&handoff.artifact.hash_input).unwrap();
    assert!(!hash_input.contains("/tmp/private/orders.parquet"));
    assert!(!hash_input.contains("sha256:footer"));

    let temp = tempfile::tempdir().unwrap();
    let store = SchemaSnapshotStore::new(temp.path());
    store.write(&handoff.artifact).unwrap();
    assert_eq!(store.read(&handoff.reference).unwrap(), handoff.artifact);
}

#[test]
fn local_parquet_discover_autopin_writes_normalized_snapshot_and_pins_clone() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/vendors.parquet"));
    let resource = compile_single_project_resource(temp.path());

    let prepared = prepare_local_parquet_discover_resource(temp.path(), &resource).unwrap();
    let discovery = prepared.discovery.as_ref().unwrap();
    let snapshot_path = temp.path().join(&discovery.snapshot.artifact.path);

    assert!(matches!(
        resource.descriptor().schema_source,
        SchemaSource::Discover
    ));
    assert!(snapshot_path.is_file());
    assert_eq!(
        discovery.snapshot.artifact.metadata["cdf:normalizer"],
        NORMALIZER_NAMECASE_V1
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].name,
        "vendor_id"
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].metadata["cdf:source_name"],
        "VendorID"
    );
    let SchemaSource::Discovered { snapshot } = &prepared.resource.descriptor().schema_source
    else {
        panic!("expected auto-pinned discovered schema source");
    };
    assert_eq!(
        snapshot.schema_hash,
        discovery.snapshot.artifact.schema_hash
    );
    assert_eq!(snapshot.path, discovery.snapshot.artifact.path);
    let schema = prepared.resource.schema();
    let vendor = schema.field_with_name("vendor_id").unwrap();
    assert_eq!(source_name(vendor), Some("VendorID"));

    let repeated = prepare_local_parquet_discover_resource(temp.path(), &resource).unwrap();
    assert_eq!(
        repeated
            .discovery
            .as_ref()
            .unwrap()
            .snapshot
            .artifact
            .schema_hash,
        discovery.snapshot.artifact.schema_hash
    );
}

#[test]
fn generic_schema_discovery_dispatch_preserves_local_parquet_behavior_without_writes() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/vendors.parquet"));
    let resource = compile_single_project_resource(temp.path());

    let discovery = discover_resource_schema(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
    )
    .unwrap();

    assert!(!temp.path().join(".cdf/schemas").exists());
    assert_eq!(
        discovery.snapshot.artifact.metadata["probe"],
        SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER
    );
    assert_eq!(
        discovery.snapshot.artifact.metadata["format"],
        SCHEMA_DISCOVERY_FORMAT_PARQUET
    );
    assert_eq!(
        discovery.snapshot.artifact.metadata["cdf:normalizer"],
        NORMALIZER_NAMECASE_V1
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].name,
        "vendor_id"
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].metadata["cdf:source_name"],
        "VendorID"
    );
    assert_eq!(
        discovery.snapshot.source_identity["path"],
        "vendors.parquet"
    );
    assert!(
        discovery
            .snapshot
            .source_identity
            .contains_key("footer_sha256")
    );
}

#[test]
fn generic_discover_prepare_preserves_local_parquet_autopin_behavior() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/vendors.parquet"));
    let resource = compile_single_project_resource(temp.path());
    let secret_provider = EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>());

    let prepared = prepare_discover_resource(temp.path(), &resource, &secret_provider).unwrap();
    let discovery = prepared.discovery.as_ref().unwrap();
    let snapshot_path = temp.path().join(&discovery.snapshot.artifact.path);

    assert!(snapshot_path.is_file());
    assert_eq!(
        discovery.snapshot.artifact.metadata["probe"],
        SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].name,
        "vendor_id"
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].metadata["cdf:source_name"],
        "VendorID"
    );
    let SchemaSource::Discovered { snapshot } = &prepared.resource.descriptor().schema_source
    else {
        panic!("expected generic auto-pin to set discovered schema source");
    };
    assert_eq!(
        snapshot.schema_hash,
        discovery.snapshot.artifact.schema_hash
    );
}

#[test]
fn http_parquet_schema_discovery_uses_bounded_ranges_without_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = vendor_parquet_bytes_with_rows(10_000);
    assert!(parquet.len() > 16 * 1024);
    write_http_discover_project(temp.path(), "");
    let resource = compile_single_project_resource(temp.path());
    let transport = RecordingHttpFileTransport::new(parquet.clone());
    let dependencies = http_file_dependencies(transport.clone());

    let discovery = discover_resource_schema_with_file_dependencies(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies,
    )
    .unwrap();

    assert!(!temp.path().join(".cdf/schemas").exists());
    assert!(!temp.path().join(".cdf/packages").exists());
    assert!(!temp.path().join(".cdf/state.db").exists());
    assert_eq!(
        discovery.snapshot.artifact.metadata["probe"],
        SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER
    );
    assert_eq!(discovery.snapshot.artifact.metadata["source_kind"], "files");
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].name,
        "vendor_id"
    );
    assert_eq!(
        discovery.snapshot.artifact.schema.fields[0].metadata["cdf:source_name"],
        "VendorID"
    );
    assert_eq!(
        discovery.snapshot.source_identity["url"],
        "https://data.example.test/trip-data/vendors.parquet"
    );
    assert_eq!(
        discovery.snapshot.source_identity["size_bytes"],
        parquet.len().to_string()
    );
    assert_eq!(
        discovery.snapshot.source_identity["etag"],
        "\"fixture-etag\""
    );
    assert_eq!(discovery.snapshot.source_identity["row_count"], "10000");
    assert!(discovery.snapshot.source_identity["footer_sha256"].starts_with("sha256:"));
    let requests = transport.requests();
    assert_only_bounded_http_file_gets(&requests);
    assert_http_file_gets_download_less_than_fixture(&requests, parquet.len());
}

#[test]
fn http_parquet_auto_pin_plan_preview_and_run_use_file_runtime() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = vendor_parquet_bytes();
    write_http_discover_project(temp.path(), "");
    let resource = compile_single_project_resource(temp.path());
    let transport = RecordingHttpFileTransport::new(parquet.clone());
    let dependencies = http_file_dependencies(transport.clone());
    let secret_provider = EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>());

    let prepared = prepare_discover_resource_with_file_dependencies(
        temp.path(),
        &resource,
        &secret_provider,
        dependencies.clone(),
    )
    .unwrap();
    let discovery = prepared.discovery.as_ref().unwrap();
    assert!(
        temp.path()
            .join(&discovery.snapshot.artifact.path)
            .is_file()
    );
    let SchemaSource::Discovered { snapshot } = &prepared.resource.descriptor().schema_source
    else {
        panic!("expected HTTP Parquet auto-pin to set discovered schema source");
    };
    assert_eq!(
        snapshot.schema_hash,
        discovery.snapshot.artifact.schema_hash
    );

    let file_resource = prepared
        .resource
        .to_file_resource(dependencies.clone())
        .unwrap();
    let plan = live_plan_for_stream(&file_resource, "pkg-http-parquet-runtime");
    assert_eq!(plan.scan.partitions.len(), 1);
    let partition = plan.scan.partitions[0].clone();
    assert_eq!(
        partition.metadata["path"],
        "https://data.example.test/trip-data/vendors.parquet"
    );
    assert_eq!(partition.metadata["bytes"], parquet.len().to_string());
    assert_eq!(partition.metadata["etag"], "\"fixture-etag\"");
    assert_eq!(
        partition.metadata["bytes_loaded"],
        parquet.len().to_string()
    );

    let preview_stream = futures_executor::block_on(file_resource.open_preview(partition)).unwrap();
    let preview_rows = futures_executor::block_on_stream(preview_stream)
        .map(|batch| batch.unwrap().header.row_count)
        .sum::<u64>();
    assert_eq!(preview_rows, 2);

    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::file(&file_resource),
        plan,
        package_root: temp.path().join(".cdf/packages"),
        state_store_path: temp.path().join(".cdf/state.db"),
        pipeline_id: PipelineId::new("pipeline-http").unwrap(),
        package_id: "pkg-http-parquet-runtime".to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-http-parquet-runtime").unwrap(),
        destination: ResolvedProjectDestination::duckdb(
            duckdb_path,
            TargetName::new("events").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-http-parquet-runtime").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    assert_eq!(report.row_count, 2);
    assert_eq!(report.segment_count, 1);
    let SourcePosition::FileManifest(manifest) = &report.checkpoint.delta.output_position else {
        panic!("checkpoint output position should be a file manifest");
    };
    assert_eq!(manifest.files.len(), 1);
    assert_eq!(
        manifest.files[0].path,
        "https://data.example.test/trip-data/vendors.parquet"
    );
    assert_eq!(manifest.files[0].size_bytes, parquet.len() as u64);
    assert_eq!(manifest.files[0].etag.as_deref(), Some("\"fixture-etag\""));
    assert_eq!(manifest.files[0].sha256, None);
    assert_only_bounded_http_file_gets(&transport.requests());
}

#[test]
fn http_file_discovery_egress_and_auth_fail_before_transport_use() {
    let temp = tempfile::tempdir().unwrap();
    let parquet = vendor_parquet_bytes();
    write_http_discover_project(temp.path(), r#"egress_allowlist = ["other.example.test"]"#);
    let resource = compile_single_project_resource(temp.path());
    let transport = RecordingHttpFileTransport::new(parquet.clone());
    let dependencies = http_file_dependencies(transport.clone());

    let error = discover_resource_schema_with_file_dependencies(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies,
    )
    .unwrap_err();

    assert!(error.to_string().contains("egress"));
    assert!(transport.requests().is_empty());

    let auth_temp = tempfile::tempdir().unwrap();
    write_http_discover_project(
        auth_temp.path(),
        r#"
auth = { kind = "bearer", token = "secret://env/HTTP_TOKEN" }
egress_allowlist = ["data.example.test"]
"#,
    );
    let auth_resource = compile_single_project_resource(auth_temp.path());
    let auth_transport = RecordingHttpFileTransport::new(parquet);
    let auth_dependencies = FileRuntimeDependencies::new(
        FileTransportFacade::new()
            .with_http_transport(auth_transport.clone())
            .with_secret_provider(StaticSecretProvider::new([(
                "secret://env/HTTP_TOKEN",
                "super-secret-http-token",
            )])),
    );

    let auth_error = discover_resource_schema_with_file_dependencies(
        &auth_resource,
        &StaticSecretProvider::new([("secret://env/HTTP_TOKEN", "super-secret-http-token")]),
        auth_dependencies,
    )
    .unwrap_err();
    let message = auth_error.to_string();
    assert!(message.contains("credential resolution is not implemented"));
    assert!(!message.contains("super-secret-http-token"));
    assert!(auth_transport.requests().is_empty());
}

#[test]
fn local_parquet_discover_autopin_leaves_declared_resources_unprobed() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.missing");
    let declared = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "*.missing"
format = "parquet"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "VendorID", type = "int32", nullable = false },
] }
"#;
    fs::write(temp.path().join("resources/files.toml"), declared).unwrap();
    let resource = compile_single_project_resource(temp.path());

    let prepared = prepare_local_parquet_discover_resource(temp.path(), &resource).unwrap();

    assert!(prepared.discovery.is_none());
    assert!(matches!(
        prepared.resource.descriptor().schema_source,
        SchemaSource::Declared { .. }
    ));
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn local_parquet_discover_autopin_rejects_non_parquet_without_snapshot_write() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "ndjson", "*.ndjson");
    let resource = compile_single_project_resource(temp.path());

    let error = prepare_local_parquet_discover_resource(temp.path(), &resource).unwrap_err();

    let message = error.to_string();
    assert!(message.contains("unsupported schema discovery slice"));
    assert!(message.contains("format"));
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn local_parquet_discover_autopin_rejects_multi_file_glob_without_snapshot_write() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/a.parquet"));
    write_vendor_parquet(&temp.path().join("data/b.parquet"));
    let resource = compile_single_project_resource(temp.path());

    let error = prepare_local_parquet_discover_resource(temp.path(), &resource).unwrap_err();

    let message = error.to_string();
    assert!(message.contains("multi-file Parquet discovery is unsupported"));
    assert!(message.contains("resolved to 2 files"));
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn generic_schema_discovery_dispatch_samples_rest_without_snapshot_write() {
    let temp = tempfile::tempdir().unwrap();
    let project = r#"
[project]
name = "api"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."api.*"]
source = "resources/api.toml"
"#;
    let rest = r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"
auth = { kind = "bearer", token = "secret://env/API_TOKEN" }
egress_allowlist = ["api.example.test"]

[resource.items]
path = "/items"
records = "$.items"
cursor = { field = "updated_at", param = "since", ordering = "exact", lag = "0ms" }
write_disposition = "append"
trust = "governed"
"#;
    let config = parse_cdf_toml(project).unwrap();
    let resolver = InMemoryResourceSourceResolver::new().with_toml("resources/api.toml", rest);
    let mut resources = compile_project_declarative_resources(&config, &resolver).unwrap();
    let resource = resources.remove(0);
    let mut transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "VendorID": 1, "updated_at": 10, "active": true, "score": 4.5 },
            { "VendorID": 2, "updated_at": 20, "active": false, "score": null },
            { "VendorID": 3, "updated_at": 30, "active": true }
        ] }"#,
    )]);
    let secret_provider =
        StaticSecretProvider::new([("secret://env/API_TOKEN", "rest-discover-secret")]);

    let discovery =
        discover_resource_schema_with_rest_transport(&resource, &secret_provider, &mut transport)
            .unwrap();

    assert!(!temp.path().join(".cdf/schemas").exists());
    assert_eq!(
        discovery.snapshot.artifact.metadata["probe"],
        "rest-sample-page"
    );
    assert_eq!(discovery.snapshot.artifact.metadata["source_kind"], "rest");
    assert_eq!(
        discovery.snapshot.artifact.metadata["cdf:normalizer"],
        NORMALIZER_NAMECASE_V1
    );
    assert!(
        discovery
            .snapshot
            .artifact
            .schema
            .fields
            .iter()
            .any(|field| field.name == "active")
    );
    let score = discovery
        .snapshot
        .artifact
        .schema
        .fields
        .iter()
        .find(|field| field.name == "score")
        .unwrap();
    assert!(score.nullable);
    assert!(
        discovery
            .snapshot
            .artifact
            .schema
            .fields
            .iter()
            .any(|field| field.name == "updated_at")
    );
    let vendor = discovery
        .snapshot
        .artifact
        .schema
        .fields
        .iter()
        .find(|field| field.name == "vendor_id")
        .unwrap();
    assert_eq!(vendor.metadata["cdf:source_name"], "VendorID");
    assert_eq!(discovery.snapshot.source_identity["source_kind"], "rest");
    assert_eq!(discovery.snapshot.source_identity["path"], "/items");
    assert_eq!(discovery.snapshot.source_identity["sample_pages"], "1");
    assert_eq!(discovery.snapshot.source_identity["sample_records"], "3");

    let requests = transport.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].url, "https://api.example.test/items");
    assert_eq!(
        requests[0].headers.get("authorization").map(String::as_str),
        Some("Bearer rest-discover-secret")
    );
    let artifact_text = serde_json::to_string(&discovery.snapshot.artifact).unwrap();
    assert!(!artifact_text.contains("rest-discover-secret"));
}

#[test]
fn generic_discover_prepare_autopins_rest_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    let project = r#"
[project]
name = "api"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."api.*"]
source = "resources/api.toml"
"#;
    let rest = r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"

[resource.items]
path = "/items"
records = "$.items"
cursor = { field = "updated_at", param = "since", ordering = "exact", lag = "0ms" }
write_disposition = "append"
trust = "governed"
"#;
    let config = parse_cdf_toml(project).unwrap();
    let resolver = InMemoryResourceSourceResolver::new().with_toml("resources/api.toml", rest);
    let mut resources = compile_project_declarative_resources(&config, &resolver).unwrap();
    let resource = resources.remove(0);
    let mut transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "VendorID": 1, "updated_at": 10 },
            { "VendorID": 2, "updated_at": 20 }
        ] }"#,
    )]);
    let secret_provider = EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>());

    let prepared = prepare_discover_resource_with_rest_transport(
        temp.path(),
        &resource,
        &secret_provider,
        &mut transport,
    )
    .unwrap();

    let discovery = prepared.discovery.as_ref().unwrap();
    let snapshot_path = temp.path().join(&discovery.snapshot.artifact.path);
    assert!(snapshot_path.is_file());
    let SchemaSource::Discovered { snapshot } = &prepared.resource.descriptor().schema_source
    else {
        panic!("expected REST auto-pin to set discovered schema source");
    };
    assert_eq!(
        snapshot.schema_hash,
        discovery.snapshot.artifact.schema_hash
    );
    assert_eq!(
        prepared
            .resource
            .schema()
            .field_with_name("vendor_id")
            .unwrap()
            .metadata()["cdf:source_name"],
        "VendorID"
    );
}

#[test]
fn generic_schema_discovery_dispatch_fails_closed_for_sql_query_resource() {
    let project = r#"
[project]
name = "warehouse"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."warehouse.*"]
source = "resources/sql.toml"
"#;
    let sql = r#"
[source.warehouse]
kind = "sql"
connection = "secret://env/POSTGRES_URL"
dialect = "postgres"

[resource.orders]
query = "SELECT * FROM public.orders"
write_disposition = "append"
trust = "governed"
"#;
    let config = parse_cdf_toml(project).unwrap();
    let resolver = InMemoryResourceSourceResolver::new().with_toml("resources/sql.toml", sql);
    let mut resources = compile_project_declarative_resources(&config, &resolver).unwrap();
    let resource = resources.remove(0);

    let error = discover_resource_schema(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
    )
    .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("unsupported schema discovery slice"));
    assert!(message.contains("warehouse.orders"));
    assert!(message.contains("query resources are not supported"));
}

#[test]
fn generic_schema_discovery_dispatch_fails_closed_for_non_postgres_sql_dialect() {
    let project = r#"
[project]
name = "warehouse"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."warehouse.*"]
source = "resources/sql.toml"
"#;
    let sql = r#"
[source.warehouse]
kind = "sql"
connection = "secret://env/WAREHOUSE_URL"
dialect = "mysql"

[resource.orders]
table = "orders"
write_disposition = "append"
trust = "governed"
"#;
    let config = parse_cdf_toml(project).unwrap();
    let resolver = InMemoryResourceSourceResolver::new().with_toml("resources/sql.toml", sql);
    let mut resources = compile_project_declarative_resources(&config, &resolver).unwrap();
    let resource = resources.remove(0);

    let error = discover_resource_schema(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
    )
    .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("unsupported schema discovery slice"));
    assert!(message.contains("warehouse.orders"));
    assert!(message.contains("SQL dialect `mysql` discovery is not implemented"));
}

fn json_response(body: &str) -> HttpResponse {
    HttpResponse::new(200).with_body(body.as_bytes().to_vec())
}

#[derive(Clone, Default)]
struct RecordingTransport {
    state: Arc<Mutex<RecordingTransportState>>,
}

#[derive(Default)]
struct RecordingTransportState {
    requests: Vec<HttpRequest>,
    responses: VecDeque<HttpResponse>,
}

impl RecordingTransport {
    fn new<I>(responses: I) -> Self
    where
        I: IntoIterator<Item = HttpResponse>,
    {
        Self {
            state: Arc::new(Mutex::new(RecordingTransportState {
                requests: Vec::new(),
                responses: responses.into_iter().collect(),
            })),
        }
    }

    fn requests(&self) -> Vec<HttpRequest> {
        self.state.lock().unwrap().requests.clone()
    }
}

impl HttpTransport for RecordingTransport {
    fn send(&mut self, request: HttpRequest) -> Result<HttpResponse> {
        let mut state = self.state.lock().unwrap();
        state.requests.push(request);
        state
            .responses
            .pop_front()
            .ok_or_else(|| CdfError::internal("test transport exhausted responses"))
    }
}

#[derive(Clone)]
struct RecordingHttpFileTransport {
    state: Arc<Mutex<RecordingHttpFileTransportState>>,
}

struct RecordingHttpFileTransportState {
    requests: Vec<HttpFileRequest>,
    body: Vec<u8>,
    etag: String,
}

impl RecordingHttpFileTransport {
    fn new(body: Vec<u8>) -> Self {
        Self {
            state: Arc::new(Mutex::new(RecordingHttpFileTransportState {
                requests: Vec::new(),
                body,
                etag: "\"fixture-etag\"".to_owned(),
            })),
        }
    }

    fn requests(&self) -> Vec<HttpFileRequest> {
        self.state.lock().unwrap().requests.clone()
    }
}

impl HttpFileTransport for RecordingHttpFileTransport {
    fn send(&mut self, request: HttpFileRequest) -> Result<HttpFileResponse> {
        let mut state = self.state.lock().unwrap();
        state.requests.push(request.clone());
        match request.method {
            HttpMethod::Head => Ok(HttpFileResponse::new(200)
                .with_header("Content-Length", state.body.len().to_string())
                .with_header("ETag", state.etag.clone())
                .with_header("Last-Modified", "Wed, 08 Jul 2026 12:00:00 GMT")),
            HttpMethod::Get => {
                let range = request.headers.get("range").ok_or_else(|| {
                    CdfError::data("test HTTP file transport requires ranged GET")
                })?;
                let (start, end) = parse_http_fixture_range(range, state.body.len())?;
                let bytes = state.body[start..=end].to_vec();
                Ok(HttpFileResponse::new(206)
                    .with_header(
                        "Content-Range",
                        format!("bytes {start}-{end}/{}", state.body.len()),
                    )
                    .with_body(bytes))
            }
            _ => Ok(HttpFileResponse::new(405)),
        }
    }
}

fn parse_http_fixture_range(range: &str, len: usize) -> Result<(usize, usize)> {
    let raw = range
        .strip_prefix("bytes=")
        .ok_or_else(|| CdfError::data(format!("invalid test range header `{range}`")))?;
    let (start, end) = raw
        .split_once('-')
        .ok_or_else(|| CdfError::data(format!("invalid test range header `{range}`")))?;
    let start = start
        .parse::<usize>()
        .map_err(|error| CdfError::data(format!("invalid range start: {error}")))?;
    let end = end
        .parse::<usize>()
        .map_err(|error| CdfError::data(format!("invalid range end: {error}")))?;
    if start > end || end >= len {
        return Err(CdfError::data(format!(
            "test range {start}-{end} exceeds fixture length {len}"
        )));
    }
    Ok((start, end))
}

fn vendor_parquet_bytes() -> Vec<u8> {
    vendor_parquet_bytes_with_rows(2)
}

fn vendor_parquet_bytes_with_rows(row_count: i32) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "VendorID",
        DataType::Int32,
        false,
    )]));
    let values = (0..row_count).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap();
    cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap()
}

fn write_http_discover_project(root: &Path, source_extra: &str) {
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "http_files"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."remote.*"]
source = "resources/files.toml"
"#,
    )
    .unwrap();
    fs::write(
        root.join("resources/files.toml"),
        format!(
            r#"
[source.remote]
kind = "files"
root = "https://data.example.test/trip-data"
{source_extra}

[resource.events]
glob = "vendors.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
"#
        ),
    )
    .unwrap();
}

fn http_file_dependencies(transport: RecordingHttpFileTransport) -> FileRuntimeDependencies {
    FileRuntimeDependencies::new(FileTransportFacade::new().with_http_transport(transport))
}

fn live_plan_for_stream(resource: &dyn QueryableResource, package_id: &str) -> EnginePlan {
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    let validation_program = compile_validation_program(&policy, &observed_schema).unwrap();
    Planner::new()
        .plan_tier_b(
            resource,
            EnginePlanInput {
                request: ScanRequest {
                    resource_id: resource.descriptor().resource_id.clone(),
                    projection: None,
                    filters: Vec::new(),
                    limit: None,
                    order_by: Vec::new(),
                    scope: resource.descriptor().state_scope.clone(),
                },
                validation_program,
                boundedness: PlanBoundedness::Bounded,
                package_id: package_id.to_owned(),
            },
        )
        .unwrap()
}

fn assert_only_bounded_http_file_gets(requests: &[HttpFileRequest]) {
    assert!(
        requests
            .iter()
            .any(|request| request.method == HttpMethod::Head),
        "expected an HTTP HEAD metadata request, got {requests:?}"
    );
    let get_requests = requests
        .iter()
        .filter(|request| request.method == HttpMethod::Get)
        .collect::<Vec<_>>();
    assert!(
        !get_requests.is_empty(),
        "expected bounded HTTP GET requests, got {requests:?}"
    );
    for request in get_requests {
        let range = request
            .headers
            .get("range")
            .expect("HTTP file GET should carry Range header");
        assert!(
            range.starts_with("bytes="),
            "HTTP file GET should use byte range, got {range}"
        );
    }
}

fn assert_http_file_gets_download_less_than_fixture(
    requests: &[HttpFileRequest],
    fixture_len: usize,
) {
    let downloaded = requests
        .iter()
        .filter(|request| request.method == HttpMethod::Get)
        .map(|request| {
            let range = request
                .headers
                .get("range")
                .expect("HTTP file GET should carry Range header");
            let (start, end) = parse_http_fixture_range(range, fixture_len).unwrap();
            end - start + 1
        })
        .sum::<usize>();
    assert!(
        downloaded < fixture_len,
        "expected discovery to use partial ranged reads, downloaded {downloaded} of {fixture_len} bytes"
    );
}

struct StaticSecretProvider {
    values: BTreeMap<String, String>,
}

impl StaticSecretProvider {
    fn new<I, K, V>(values: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self {
            values: values
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        }
    }
}

impl SecretProvider for StaticSecretProvider {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
        self.values
            .get(uri.as_str())
            .map(|value| SecretValue::new(value.clone()))
            .ok_or_else(|| CdfError::auth(format!("missing test secret `{uri}`")))
    }
}

fn write_discover_project(root: &Path, format: &str, glob: &str) {
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "files"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."local.*"]
source = "resources/files.toml"
"#,
    )
    .unwrap();
    fs::write(
        root.join("resources/files.toml"),
        format!(
            r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "{glob}"
format = "{format}"
write_disposition = "append"
trust = "governed"
"#
        ),
    )
    .unwrap();
}

fn compile_single_project_resource(root: &Path) -> cdf_declarative::CompiledResource {
    let config = parse_cdf_toml(&fs::read_to_string(root.join("cdf.toml")).unwrap()).unwrap();
    let resolver = FileResourceSourceResolver::new(root);
    let mut resources =
        compile_project_declarative_resources_with_root(&config, &resolver, root).unwrap();
    assert_eq!(resources.len(), 1);
    resources.remove(0)
}

fn write_vendor_parquet(path: &Path) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "VendorID",
        DataType::Int32,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32]))]).unwrap();
    let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    fs::write(path, bytes).unwrap();
}

#[test]
fn contract_freeze_preserves_existing_dependency_and_destination_data() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let resources = compile_project_declarative_resources(&config, &resolver).unwrap();
    let sheet = destination_sheet("duckdb", TypeMappingFidelity::Lossless);
    let dependency_tuple = DependencyTuple {
        cdf: "0.1.0-old".to_owned(),
        arrow_rs: "59.1.0-old".to_owned(),
        datafusion: Some("pinned-datafusion".to_owned()),
        object_store: Some("pinned-object-store".to_owned()),
        duckdb_rs: Some("pinned-duckdb".to_owned()),
        rust: Some("pinned-rust".to_owned()),
    };
    let existing = generate_lockfile(
        &config,
        &resources,
        dependency_tuple.clone(),
        std::slice::from_ref(&sheet),
        BTreeMap::new(),
    )
    .unwrap();

    let (lock, report) = freeze_contract_snapshots(
        &config,
        &resources,
        Some(&existing),
        "duckdb://ignored-by-existing-lock",
        Some("github.issues"),
    )
    .unwrap();

    assert_eq!(lock.dependency_tuple, dependency_tuple);
    assert_eq!(lock.destinations, existing.destinations);
    assert_eq!(report.resource_ids, vec!["github.issues"]);
    let snapshot = lock.resources["github.issues"].contract.as_ref().unwrap();
    assert!(
        snapshot
            .policy_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
    assert!(
        snapshot
            .validation_program_hash
            .as_ref()
            .unwrap()
            .starts_with("sha256:")
    );
}

#[test]
fn contract_test_reports_field_level_snapshot_drift() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);
    let resources = compile_project_declarative_resources(&config, &resolver).unwrap();
    let (lock, _) =
        freeze_contract_snapshots(&config, &resources, None, "duckdb://.cdf/dev.duckdb", None)
            .unwrap();
    let changed_resource = GITHUB_RESOURCE.replace(
        "  { name = \"updated_at\", type = \"timestamp_micros\", nullable = false, timezone = \"UTC\" },",
        concat!(
            "  { name = \"updated_at\", type = \"timestamp_micros\", nullable = false, timezone = \"UTC\" },\n",
            "  { name = \"ingested_at\", type = \"int64\", nullable = true },"
        ),
    );
    let changed_resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", &changed_resource);
    let changed_resources =
        compile_project_declarative_resources(&config, &changed_resolver).unwrap();

    let report = test_contract_snapshots(&lock, &changed_resources, Some("github.issues")).unwrap();

    assert_eq!(report.counts.passed, 0);
    assert_eq!(report.counts.drifted, 1);
    let fields = report
        .drift_details
        .iter()
        .map(|detail| detail.field.as_str())
        .collect::<Vec<_>>();
    assert!(fields.contains(&"schema_hash"));
    assert!(fields.contains(&"validation_program_hash"));
}

fn destination_sheet(name: &str, fidelity: TypeMappingFidelity) -> DestinationSheet {
    DestinationSheet {
        destination: DestinationId::new(name).unwrap(),
        supported_dispositions: vec![WriteDisposition::Append, WriteDisposition::Merge],
        transactions: TransactionSupport::AtomicPackage,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: vec![TypeMapping {
            arrow_type: "utf8".to_owned(),
            destination_type: "text".to_owned(),
            fidelity,
        }],
        identifier_rules: IdentifierRules {
            normalizer: NORMALIZER_NAMECASE_V1.to_owned(),
            max_length: Some(63),
            allowed_pattern: Some("[a-z_][a-z0-9_]*".to_owned()),
        },
        migration_support: CapabilitySupport::Supported,
        quarantine_tables: CapabilitySupport::Supported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    }
}

#[test]
fn inline_uri_credentials_are_rejected() {
    let input = BOOK_PROJECT.replace(
        "destination = \"duckdb://.cdf/dev.duckdb\"",
        "destination = \"postgres://user:password@example.com/db\"",
    );
    let config = parse_cdf_toml(&input).unwrap();

    let error = config.effective_environment("dev").and_then(|env| {
        validate_environment_uri_fields(&env)?;
        Ok(())
    });

    assert!(
        error
            .unwrap_err()
            .to_string()
            .contains("inline credentials")
    );
}

#[test]
fn secret_ref_requires_provider_and_key() {
    assert!(SecretRef::new("secret://env/TOKEN").is_ok());
    assert!(SecretRef::new("env:TOKEN").is_err());
    assert!(SecretRef::new("secret://env").is_err());
}

#[test]
fn declarative_resource_compilation_hook_uses_cdf_declarative() {
    let config = parse_cdf_toml(BOOK_PROJECT).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/github.toml", GITHUB_RESOURCE);

    let resources = compile_project_declarative_resources(&config, &resolver).unwrap();

    assert_eq!(resources.len(), 1);
    assert_eq!(
        resources[0].descriptor().resource_id.as_str(),
        "github.issues"
    );
}

#[test]
fn declarative_resource_mapping_pattern_must_match_compiled_id() {
    let project = r#"
[project]
name = "tlc"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."yellow"]
source = "resources/tlc.toml"
"#;
    let resource = r#"
[source.tlc]
kind = "files"
root = "data"

[resource.yellow]
glob = "*.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
"#;
    let config = parse_cdf_toml(project).unwrap();
    let resolver = InMemoryResourceSourceResolver::new().with_toml("resources/tlc.toml", resource);

    let error = compile_project_declarative_resources(&config, &resolver).unwrap_err();

    let message = error.to_string();
    assert!(message.contains("resource mapping pattern `yellow`"));
    assert!(message.contains("tlc.yellow"));
    assert!(message.contains("`<source>.<resource>`"));
    assert!(message.contains("[resources.\"tlc.yellow\"]"));
}

#[test]
fn declarative_file_roots_resolve_under_project_root_for_runtime_compile() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir_all(temp.path().join("resources")).unwrap();
    let project = r#"
[project]
name = "files"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."local.*"]
source = "resources/files.toml"
"#;
    let resource = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "*.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
"#;
    fs::write(temp.path().join("resources/files.toml"), resource).unwrap();
    let config = parse_cdf_toml(project).unwrap();
    let resolver = FileResourceSourceResolver::new(temp.path());

    let resources =
        compile_project_declarative_resources_with_root(&config, &resolver, temp.path()).unwrap();

    let CompiledResourcePlan::Files(plan) = resources[0].plan() else {
        panic!("expected file resource plan");
    };
    assert_eq!(
        plan.root,
        temp.path().join("data").to_str().unwrap().to_owned()
    );
}

#[test]
fn local_project_scaffold_writes_valid_project_without_runtime_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("fresh-project");

    let report = write_local_project_scaffold(ProjectScaffoldOptions {
        root: root.clone(),
        project_name: None,
        force: false,
    })
    .unwrap();

    assert_eq!(report.project_name, "fresh-project");
    assert_eq!(
        report.created,
        vec![
            "cdf.toml",
            "README.md",
            "resources",
            "resources/files.toml",
            "data"
        ]
    );
    assert!(root.join("cdf.toml").is_file());
    assert!(root.join("README.md").is_file());
    assert!(root.join("resources/files.toml").is_file());
    assert!(root.join("data").is_dir());
    assert!(fs::read_dir(root.join("data")).unwrap().next().is_none());
    assert!(!root.join(".cdf").exists());
    assert!(!root.join("cdf.lock").exists());

    let config = parse_cdf_toml(&fs::read_to_string(root.join("cdf.toml")).unwrap()).unwrap();
    let readme = fs::read_to_string(root.join("README.md")).unwrap();
    assert!(readme.contains("docs/quickstart.md"));
    assert!(readme.contains("cdf validate"));
    assert!(readme.contains("cdf plan local.events --target local_events"));
    assert!(readme.contains("cdf run --resource local.events"));
    assert!(!readme.contains("secret://"));
    assert!(!readme.contains(root.to_str().unwrap()));
    let resolver = FileResourceSourceResolver::new(&root);
    let provider = EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>());
    let validation = validate_project(&config, Some("dev"), &resolver, &provider).unwrap();

    assert_eq!(validation.declarative_resources, 1);
    assert!(validation.checked_secrets.is_empty());
}

#[test]
fn declarative_sql_secret_is_collected_for_validation() {
    let project = BOOK_PROJECT.replace(
        "[resources.\"github.*\"]\nsource = \"resources/github.toml\"",
        "[resources.\"warehouse.*\"]\nsource = \"resources/sql.toml\"",
    );
    let sql_resource = r#"
[source.warehouse]
kind = "sql"
connection = "secret://env/POSTGRES_URL"

[resource.orders]
table = "public.orders"
primary_key = ["id"]
merge_key = ["id"]
write_disposition = "merge"
trust = "governed"
"#;
    let config = parse_cdf_toml(&project).unwrap();
    let resolver =
        InMemoryResourceSourceResolver::new().with_toml("resources/sql.toml", sql_resource);
    let provider = EnvSecretProvider::from_map([
        ("POSTGRES_URL", "postgres-url-value"),
        ("PROD_DWH", "postgres-dsn-value"),
    ]);

    let report = validate_project(&config, Some("prod"), &resolver, &provider).unwrap();

    assert!(
        report
            .checked_secrets
            .iter()
            .any(|check| check.uri.as_str() == "secret://env/POSTGRES_URL")
    );
    assert!(!format!("{report:?}").contains("postgres-url-value"));
}

#[test]
fn unsupported_keychain_provider_is_explicit_not_guessy() {
    let provider = DefaultSecretProvider::default();
    let uri = SecretUri::new("secret://keychain/prod-token").unwrap();
    let error = provider.resolve(&uri).unwrap_err();

    assert!(error.to_string().contains("not available"));
    assert!(!error.to_string().contains("prod-token-value"));
}

#[test]
fn auth_declaration_secret_uri_model_still_rejects_values() {
    let auth = AuthDeclaration::Bearer {
        token: "secret://env/TOKEN".to_owned(),
    };
    let source = SourceDeclaration::Rest(cdf_declarative::RestSourceDeclaration {
        base_url: "https://api.example.com".to_owned(),
        auth: Some(auth),
        rate_limit: None,
        egress_allowlist: Vec::new(),
    });

    assert!(matches!(source, SourceDeclaration::Rest(_)));
}
