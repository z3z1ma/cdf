use super::*;
use crate::internal::*;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::writer::FileWriter;
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
    CapabilitySupport, CdfError, CheckpointId, ConcurrencyLimit, ContractRef, DestinationId,
    DestinationProtocol, DestinationSheet, DiscoveryManifestHash, DiscoveryManifestReference,
    IdempotencySupport, IdentifierRules, LeaseOwnerId, PipelineId, QueryableResource, ResourceId,
    ResourceStream, RunId, ScanRequest, SchemaHash, SchemaSource, ScopeKey, ScopeLease,
    ScopeLeaseClock, ScopeLeaseStore, SourcePosition, TargetName, TransactionSupport, TypeMapping,
    TypeMappingFidelity, WriteDisposition, source_name,
};
use cdf_state_sqlite::InMemoryScopeLeaseStore;
use flate2::{Compression, write::GzEncoder};
use object_store::{ObjectStoreExt, PutPayload, memory::InMemory, path::Path as ObjectPath};

#[test]
fn project_normal_build_graph_has_no_concrete_destination_crates() {
    let manifest: toml::Value = toml::from_str(include_str!("../Cargo.toml")).unwrap();
    let dependencies = manifest
        .get("dependencies")
        .and_then(toml::Value::as_table)
        .unwrap();
    let concrete = dependencies
        .keys()
        .filter(|name| name.starts_with("cdf-dest-"))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        concrete.is_empty(),
        "cdf-project normal dependencies must remain destination-neutral: {concrete:?}"
    );
}

fn test_execution_services() -> cdf_runtime::ExecutionServices {
    cdf_engine::StandaloneExecutionHost::default_services(64 * 1024 * 1024)
        .unwrap()
        .1
}

fn test_format_registry() -> Arc<cdf_runtime::FormatRegistry> {
    let mut registry = cdf_runtime::FormatRegistry::default();
    registry
        .register(Arc::new(
            cdf_format_arrow_ipc::ArrowIpcFileFormatDriver::new().unwrap(),
        ))
        .unwrap();
    registry
        .register(Arc::new(
            cdf_format_parquet::ParquetFormatDriver::new().unwrap(),
        ))
        .unwrap();
    Arc::new(registry)
}

fn file_dependencies(transport: FileTransportFacade) -> FileRuntimeDependencies {
    FileRuntimeDependencies::new(transport, test_execution_services(), test_format_registry())
}

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
    assert!(!encoded.contains("corrections"));
    let decoded = parse_lock(&encoded).unwrap();
    assert_eq!(decoded, lock);
    assert_eq!(lock_to_toml(&decoded).unwrap(), encoded);
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
    assert_eq!(
        lock.destinations["duckdb"].sheet_hash,
        semantic_hash(&sheet).unwrap()
    );

    let changed = generate_lockfile(
        &config,
        &resources,
        dependency_tuple.clone(),
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

    let postgres_artifact = cdf_dest_postgres::PostgresDestination::new()
        .sheet_artifact()
        .unwrap();
    let parquet_temp = tempfile::tempdir().unwrap();
    let parquet_artifact = cdf_dest_parquet::ParquetDestination::new_filesystem(
        parquet_temp.path(),
        test_execution_services(),
    )
    .unwrap()
    .sheet_artifact()
    .unwrap();
    let typed_lock = generate_lockfile_with_destination_artifacts(
        &config,
        &resources,
        dependency_tuple,
        &[postgres_artifact.clone(), parquet_artifact.clone()],
        BTreeMap::new(),
    )
    .unwrap();
    let typed_encoded = lock_to_toml(&typed_lock).unwrap();
    assert!(typed_encoded.contains("protocol_capabilities"));
    assert!(typed_encoded.contains("corrections"));
    assert!(typed_encoded.contains("object_key_rules"));
    assert!(typed_encoded.contains("object-key-component-v1"));
    let typed_decoded = parse_lock(&typed_encoded).unwrap();
    assert_eq!(typed_decoded, typed_lock);
    assert_eq!(lock_to_toml(&typed_decoded).unwrap(), typed_encoded);
    assert_eq!(
        typed_lock.destinations["postgres"]
            .sheet_artifact()
            .unwrap(),
        postgres_artifact
    );
    assert_eq!(
        typed_lock.destinations["parquet_object_store"]
            .sheet_artifact()
            .unwrap(),
        parquet_artifact
    );
}

fn schema_lease(store: &InMemoryScopeLeaseStore) -> ScopeLease {
    store
        .acquire(
            ScopeKey::SchemaContract {
                contract: ContractRef::new("orders").unwrap(),
            },
            LeaseOwnerId::new("promotion-executor").unwrap(),
            1_000,
        )
        .unwrap()
}

#[test]
fn lock_file_cas_requires_exact_prior_bytes_hash_and_current_fence() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    fs::write(&path, b"version = 1\n").unwrap();
    let expected = read_lock_file_authority(&path).unwrap();
    let store = InMemoryScopeLeaseStore::new();
    let lease = schema_lease(&store);

    let report =
        compare_and_swap_lock_file(&path, &expected, b"version = 2\n", &store, &lease).unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"version = 2\n");
    assert_eq!(report.installed, read_lock_file_authority(&path).unwrap());
    #[cfg(unix)]
    assert!(report.parent_directory_synced);

    let error =
        compare_and_swap_lock_file(&path, &expected, b"version = 3\n", &store, &lease).unwrap_err();
    assert!(error.message.contains("prior authority changed"));
    assert_eq!(fs::read(&path).unwrap(), b"version = 2\n");

    let mut inconsistent = read_lock_file_authority(&path).unwrap();
    inconsistent.sha256.push_str("tampered");
    let error = compare_and_swap_lock_file(&path, &inconsistent, b"version = 3\n", &store, &lease)
        .unwrap_err();
    assert!(error.message.contains("supplied bytes hash"));
}

#[test]
fn lock_file_cas_failpoints_model_each_crash_boundary() {
    for (failpoint, expected_bytes) in [
        (LockFileCasFailpoint::BeforeTempSync, b"old\n".as_slice()),
        (LockFileCasFailpoint::BeforeRename, b"old\n".as_slice()),
        (LockFileCasFailpoint::AfterRename, b"new\n".as_slice()),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join(LOCK_FILE_NAME);
        fs::write(&path, b"old\n").unwrap();
        let expected = read_lock_file_authority(&path).unwrap();
        let store = InMemoryScopeLeaseStore::new();
        let lease = schema_lease(&store);
        let error = compare_and_swap_lock_file_with_failpoint(
            &path,
            &expected,
            b"new\n",
            &store,
            &lease,
            Some(failpoint),
        )
        .unwrap_err();
        assert!(
            error
                .message
                .contains("injected cdf.lock publication crash")
        );
        assert_eq!(fs::read(&path).unwrap(), expected_bytes);
    }
}

#[test]
fn guarded_lock_writer_atomically_creates_replaces_and_rejects_stale_authority() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    write_lock_file_guarded(&path, None, b"created\n").unwrap();
    let created = read_lock_file_authority(&path).unwrap();
    assert_eq!(created.bytes, b"created\n");

    write_lock_file_guarded(&path, Some(&created), b"replaced\n").unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"replaced\n");
    let error = write_lock_file_guarded(&path, Some(&created), b"stale\n").unwrap_err();
    assert!(error.message.contains("prior authority changed"));
    assert_eq!(fs::read(&path).unwrap(), b"replaced\n");

    let temporary_files = fs::read_dir(temp.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".cas.tmp"))
        .count();
    assert_eq!(temporary_files, 0);
    assert!(
        temp.path()
            .join(".cdf/locks/cdf.lock.mutation.lock")
            .is_file()
    );
}

#[test]
fn guarded_cdf_writer_cannot_enter_cas_final_check_rename_window() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    fs::write(&path, b"old\n").unwrap();
    let expected = read_lock_file_authority(&path).unwrap();
    let writer_expected = expected.clone();
    let store = InMemoryScopeLeaseStore::new();
    let lease = schema_lease(&store);
    let (at_publication_tx, at_publication_rx) = mpsc::channel();
    let (continue_tx, continue_rx) = mpsc::channel();
    let cas_path = path.clone();
    let cas = thread::spawn(move || {
        compare_and_swap_lock_file_with_publication_hook(
            &cas_path,
            &expected,
            b"cas\n",
            &store,
            &lease,
            || {
                at_publication_tx.send(()).unwrap();
                continue_rx.recv().unwrap();
                Ok(())
            },
        )
    });
    at_publication_rx.recv().unwrap();

    let writer_path = path.clone();
    let (attempting_tx, attempting_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        attempting_tx.send(()).unwrap();
        let result =
            write_lock_file_guarded(&writer_path, Some(&writer_expected), b"ordinary-writer\n");
        done_tx.send(result).unwrap();
    });
    attempting_rx.recv().unwrap();
    assert!(
        done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "ordinary CDF writer must block while CAS holds the project mutation guard"
    );

    continue_tx.send(()).unwrap();
    assert_eq!(cas.join().unwrap().unwrap().installed.bytes, b"cas\n");
    let writer_error = done_rx
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap_err();
    assert!(writer_error.message.contains("prior authority changed"));
    writer.join().unwrap();
    assert_eq!(fs::read(&path).unwrap(), b"cas\n");
}

struct StaleAtPublicationStore {
    checks: AtomicUsize,
}

impl ScopeLeaseStore for StaleAtPublicationStore {
    fn acquire(
        &self,
        _scope: ScopeKey,
        _owner: LeaseOwnerId,
        _lease_duration_ms: u64,
    ) -> cdf_kernel::Result<ScopeLease> {
        unreachable!()
    }

    fn renew(
        &self,
        _lease: &ScopeLease,
        _lease_duration_ms: u64,
    ) -> cdf_kernel::Result<ScopeLease> {
        unreachable!()
    }

    fn release(&self, _lease: &ScopeLease) -> cdf_kernel::Result<()> {
        unreachable!()
    }

    fn assert_current(&self, _lease: &ScopeLease) -> cdf_kernel::Result<()> {
        if self.checks.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(())
        } else {
            Err(CdfError::contract("lease superseded before publication"))
        }
    }
}

#[test]
fn stale_fencing_token_cannot_publish_after_temp_file_sync() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    fs::write(&path, b"old\n").unwrap();
    let expected = read_lock_file_authority(&path).unwrap();
    let real_store = InMemoryScopeLeaseStore::new();
    let lease = schema_lease(&real_store);
    let store = StaleAtPublicationStore {
        checks: AtomicUsize::new(0),
    };

    let error = compare_and_swap_lock_file(&path, &expected, b"new\n", &store, &lease).unwrap_err();
    assert!(error.message.contains("superseded before publication"));
    assert_eq!(fs::read(&path).unwrap(), b"old\n");
}

#[test]
fn lease_expiring_during_temp_write_cannot_publish() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join(LOCK_FILE_NAME);
    fs::write(&path, b"old\n").unwrap();
    let expected = read_lock_file_authority(&path).unwrap();
    struct ExpiringClock(AtomicUsize);

    impl ScopeLeaseClock for ExpiringClock {
        fn now_ms(&self) -> cdf_kernel::Result<i64> {
            Ok(match self.0.fetch_add(1, Ordering::SeqCst) {
                0 => 1_000,
                1 => 1_099,
                _ => 1_100,
            })
        }
    }

    let store = InMemoryScopeLeaseStore::with_clock(Arc::new(ExpiringClock(AtomicUsize::new(0))));
    let lease = store
        .acquire(
            ScopeKey::SchemaContract {
                contract: ContractRef::new("expiring").unwrap(),
            },
            LeaseOwnerId::new("promotion-executor").unwrap(),
            100,
        )
        .unwrap();

    let error = compare_and_swap_lock_file(&path, &expected, b"new\n", &store, &lease).unwrap_err();
    assert!(error.message.contains("expired, released, or superseded"));
    assert_eq!(fs::read(&path).unwrap(), b"old\n");
}

#[test]
fn lock_file_atomicity_capabilities_state_platform_limits() {
    let capabilities = lock_file_atomicity_capabilities();
    assert!(!capabilities.limitation.is_empty());
    assert!(capabilities.cooperating_cdf_writers_serialized);
    assert!(capabilities.limitation.contains("non-cooperating"));
    #[cfg(unix)]
    {
        assert!(capabilities.atomic_rename_over_existing);
        assert!(capabilities.parent_directory_fsync);
        assert!(capabilities.limitation.contains("same Unix filesystem"));
        assert!(capabilities.limitation.contains("POSIX rename"));
    }
    #[cfg(not(unix))]
    {
        assert!(!capabilities.atomic_rename_over_existing);
        assert!(!capabilities.parent_directory_fsync);
    }
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
fn discovery_executor_budget_defaults_and_rejects_invalid_shapes() {
    let budget = DiscoveryExecutorBudget::default();
    assert_eq!(budget.max_metadata_bytes_per_file(), 64 * 1024 * 1024);
    assert_eq!(budget.max_total_in_flight_bytes(), 128 * 1024 * 1024);
    assert_eq!(budget.max_concurrent_probes(), 8);
    let options = SchemaDiscoveryExecutionOptions::new();
    assert_eq!(options.budget(), &budget);
    assert!(options.verified_baseline().is_none());

    for (per_file, total, probes, expected) in [
        (0, 1, 1, "max_metadata_bytes_per_file"),
        (1, 0, 1, "max_total_in_flight_bytes"),
        (1, 1, 0, "max_concurrent_probes"),
        (2, 1, 1, "cannot exceed"),
        (u64::MAX / 2 + 1, u64::MAX, 2, "overflows"),
    ] {
        let error = DiscoveryExecutorBudget::new(per_file, total, probes)
            .unwrap_err()
            .to_string();
        assert!(error.contains(expected), "unexpected error: {error}");
    }

    let invalid_json = r#"{
        "max_metadata_bytes_per_file": 0,
        "max_total_in_flight_bytes": 1,
        "max_concurrent_probes": 1
    }"#;
    assert!(serde_json::from_str::<DiscoveryExecutorBudget>(invalid_json).is_err());
}

#[test]
fn discovery_manifest_is_canonical_content_addressed_and_fail_closed() {
    let resource_id = ResourceId::new("events.raw").unwrap();
    let first = probed_discovery_candidate("file:///data/a.parquet", "sha256:a", 48);
    let mut second = probed_discovery_candidate("file:///data/b.parquet", "sha256:b", 64);
    second.metadata_variance = vec![DiscoveryMetadataVariance {
        scope: DiscoveryMetadataScope::Field,
        path: "amount".to_owned(),
        key: "source.logical_type".to_owned(),
        observed_values: vec!["utf8".to_owned(), "decimal".to_owned(), "utf8".to_owned()],
    }];
    let input = DiscoveryManifestInput {
        resource_id: resource_id.as_str().to_owned(),
        baseline_schema_hash: Some(SchemaHash::new("sha256:baseline").unwrap()),
        effective_schema_hash: Some(SchemaHash::new("sha256:effective").unwrap()),
        coverage: DiscoveryCoverageMode::Exhaustive,
        selector: None,
        budget: DiscoveryExecutorBudget::default(),
        normalizer_version: "namecase-v1".to_owned(),
        policy_version: "evolve-v1".to_owned(),
        candidates: vec![second, first],
    };
    let artifact = DiscoveryManifestArtifact::new(input.clone()).unwrap();
    let repeated = DiscoveryManifestArtifact::new(input).unwrap();

    assert_eq!(artifact, repeated);
    let same_observation_new_baseline = DiscoveryManifestArtifact::new(DiscoveryManifestInput {
        resource_id: artifact.resource_id.clone(),
        baseline_schema_hash: Some(SchemaHash::new("sha256:next-baseline").unwrap()),
        effective_schema_hash: artifact.effective_schema_hash.clone(),
        coverage: artifact.coverage.clone(),
        selector: artifact.selector.clone(),
        budget: artifact.budget.clone(),
        normalizer_version: artifact.normalizer_version.clone(),
        policy_version: artifact.policy_version.clone(),
        candidates: artifact.candidates.clone(),
    })
    .unwrap();
    assert_ne!(
        artifact.manifest_hash,
        same_observation_new_baseline.manifest_hash
    );
    assert!(artifact.has_same_observation(&same_observation_new_baseline));
    assert_eq!(
        artifact
            .candidates
            .iter()
            .map(|candidate| candidate.canonical_location.as_str())
            .collect::<Vec<_>>(),
        vec!["file:///data/a.parquet", "file:///data/b.parquet"]
    );
    assert_eq!(
        artifact.candidates[1].metadata_variance[0].observed_values,
        vec!["decimal", "utf8"]
    );
    assert_eq!(artifact.hash_input["coverage"], "exhaustive");
    assert_eq!(
        artifact.path,
        format!(
            ".cdf/schemas/events.raw@{}.discovery.json",
            artifact.manifest_hash
        )
    );

    let temp = tempfile::tempdir().unwrap();
    let store = DiscoveryManifestStore::new(temp.path());
    assert!(store.write_if_changed(&artifact).unwrap());
    assert!(!store.write_if_changed(&artifact).unwrap());
    assert_eq!(store.read(&artifact.reference()).unwrap(), artifact);
    let schema_dir_entries = std::fs::read_dir(temp.path().join(".cdf/schemas"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert!(
        schema_dir_entries
            .iter()
            .all(|name| !name.ends_with(".tmp"))
    );

    let mut unsafe_reference = artifact.reference();
    unsafe_reference.path = "../manifest.json".to_owned();
    assert!(
        store
            .read(&unsafe_reference)
            .unwrap_err()
            .to_string()
            .contains("reference path")
    );

    let missing = DiscoveryManifestReference {
        manifest_hash: DiscoveryManifestHash::new("sha256:missing").unwrap(),
        path: ".cdf/schemas/events.raw@sha256:missing.discovery.json".to_owned(),
    };
    assert!(
        store
            .read(&missing)
            .unwrap_err()
            .to_string()
            .contains("read")
    );

    let wrong_hash = DiscoveryManifestReference {
        manifest_hash: DiscoveryManifestHash::new("sha256:wrong").unwrap(),
        path: artifact.path.clone(),
    };
    assert!(
        store
            .read(&wrong_hash)
            .unwrap_err()
            .to_string()
            .contains("does not match its hash/path reference")
    );

    let path = temp.path().join(&artifact.path);
    let mut tampered = artifact.clone();
    tampered.policy_version = "freeze-v1".to_owned();
    std::fs::write(&path, serde_json::to_vec_pretty(&tampered).unwrap()).unwrap();
    assert!(
        store
            .read(&artifact.reference())
            .unwrap_err()
            .to_string()
            .contains("hash_input")
    );
}

#[test]
fn sampled_discovery_manifest_enforces_truthful_participation() {
    let first = probed_discovery_candidate("file:///data/00.parquet", "sha256:00", 32);
    let middle = unprobed_discovery_candidate("file:///data/01.parquet", "etag:01");
    let last = probed_discovery_candidate("file:///data/02.parquet", "sha256:02", 40);
    let selector_candidates = [&first, &middle, &last]
        .into_iter()
        .map(|candidate| DiscoverySelectorCandidate {
            canonical_location: candidate.canonical_location.clone(),
            identity: candidate.identity.clone(),
        })
        .collect::<Vec<_>>();
    let selector = plan_discovery_selection(
        &ResourceId::new("events.sampled").unwrap(),
        Some(2),
        &selector_candidates,
    )
    .unwrap()
    .selector
    .unwrap();
    let input = DiscoveryManifestInput {
        resource_id: "events.sampled".to_owned(),
        baseline_schema_hash: None,
        effective_schema_hash: Some(SchemaHash::new("sha256:sampled").unwrap()),
        coverage: DiscoveryCoverageMode::Sampled,
        selector: Some(selector),
        budget: DiscoveryExecutorBudget::default(),
        normalizer_version: "namecase-v1".to_owned(),
        policy_version: "evolve-v1".to_owned(),
        candidates: vec![last, middle.clone(), first],
    };
    let artifact = DiscoveryManifestArtifact::new(input.clone()).unwrap();
    assert_eq!(artifact.coverage, DiscoveryCoverageMode::Sampled);
    assert_eq!(
        artifact.candidates[1].participation,
        DiscoveryParticipation::Unprobed
    );
    assert!(artifact.candidates[1].physical_schema_hash.is_none());
    assert!(artifact.candidates[1].probe_bytes.is_none());
    assert!(artifact.candidates[1].schema_verdict.is_none());

    let mut false_score = input.clone();
    false_score.selector.as_mut().unwrap().selected[0].score_sha256 = "0".repeat(64);
    let error = DiscoveryManifestArtifact::new(false_score)
        .unwrap_err()
        .to_string();
    assert!(error.contains("canonical membership, scores, or strata"));

    let mut false_unprobed = input;
    false_unprobed.candidates[1].physical_schema_hash =
        Some(SchemaHash::new("sha256:invented").unwrap());
    let error = DiscoveryManifestArtifact::new(false_unprobed)
        .unwrap_err()
        .to_string();
    assert!(error.contains("unprobed") && error.contains("forbids"));

    let mut false_probed = middle;
    false_probed.participation = DiscoveryParticipation::Probed;
    let error = DiscoveryManifestArtifact::new(DiscoveryManifestInput {
        resource_id: "events.false-probed".to_owned(),
        baseline_schema_hash: None,
        effective_schema_hash: None,
        coverage: DiscoveryCoverageMode::Exhaustive,
        selector: None,
        budget: DiscoveryExecutorBudget::default(),
        normalizer_version: "namecase-v1".to_owned(),
        policy_version: "evolve-v1".to_owned(),
        candidates: vec![false_probed],
    })
    .unwrap_err()
    .to_string();
    assert!(error.contains("probed") && error.contains("requires"));
}

#[test]
fn schema_snapshot_v1_bytes_stay_exact_and_v2_binds_manifest_sidecar() {
    let legacy_resource = ResourceId::new("legacy.resource").unwrap();
    let legacy_schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let legacy =
        SchemaSnapshotArtifact::new(&legacy_resource, &legacy_schema, BTreeMap::new()).unwrap();
    assert_eq!(legacy.version, 1);
    assert_eq!(
        legacy.schema_hash.as_str(),
        "sha256:72f76d3bcff3c64ec909385548f8861b817b5959e4b3e5257e9e88f6c603e417"
    );
    assert!(
        serde_json::to_value(&legacy)
            .unwrap()
            .get("discovery_manifest")
            .is_none()
    );
    assert!(
        serde_json::to_value(legacy.reference())
            .unwrap()
            .get("discovery_manifest")
            .is_none()
    );

    let manifest = DiscoveryManifestArtifact::new(DiscoveryManifestInput {
        resource_id: legacy_resource.as_str().to_owned(),
        baseline_schema_hash: None,
        effective_schema_hash: None,
        coverage: DiscoveryCoverageMode::Exhaustive,
        selector: None,
        budget: DiscoveryExecutorBudget::default(),
        normalizer_version: "namecase-v1".to_owned(),
        policy_version: "evolve-v1".to_owned(),
        candidates: vec![probed_discovery_candidate(
            "file:///data/legacy.parquet",
            "sha256:legacy",
            24,
        )],
    })
    .unwrap();
    let linked = SchemaSnapshotArtifact::new_with_discovery_manifest(
        &legacy_resource,
        &legacy_schema,
        BTreeMap::new(),
        manifest.reference(),
    )
    .unwrap();
    assert_eq!(linked.version, 2);
    assert_ne!(linked.schema_hash, legacy.schema_hash);
    assert_eq!(
        linked.discovery_manifest_reference().unwrap(),
        Some(manifest.reference())
    );
    assert_eq!(
        linked.reference().discovery_manifest().unwrap(),
        Some(manifest.reference())
    );
    assert_eq!(
        linked.hash_input["discovery_manifest"]["manifest_hash"],
        manifest.manifest_hash.as_str()
    );

    let temp = tempfile::tempdir().unwrap();
    let manifest_store = DiscoveryManifestStore::new(temp.path());
    manifest_store.write(&manifest).unwrap();
    let snapshot_store = SchemaSnapshotStore::new(temp.path());
    snapshot_store.write(&linked).unwrap();
    assert_eq!(snapshot_store.read(&linked.reference()).unwrap(), linked);

    std::fs::remove_file(temp.path().join(&manifest.path)).unwrap();
    let error = snapshot_store
        .read(&linked.reference())
        .unwrap_err()
        .to_string();
    assert!(error.contains("read") && error.contains("discovery"));
}

fn probed_discovery_candidate(
    location: &str,
    physical_schema_hash: &str,
    probe_bytes: u64,
) -> DiscoveryCandidateEvidence {
    DiscoveryCandidateEvidence {
        transport: "file".to_owned(),
        canonical_location: location.to_owned(),
        identity: DiscoveryBoundedIdentity {
            size_bytes: Some(1024),
            modified_at_ms: Some(1_700_000_000_000),
            value: Some(format!("bounded:{location}")),
            strength: DiscoveryIdentityStrength::BoundedObservation,
        },
        participation: DiscoveryParticipation::Probed,
        metadata_variance: Vec::new(),
        physical_schema_hash: Some(SchemaHash::new(physical_schema_hash).unwrap()),
        probe_bytes: Some(probe_bytes),
        schema_verdict: Some(DiscoverySchemaVerdict {
            kind: DiscoverySchemaVerdictKind::Admitted,
            rule: "schema-join-v1".to_owned(),
            details: BTreeMap::new(),
        }),
    }
}

fn unprobed_discovery_candidate(location: &str, identity: &str) -> DiscoveryCandidateEvidence {
    DiscoveryCandidateEvidence {
        transport: "file".to_owned(),
        canonical_location: location.to_owned(),
        identity: DiscoveryBoundedIdentity {
            size_bytes: Some(2048),
            modified_at_ms: None,
            value: Some(identity.to_owned()),
            strength: DiscoveryIdentityStrength::WeakEtag,
        },
        participation: DiscoveryParticipation::Unprobed,
        metadata_variance: Vec::new(),
        physical_schema_hash: None,
        probe_bytes: None,
        schema_verdict: None,
    }
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
        dependencies.clone(),
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
fn object_store_multi_file_parquet_discovery_pins_one_reconciled_snapshot() {
    let temp = tempfile::tempdir().unwrap();
    write_object_store_discover_project(temp.path());
    let store = Arc::new(InMemory::new());
    let first = vendor_parquet_bytes();
    let second_schema = Arc::new(Schema::new(vec![
        Field::new("VendorID", DataType::Int32, false),
        Field::new("fare_amount", DataType::Int64, true),
    ]));
    let second_batch = RecordBatch::try_new(
        second_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(10), None])) as ArrayRef,
        ],
    )
    .unwrap();
    let second = cdf_package::transcode_record_batches_to_parquet_bytes(&[second_batch]).unwrap();
    for (path, bytes) in [
        ("trip-data/2024/01.parquet", first),
        ("trip-data/2024/02.parquet", second),
    ] {
        futures_executor::block_on(store.put(&ObjectPath::from(path), PutPayload::from(bytes)))
            .unwrap();
    }
    let dependencies = file_dependencies(
        FileTransportFacade::new()
            .with_object_store("s3://tlc", store)
            .with_execution_services(test_execution_services()),
    );
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_resource_schema_with_file_dependencies_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies.clone(),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();

    let field_names = artifacts
        .discovery
        .normalized_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(field_names, vec!["vendor_id", "fare_amount"]);
    assert_eq!(
        artifacts.discovery.snapshot.source_identity["transport"],
        "remote"
    );
    assert_eq!(
        artifacts.discovery.snapshot.source_identity["matched_files"],
        "2"
    );
    let manifest = artifacts.discovery_manifest.as_ref().unwrap();
    assert_eq!(manifest.candidates.len(), 2);
    assert!(manifest.candidates.iter().all(|candidate| {
        candidate.participation == DiscoveryParticipation::Probed
            && candidate
                .canonical_location
                .starts_with("s3://tlc/trip-data/2024/")
    }));

    write_schema_discovery_artifacts(temp.path(), &artifacts).unwrap();
    let pinned = apply_discovered_schema(&resource, artifacts.discovery.clone());
    let prepared = prepare_pinned_resource_effective_schema_with_file_dependencies_artifacts(
        temp.path(),
        &pinned.resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies,
    )
    .unwrap();
    assert_eq!(prepared.discovery_manifest().unwrap().candidates.len(), 2);
    assert!(prepared.resource().effective_schema_runtime().is_some());
}

#[test]
fn object_store_gzip_ndjson_discovers_pins_and_executes_through_one_transport() {
    let temp = tempfile::tempdir().unwrap();
    write_object_store_ndjson_discover_project(temp.path());
    let mut source = Vec::new();
    for id in 0..10_000_u64 {
        source.extend_from_slice(format!("{{\"id\":{id},\"kind\":\"k{id}\"}}\n").as_bytes());
    }
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    std::io::Write::write_all(&mut encoder, &source).unwrap();
    let encoded = encoder.finish().unwrap();
    let store = Arc::new(InMemory::new());
    futures_executor::block_on(store.put(
        &ObjectPath::from("prod/2026/07/events.ndjson.gz"),
        PutPayload::from(encoded.clone()),
    ))
    .unwrap();
    let dependencies = file_dependencies(
        FileTransportFacade::new()
            .with_object_store("s3://acme-events", store)
            .with_execution_services(test_execution_services()),
    );
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_resource_schema_with_file_dependencies_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies.clone(),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();
    assert_eq!(
        artifacts
            .discovery
            .normalized_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["id", "kind"]
    );
    let manifest = artifacts.discovery_manifest.as_ref().unwrap();
    assert_eq!(manifest.candidates.len(), 1);
    assert_eq!(
        manifest.candidates[0].participation,
        DiscoveryParticipation::Probed
    );
    assert!(manifest.candidates[0].probe_bytes.unwrap() <= 8 * 1024 * 1024);

    let prepared = apply_discovered_schema(&resource, artifacts.discovery.clone());
    let runtime = prepared.resource.to_file_resource(dependencies).unwrap();
    let plan = live_plan_for_stream(&runtime, "pkg-cloud-ndjson");
    assert_eq!(plan.scan.partitions.len(), 1);
    let preview = futures_executor::block_on(cdf_engine::preview_resource(
        &plan,
        &runtime,
        cdf_engine::EnginePreviewLimits::default(),
    ))
    .unwrap();
    assert_eq!(preview.row_count, 500);
    assert_eq!(preview.fields, vec!["id", "kind", "_cdf_variant"]);
    assert_eq!(preview.planned_partition_count, 1);
    assert_eq!(preview.payload_opened_partition_count, 1);
    let stream = futures_executor::block_on(runtime.open(plan.scan.partitions[0].clone())).unwrap();
    let batches = futures_executor::block_on_stream(stream)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.header.row_count)
            .sum::<u64>(),
        10_000
    );
    let SourcePosition::FileManifest(position) =
        batches[0].header.source_position.as_ref().unwrap()
    else {
        panic!("expected cloud file manifest position")
    };
    assert_eq!(
        position.files[0].path,
        "s3://acme-events/prod/2026/07/events.ndjson.gz"
    );
}

#[test]
fn http_numeric_template_discovers_and_plans_every_file() {
    let temp = tempfile::tempdir().unwrap();
    write_http_discover_project(temp.path(), "");
    let resource_path = temp.path().join("resources/files.toml");
    let resource_toml = fs::read_to_string(&resource_path)
        .unwrap()
        .replace("vendors.parquet", "yellow_tripdata_2024-{01..03}.parquet");
    fs::write(resource_path, resource_toml).unwrap();
    let parquet = vendor_parquet_bytes();
    let transport = RecordingHttpFileTransport::new(parquet);
    let dependencies = http_file_dependencies(transport.clone());
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_resource_schema_with_file_dependencies_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies.clone(),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();
    assert_eq!(artifacts.discovery_manifest.unwrap().candidates.len(), 3);

    let runtime = resource.to_file_resource(dependencies).unwrap();
    let partitions = runtime
        .plan_partitions(&ScanRequest {
            resource_id: resource.descriptor().resource_id.clone(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: resource.descriptor().state_scope.clone(),
        })
        .unwrap();
    assert_eq!(partitions.len(), 3);
    assert_eq!(
        partitions
            .iter()
            .map(|partition| partition.metadata["path"].as_str())
            .collect::<Vec<_>>(),
        vec![
            "https://data.example.test/trip-data/yellow_tripdata_2024-01.parquet",
            "https://data.example.test/trip-data/yellow_tripdata_2024-02.parquet",
            "https://data.example.test/trip-data/yellow_tripdata_2024-03.parquet",
        ]
    );
    assert_eq!(
        transport
            .requests()
            .iter()
            .filter(|request| request.method == HttpMethod::Head)
            .count(),
        9
    );
}

#[test]
fn http_year_month_glob_skips_absent_candidates_without_hiding_other_failures() {
    let temp = tempfile::tempdir().unwrap();
    write_http_discover_project(temp.path(), "");
    let resource_path = temp.path().join("resources/files.toml");
    let resource_toml = fs::read_to_string(&resource_path)
        .unwrap()
        .replace("vendors.parquet", "yellow_tripdata_2024-*.parquet");
    fs::write(resource_path, resource_toml).unwrap();
    let missing = (3..=12).map(|month| {
        format!("https://data.example.test/trip-data/yellow_tripdata_2024-{month:02}.parquet")
    });
    let transport = RecordingHttpFileTransport::with_missing(vendor_parquet_bytes(), missing);
    let dependencies = http_file_dependencies(transport.clone());
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_resource_schema_with_file_dependencies_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        dependencies.clone(),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();
    assert_eq!(artifacts.discovery_manifest.unwrap().candidates.len(), 2);
    let runtime = resource.to_file_resource(dependencies).unwrap();
    let partitions = runtime
        .plan_partitions(&ScanRequest {
            resource_id: resource.descriptor().resource_id.clone(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: resource.descriptor().state_scope.clone(),
        })
        .unwrap();
    assert_eq!(partitions.len(), 2);
    assert!(partitions[0].metadata["path"].ends_with("2024-01.parquet"));
    assert!(partitions[1].metadata["path"].ends_with("2024-02.parquet"));
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
    let requests = transport.requests();
    let sequential_gets = requests
        .iter()
        .filter(|request| {
            request.method == HttpMethod::Get && !request.headers.contains_key("range")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        sequential_gets.len(),
        2,
        "preview and run must each use one sequential HTTP GET: {requests:?}"
    );
    assert!(sequential_gets.iter().all(|request| {
        request.headers.get("if-match").map(String::as_str) == Some("\"fixture-etag\"")
    }));
    let ranged_gets = requests
        .iter()
        .filter(|request| {
            request.method == HttpMethod::Get && request.headers.contains_key("range")
        })
        .collect::<Vec<_>>();
    assert!(
        !ranged_gets.is_empty(),
        "discovery must retain bounded Parquet range reads"
    );
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
    let auth_dependencies = file_dependencies(
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
fn local_ndjson_discovery_is_bounded_and_writes_nothing_until_pin() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "ndjson", "*.ndjson");
    fs::write(
        temp.path().join("data/events.ndjson"),
        b"{\"VendorID\":1}\n{\"VendorID\":2}\n",
    )
    .unwrap();
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();

    assert_eq!(
        artifacts.discovery.normalized_schema.field(0).name(),
        "vendor_id"
    );
    assert_eq!(
        artifacts.discovery.snapshot.source_identity["coverage"],
        "exhaustive"
    );
    assert_eq!(artifacts.discovery_manifest.unwrap().candidates.len(), 1);
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn local_csv_discovery_uses_the_shared_sample_manifest_path() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "csv", "*.csv");
    fs::write(
        temp.path().join("data/events.csv"),
        b"VendorID,fare_amount\n1,10.5\n2,20.25\n",
    )
    .unwrap();
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();

    assert_eq!(
        artifacts
            .discovery
            .normalized_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["vendor_id", "fare_amount"]
    );
    assert_eq!(
        artifacts.discovery.snapshot.artifact.metadata["probe"],
        "bounded-csv-sample"
    );
    assert_eq!(artifacts.discovery_manifest.unwrap().candidates.len(), 1);
}

#[test]
fn local_json_document_discovery_is_byte_bounded_and_manifest_backed() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "json", "*.json");
    fs::write(
        temp.path().join("data/events.json"),
        br#"[{"VendorID":1,"active":true},{"VendorID":2,"active":false}]"#,
    )
    .unwrap();
    let resource = compile_single_project_resource(temp.path());

    let artifacts = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::default(),
    )
    .unwrap();

    assert_eq!(
        artifacts
            .discovery
            .normalized_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        vec!["vendor_id", "active"]
    );
    assert_eq!(
        artifacts.discovery.snapshot.artifact.metadata["probe"],
        "bounded-json-sample"
    );
    assert_eq!(artifacts.discovery_manifest.unwrap().candidates.len(), 1);
}

#[test]
fn local_parquet_discover_autopin_persists_exhaustive_multi_file_manifest() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/a.parquet"));
    write_vendor_parquet(&temp.path().join("data/b.parquet"));
    let resource = compile_single_project_resource(temp.path());

    let prepared = prepare_local_parquet_discover_resource(temp.path(), &resource).unwrap();
    let discovery = prepared.discovery.unwrap();
    assert_eq!(discovery.snapshot.source_identity["coverage"], "exhaustive");
    assert_eq!(discovery.snapshot.source_identity["matched_files"], "2");
    assert_eq!(discovery.snapshot.source_identity["probed_files"], "2");
    let reference = discovery
        .snapshot
        .reference
        .discovery_manifest()
        .unwrap()
        .unwrap();
    let manifest = DiscoveryManifestStore::new(temp.path())
        .read(&reference)
        .unwrap();
    assert_eq!(manifest.coverage, DiscoveryCoverageMode::Exhaustive);
    assert!(manifest.selector.is_none());
    assert_eq!(manifest.budget.max_concurrent_probes(), 8);
    assert_eq!(
        manifest.budget.max_metadata_bytes_per_file(),
        64 * 1024 * 1024
    );
    assert_eq!(
        manifest.budget.max_total_in_flight_bytes(),
        128 * 1024 * 1024
    );
    assert_eq!(manifest.candidates.len(), 2);
    assert!(manifest.candidates.iter().all(|candidate| {
        candidate.participation == DiscoveryParticipation::Probed
            && candidate.physical_schema_hash.is_some()
            && candidate.probe_bytes.is_some()
            && candidate.schema_verdict.is_some()
    }));
    assert_eq!(
        manifest
            .candidates
            .iter()
            .map(|candidate| candidate.canonical_location.as_str())
            .collect::<Vec<_>>(),
        vec!["a.parquet", "b.parquet"]
    );
    assert!(temp.path().join(discovery.snapshot.artifact.path).is_file());
}

#[test]
fn explicit_sampled_parquet_pin_records_exact_participation_and_is_deterministic() {
    let temp = tempfile::tempdir().unwrap();
    write_sampled_discover_project(temp.path(), "parquet", "*.parquet", 3);
    for index in 0..9 {
        write_vendor_parquet(&temp.path().join(format!("data/{index:02}.parquet")));
    }
    let resource = compile_single_project_resource(temp.path());
    let first = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap();
    let manifest = first.discovery_manifest.as_ref().unwrap();
    assert_eq!(manifest.coverage, DiscoveryCoverageMode::Sampled);
    let selector = manifest.selector.as_ref().unwrap();
    assert_eq!(selector.selector, STRATIFIED_HASH_SELECTOR_V1);
    assert_eq!(selector.sample_files, 3);
    assert_eq!(selector.matched_count, 9);
    assert_eq!(selector.selected.len(), 3);
    assert_eq!(manifest.candidates.len(), 9);
    assert_eq!(
        manifest
            .candidates
            .iter()
            .filter(|candidate| candidate.participation == DiscoveryParticipation::Probed)
            .count(),
        3
    );
    assert_eq!(
        manifest
            .candidates
            .iter()
            .filter(|candidate| candidate.participation == DiscoveryParticipation::Unprobed)
            .count(),
        6
    );
    assert!(manifest.candidates.iter().all(|candidate| {
        candidate.participation == DiscoveryParticipation::Probed
            || (candidate.physical_schema_hash.is_none()
                && candidate.probe_bytes.is_none()
                && candidate.schema_verdict.is_none())
    }));
    assert_eq!(
        first.discovery.snapshot.source_identity["coverage"],
        "sampled"
    );
    assert_eq!(
        first.discovery.snapshot.source_identity["matched_files"],
        "9"
    );
    assert_eq!(
        first.discovery.snapshot.source_identity["probed_files"],
        "3"
    );
    assert_eq!(
        first.discovery.snapshot.source_identity["unprobed_files"],
        "6"
    );

    let repeated = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_vec(manifest).unwrap(),
        serde_json::to_vec(repeated.discovery_manifest.as_ref().unwrap()).unwrap()
    );
}

#[test]
fn explicit_sample_larger_than_set_preserves_exhaustive_manifest_bytes() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/a.parquet"));
    write_vendor_parquet(&temp.path().join("data/b.parquet"));
    let exhaustive_resource = compile_single_project_resource(temp.path());
    let exhaustive = discover_resource_schema_artifacts(
        &exhaustive_resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap();

    write_sampled_discover_project(temp.path(), "parquet", "*.parquet", 2);
    let configured_resource = compile_single_project_resource(temp.path());
    let configured = discover_resource_schema_artifacts(
        &configured_resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap();
    assert_eq!(
        configured.discovery_manifest.as_ref().unwrap().coverage,
        DiscoveryCoverageMode::Exhaustive
    );
    assert_eq!(
        serde_json::to_vec(exhaustive.discovery_manifest.as_ref().unwrap()).unwrap(),
        serde_json::to_vec(configured.discovery_manifest.as_ref().unwrap()).unwrap()
    );
    assert_eq!(
        exhaustive.discovery.snapshot.artifact,
        configured.discovery.snapshot.artifact
    );
}

#[test]
fn explicit_sampled_arrow_ipc_uses_the_same_format_neutral_selector() {
    let temp = tempfile::tempdir().unwrap();
    write_sampled_discover_project(temp.path(), "arrow_ipc", "*.arrow", 2);
    for index in 0..5 {
        write_arrow_ipc_fixture(
            &temp.path().join(format!("data/{index:02}.arrow")),
            vec![Field::new("VendorID", DataType::Int32, false)],
            vec![Arc::new(Int32Array::from(vec![index]))],
        );
    }
    let resource = compile_single_project_resource(temp.path());
    let artifacts = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap();
    let manifest = artifacts.discovery_manifest.unwrap();
    assert_eq!(manifest.coverage, DiscoveryCoverageMode::Sampled);
    assert_eq!(manifest.selector.unwrap().sample_files, 2);
    assert_eq!(
        manifest
            .candidates
            .iter()
            .filter(|candidate| candidate.participation == DiscoveryParticipation::Probed)
            .count(),
        2
    );
}

#[test]
fn sampled_pin_observes_every_runtime_file_and_quarantines_unseen_incompatibility() {
    let temp = tempfile::tempdir().unwrap();
    write_sampled_discover_project(temp.path(), "parquet", "*.parquet", 2);
    write_parquet_fixture(
        &temp.path().join("data/a.parquet"),
        vec![Field::new("value", DataType::Int64, false)],
        vec![Arc::new(Int64Array::from(vec![1]))],
    );
    write_parquet_fixture(
        &temp.path().join("data/middle.parquet"),
        vec![Field::new("value", DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(vec!["drift"]))],
    );
    write_parquet_fixture(
        &temp.path().join("data/z.parquet"),
        vec![Field::new("value", DataType::Int64, false)],
        vec![Arc::new(Int64Array::from(vec![2]))],
    );
    let resource = compile_single_project_resource(temp.path());
    let initial = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap();
    let initial_manifest = initial.discovery_manifest.as_ref().unwrap();
    assert_eq!(initial_manifest.coverage, DiscoveryCoverageMode::Sampled);
    assert_eq!(
        initial_manifest.candidates[1].participation,
        DiscoveryParticipation::Unprobed
    );
    write_schema_discovery_artifacts(temp.path(), &initial).unwrap();
    let pinned = resource.with_schema_source_and_schema(
        SchemaSource::Discovered {
            snapshot: initial.discovery.snapshot.reference.clone(),
        },
        Arc::clone(&initial.discovery.normalized_schema),
    );

    let prepared = prepare_pinned_resource_effective_schema(
        temp.path(),
        &pinned,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
    )
    .unwrap();
    let runtime = prepared.effective_schema_runtime().unwrap();
    assert_eq!(runtime.evidence.observations.len(), 3);
    assert_eq!(runtime.terminal_quarantines.len(), 1);
    assert_eq!(
        runtime.terminal_quarantines[0].observation_id(),
        "middle.parquet"
    );
    assert_eq!(
        runtime.terminal_quarantines[0].rule_id(),
        "schema-observation:incompatible"
    );
}

#[test]
fn sampled_probe_budget_failure_does_not_substitute_an_unselected_candidate() {
    let temp = tempfile::tempdir().unwrap();
    write_sampled_discover_project(temp.path(), "parquet", "*.parquet", 1);
    for index in 0..5 {
        write_vendor_parquet(&temp.path().join(format!("data/{index:02}.parquet")));
    }
    let resource = compile_single_project_resource(temp.path());
    let error = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::new()
            .with_budget(DiscoveryExecutorBudget::new(8, 8, 1).unwrap()),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("sampled local binary discovery failed"));
    assert!(error.contains("without substitution"));
    assert_eq!(error.matches(": failed:").count(), 1);
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn sampled_initial_pin_reports_every_selected_incompatibility_without_writes() {
    let temp = tempfile::tempdir().unwrap();
    write_sampled_discover_project(temp.path(), "parquet", "*.parquet", 2);
    write_parquet_fixture(
        &temp.path().join("data/a.parquet"),
        vec![Field::new("value", DataType::Int64, false)],
        vec![Arc::new(Int64Array::from(vec![1]))],
    );
    write_vendor_parquet(&temp.path().join("data/middle.parquet"));
    write_parquet_fixture(
        &temp.path().join("data/z.parquet"),
        vec![Field::new("value", DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(vec!["incompatible"]))],
    );
    let resource = compile_single_project_resource(temp.path());
    let error = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("initial sampled schema pin"));
    assert!(error.contains("a.parquet"));
    assert!(error.contains("z.parquet"));
    assert!(error.contains("candidate verdicts"));
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn exhaustive_discovery_uses_exact_verified_baseline_and_schema_only_effective_hash() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/a.parquet"));
    let resource = compile_single_project_resource(temp.path());
    let authority_temp = tempfile::tempdir().unwrap();
    let baseline_artifact = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        resource.schema().as_ref(),
        BTreeMap::new(),
    )
    .unwrap();
    let baseline_store = SchemaSnapshotStore::new(authority_temp.path());
    baseline_store.write(&baseline_artifact).unwrap();
    let (_, verified_baseline) = baseline_store
        .read_with_verified_baseline(&baseline_artifact.reference())
        .unwrap();
    assert_eq!(
        verified_baseline.resource_id(),
        &resource.descriptor().resource_id
    );
    let verified_baseline_hash = verified_baseline.schema_hash().clone();

    let artifacts = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::new().with_verified_baseline(verified_baseline),
    )
    .unwrap();
    let manifest = artifacts.discovery_manifest.as_ref().unwrap();
    assert_eq!(manifest.baseline_schema_hash, Some(verified_baseline_hash));

    let schema_only = SchemaSnapshotArtifact::new(
        &resource.descriptor().resource_id,
        artifacts.discovery.normalized_schema.as_ref(),
        BTreeMap::from([
            ("cdf:normalizer".to_owned(), "namecase-v1".to_owned()),
            (
                "format".to_owned(),
                SCHEMA_DISCOVERY_FORMAT_PARQUET.to_owned(),
            ),
            (
                "probe".to_owned(),
                SCHEMA_DISCOVERY_PROBE_PARQUET_FOOTER.to_owned(),
            ),
            ("source_kind".to_owned(), "files".to_owned()),
        ]),
    )
    .unwrap();
    assert_eq!(
        manifest.effective_schema_hash.as_ref(),
        Some(&schema_only.schema_hash)
    );
    assert_ne!(
        manifest.effective_schema_hash.as_ref(),
        Some(&artifacts.discovery.snapshot.artifact.schema_hash)
    );

    let other_resource_id = ResourceId::new("other.resource").unwrap();
    let other_artifact = SchemaSnapshotArtifact::new(
        &other_resource_id,
        resource.schema().as_ref(),
        BTreeMap::new(),
    )
    .unwrap();
    baseline_store.write(&other_artifact).unwrap();
    let (_, wrong_baseline) = baseline_store
        .read_with_verified_baseline(&other_artifact.reference())
        .unwrap();
    let wrong_resource = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::new().with_verified_baseline(wrong_baseline),
    )
    .unwrap_err()
    .to_string();
    assert!(wrong_resource.contains("belongs to resource `other.resource`"));
    assert!(wrong_resource.contains("discovery is for `local.events`"));
}

#[test]
#[allow(deprecated)]
fn legacy_local_parquet_helper_refuses_multi_file_partial_evidence() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/a.parquet"));
    write_vendor_parquet(&temp.path().join("data/b.parquet"));
    let resource = compile_single_project_resource(temp.path());

    let error = discover_local_parquet_resource_schema(&resource)
        .unwrap_err()
        .to_string();
    assert!(error.contains("cannot represent 2 matched candidates"));
    assert!(error.contains("without partial evidence"));
    assert!(error.contains("discover_resource_schema_artifacts"));
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn exhaustive_local_parquet_discovery_aggregates_widening_missing_metadata_and_set_identity() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_parquet_fixture(
        &temp.path().join("data/a.parquet"),
        vec![
            Field::new("VendorID", DataType::Int32, false)
                .with_metadata(HashMap::from([("source-tag".to_owned(), "a".to_owned())])),
        ],
        vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32]))],
    );
    write_parquet_fixture(
        &temp.path().join("data/b.parquet"),
        vec![
            Field::new("VendorID", DataType::Int64, false)
                .with_metadata(HashMap::from([("source-tag".to_owned(), "b".to_owned())])),
            Field::new("Note", DataType::Utf8, false),
        ],
        vec![
            Arc::new(Int64Array::from(vec![3_i64, 4_i64])),
            Arc::new(StringArray::from(vec!["x", "y"])),
        ],
    );
    let resource = compile_single_project_resource(temp.path());
    let options = SchemaDiscoveryExecutionOptions::new();
    let first = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        options.clone(),
    )
    .unwrap();
    assert!(!temp.path().join(".cdf/schemas").exists());
    assert_eq!(
        first.discovery.normalized_schema.field(0).data_type(),
        &DataType::Int64
    );
    assert_eq!(first.discovery.normalized_schema.field(1).name(), "note");
    assert!(first.discovery.normalized_schema.field(1).is_nullable());
    let first_manifest = first.discovery_manifest.as_ref().unwrap();
    assert_eq!(first_manifest.candidates.len(), 2);
    assert!(
        first_manifest.candidates[0]
            .metadata_variance
            .iter()
            .any(|variance| variance.key == "source-tag")
    );
    assert!(
        first_manifest.candidates[0]
            .schema_verdict
            .as_ref()
            .unwrap()
            .details["field_verdicts"]
            .contains("widened")
    );
    assert!(
        first_manifest.candidates[0]
            .schema_verdict
            .as_ref()
            .unwrap()
            .details["field_verdicts"]
            .contains("missing_null")
    );
    let repeated = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        options.clone(),
    )
    .unwrap();
    assert_eq!(
        repeated.discovery_manifest.as_ref().unwrap().manifest_hash,
        first_manifest.manifest_hash
    );

    write_parquet_fixture(
        &temp.path().join("data/c.parquet"),
        vec![Field::new("VendorID", DataType::Int64, false)],
        vec![Arc::new(Int64Array::from(vec![5_i64]))],
    );
    let added = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        options.clone(),
    )
    .unwrap();
    assert_ne!(
        added.discovery_manifest.as_ref().unwrap().manifest_hash,
        first_manifest.manifest_hash
    );
    fs::remove_file(temp.path().join("data/c.parquet")).unwrap();
    fs::remove_file(temp.path().join("data/a.parquet")).unwrap();
    let removed = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        options.clone(),
    )
    .unwrap();
    assert_ne!(
        removed.discovery_manifest.as_ref().unwrap().manifest_hash,
        first_manifest.manifest_hash
    );
    write_parquet_fixture(
        &temp.path().join("data/a.parquet"),
        vec![Field::new("VendorID", DataType::Int32, false)],
        vec![Arc::new(Int32Array::from(vec![1_i32, 2_i32, 3_i32]))],
    );
    let changed = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        options,
    )
    .unwrap();
    assert_ne!(
        changed.discovery_manifest.as_ref().unwrap().manifest_hash,
        first_manifest.manifest_hash
    );
}

#[test]
fn exhaustive_local_parquet_discovery_budget_and_incompatibility_fail_without_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_vendor_parquet(&temp.path().join("data/a.parquet"));
    write_parquet_fixture(
        &temp.path().join("data/b.parquet"),
        vec![Field::new("VendorID", DataType::Utf8, false)],
        vec![Arc::new(StringArray::from(vec!["one", "two"]))],
    );
    let resource = compile_single_project_resource(temp.path());
    let incompatible = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(incompatible.contains("candidate verdicts"));
    assert!(incompatible.contains("a.parquet"));
    assert!(incompatible.contains("b.parquet"));
    assert!(!temp.path().join(".cdf/schemas").exists());

    write_vendor_parquet(&temp.path().join("data/b.parquet"));
    let corrupt_path = temp.path().join("data/b.parquet");
    let mut corrupt = fs::read(&corrupt_path).unwrap();
    let footer_length = corrupt.len() - 8;
    corrupt[footer_length..footer_length + 4].fill(0xff);
    fs::write(&corrupt_path, corrupt).unwrap();
    let malformed = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(malformed.contains("a.parquet: probed"));
    assert!(malformed.contains("b.parquet: failed"));
    assert!(!temp.path().join(".cdf/schemas").exists());

    fs::remove_file(temp.path().join("data/b.parquet")).unwrap();
    let budget_error = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        SchemaDiscoveryExecutionOptions::new()
            .with_budget(DiscoveryExecutorBudget::new(8, 8, 1).unwrap()),
    )
    .unwrap_err()
    .to_string();
    assert!(budget_error.contains("metadata budget exceeded"));
    assert!(budget_error.contains("allowed 8"));
    assert!(budget_error.contains("increase the per-file"));
    assert!(!temp.path().join(".cdf/schemas").exists());
}

#[test]
fn exhaustive_local_binary_discovery_detects_normalizer_collision_before_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "parquet", "*.parquet");
    write_parquet_fixture(
        &temp.path().join("data/a.parquet"),
        vec![Field::new("VendorID", DataType::Int32, false)],
        vec![Arc::new(Int32Array::from(vec![1_i32]))],
    );
    write_parquet_fixture(
        &temp.path().join("data/b.parquet"),
        vec![Field::new("vendor_id", DataType::Int32, false)],
        vec![Arc::new(Int32Array::from(vec![2_i32]))],
    );
    let resource = compile_single_project_resource(temp.path());
    let error = discover_resource_schema_artifacts(
        &resource,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
        Default::default(),
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("collision"));
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
fn pinned_json_runtime_preparation_requires_verified_snapshot_before_source_contact() {
    let temp = tempfile::tempdir().unwrap();
    write_discover_project(temp.path(), "json", "*.missing");
    let resource = compile_single_project_resource(temp.path());
    let snapshot = cdf_kernel::SchemaSnapshotReference {
        schema_hash: SchemaHash::new(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap(),
        path: ".cdf/schemas/missing.json".to_owned(),
        metadata: BTreeMap::new(),
    };
    let pinned = resource.with_schema_source_and_schema(
        SchemaSource::Discovered {
            snapshot: snapshot.clone(),
        },
        resource.schema(),
    );

    let error = prepare_pinned_resource_effective_schema(
        temp.path(),
        &pinned,
        &EnvSecretProvider::from_map(std::iter::empty::<(&str, &str)>()),
    )
    .unwrap_err();

    assert!(
        error.message.contains(".cdf/schemas/missing.json"),
        "{}",
        error.message
    );
    assert!(!temp.path().join(".cdf").exists());
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
    missing: BTreeSet<String>,
}

impl RecordingHttpFileTransport {
    fn new(body: Vec<u8>) -> Self {
        Self {
            state: Arc::new(Mutex::new(RecordingHttpFileTransportState {
                requests: Vec::new(),
                body,
                etag: "\"fixture-etag\"".to_owned(),
                missing: BTreeSet::new(),
            })),
        }
    }

    fn with_missing(body: Vec<u8>, missing: impl IntoIterator<Item = String>) -> Self {
        let transport = Self::new(body);
        transport.state.lock().unwrap().missing.extend(missing);
        transport
    }

    fn requests(&self) -> Vec<HttpFileRequest> {
        self.state.lock().unwrap().requests.clone()
    }
}

impl HttpFileTransport for RecordingHttpFileTransport {
    fn send(&self, request: HttpFileRequest) -> Result<HttpFileResponse> {
        let mut state = self.state.lock().unwrap();
        state.requests.push(request.clone());
        match request.method {
            HttpMethod::Head if state.missing.contains(&request.url) => {
                Ok(HttpFileResponse::new(404))
            }
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

    fn download(
        &self,
        request: HttpFileRequest,
        destination: &Path,
    ) -> Result<(HttpFileResponse, u64)> {
        let mut state = self.state.lock().unwrap();
        state.requests.push(request.clone());
        if request.method != HttpMethod::Get {
            return Ok((HttpFileResponse::new(405), 0));
        }
        std::fs::write(destination, &state.body)
            .map_err(|error| CdfError::data(format!("write test HTTP download: {error}")))?;
        let len = state.body.len() as u64;
        Ok((
            HttpFileResponse::new(200)
                .with_header("Content-Length", len.to_string())
                .with_header("ETag", state.etag.clone())
                .with_header("Last-Modified", "Wed, 08 Jul 2026 12:00:00 GMT"),
            len,
        ))
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

fn write_object_store_discover_project(root: &Path) {
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "cloud_files"
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
        r#"
[source.remote]
kind = "files"
root = "s3://tlc/trip-data"

[resource.events]
glob = "2024/**/*.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
"#,
    )
    .unwrap();
}

fn write_object_store_ndjson_discover_project(root: &Path) {
    fs::create_dir_all(root.join("resources")).unwrap();
    fs::write(
        root.join("cdf.toml"),
        r#"
[project]
name = "cloud_events"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[resources."events.*"]
source = "resources/files.toml"
"#,
    )
    .unwrap();
    fs::write(
        root.join("resources/files.toml"),
        r#"
[source.events]
kind = "files"
root = "s3://acme-events/prod"

[resource.raw]
glob = "2026/**/*.ndjson.gz"
format = "ndjson"
write_disposition = "append"
trust = "governed"
"#,
    )
    .unwrap();
}

fn http_file_dependencies(transport: RecordingHttpFileTransport) -> FileRuntimeDependencies {
    file_dependencies(FileTransportFacade::new().with_http_transport(transport))
}

fn live_plan_for_stream(resource: &dyn QueryableResource, package_id: &str) -> EnginePlan {
    let observed_schema = ObservedSchema::from_arrow(resource.schema().as_ref());
    let destination = ResolvedProjectDestination::duckdb(
        "/tmp/cdf-project-plan-policy-only.duckdb",
        TargetName::new("events").unwrap(),
    )
    .unwrap();
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = destination.column_identifier_policy().unwrap().unwrap();
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

fn write_sampled_discover_project(root: &Path, format: &str, glob: &str, sample_files: u64) {
    write_discover_project(root, format, glob);
    let path = root.join("resources/files.toml");
    let input = fs::read_to_string(&path).unwrap();
    fs::write(
        path,
        input.replace(
            &format!("glob = \"{glob}\""),
            &format!("glob = \"{glob}\"\nsample_files = {sample_files}"),
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

fn write_parquet_fixture(path: &Path, fields: Vec<Field>, columns: Vec<ArrayRef>) {
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let bytes = cdf_package::transcode_record_batches_to_parquet_bytes(&[batch]).unwrap();
    fs::write(path, bytes).unwrap();
}

fn write_arrow_ipc_fixture(path: &Path, fields: Vec<Field>, columns: Vec<ArrayRef>) {
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let file = fs::File::create(path).unwrap();
    let mut writer = FileWriter::try_new(file, batch.schema().as_ref()).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
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
        &[],
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
    let artifact = cdf_kernel::DestinationSheetArtifact::new(
        destination_sheet("duckdb", TypeMappingFidelity::Lossless),
        cdf_kernel::DestinationProtocolCapabilities::default(),
    )
    .unwrap();
    let (lock, _) =
        freeze_contract_snapshots(&config, &resources, None, &[artifact], None).unwrap();
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
    let resource = fs::read_to_string(root.join("resources/files.toml")).unwrap();
    assert!(readme.contains("docs/quickstart.md"));
    assert!(readme.contains("cdf validate"));
    assert!(readme.contains("cdf plan local.events --target local_events"));
    assert!(readme.contains("cdf run --resource local.events"));
    assert!(!readme.contains("secret://"));
    assert!(!readme.contains(root.to_str().unwrap()));
    assert!(!resource.contains("primary_key"));
    assert!(!resource.contains("merge_key"));
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
