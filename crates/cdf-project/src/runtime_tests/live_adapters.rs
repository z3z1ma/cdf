use super::{
    Arc, BTreeMap, CheckpointId, CheckpointStatus, ContractPolicy, CursorValue, DEDUP_SUMMARY_FILE,
    DedupKeep, DeliveryGuarantee, HttpResponse, IdempotencySupport, Ordering,
    PackageArtifactReplayRequest, PackageReader, PackageStatus, ParquetDestination, Path,
    PipelineId, PostgresDestination, PostgresTarget, ProjectDestinationRegistry,
    ProjectReceiptSource, ProjectResolutionContext, ProjectRunReport, ProjectRunRequest,
    ProjectRunSource, Receipt, ResolvedProjectDestination, ResourceStream, RowRule, RunEventKind,
    RunId, RunTelemetryConfig, SchemaHash, SchemaSource, SegmentEntry, SourcePosition,
    SqliteCheckpointStore, TargetName, WriteDisposition, fs, replay_package_from_artifacts,
    resolve_project_run_destination, run_project_with_scheduler_and_telemetry,
    support::{
        BackfillMockResource, BoundTestResource, LivePostgres, MockDestination,
        MockProjectDestinationRuntime, OwnedTestResource, RecordingResponse, RecordingTransport,
        SIMPLE_FILE_RESOURCE_APPEND, StaticSecretProvider, compile_test_file_resource,
        compiled_test_source_plan, destination, live_plan, live_plan_with_exact_policy,
        live_plan_with_policy, package_id_name_rows, parquet_project_run_request,
        postgres_project_run_request, postgres_runtime_resource, project_run_request,
        resolve_postgres_resource, resolve_rest_resource, resolved_duckdb_destination,
        rest_compile_registry, run_project, run_project_fixture, simple_file_resource,
        test_execution_services,
    },
};

pub(super) fn json_response(body: &str) -> RecordingResponse {
    RecordingResponse {
        response: HttpResponse::new(200),
        body: body.as_bytes().to_vec(),
    }
}

pub(super) fn package_identity_file_paths(
    reader: &PackageReader,
) -> std::collections::BTreeSet<String> {
    let mut paths = std::collections::BTreeSet::new();
    reader
        .for_each_identity_file(&mut |entry| {
            paths.insert(entry.path);
            Ok(())
        })
        .unwrap();
    paths
}

pub(super) fn package_identity_segments(reader: &PackageReader) -> Vec<SegmentEntry> {
    let mut segments = Vec::new();
    reader
        .for_each_identity_segment(&mut |entry| {
            segments.push(entry);
            Ok(())
        })
        .unwrap();
    segments
}

pub(super) const SIMPLE_FILE_RESOURCE_MERGE: &str = r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.ndjson"
format = "ndjson"
primary_key = ["id"]
merge_key = ["id"]
write_disposition = "merge"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "name", type = "string", nullable = true },
] }
"#;
pub(super) const REST_RESOURCE: &str = r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"

[resource.items]
path = "/items"
records = "$"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
] }
"#;
pub(super) const REST_RUNTIME_RESOURCE: &str = r#"
[source.api]
kind = "rest"
base_url = "https://api.example.test"
auth = { kind = "bearer", token = "secret://env/API_TOKEN" }
egress_allowlist = ["api.example.test"]

[resource.items]
path = "/items"
paginate = { kind = "next_token", query_param = "page_token", response_field = "next_token" }
records = "$.items"
primary_key = ["id"]
cursor = { field = "updated_at", param = "since", ordering = "exact", lag = "0ms" }
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
  { name = "updated_at", type = "int64", nullable = false },
] }
"#;
pub(super) fn long_identifier_file_resource(root: &Path, source_name: &str) -> OwnedTestResource {
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(
        root.join("data/events.ndjson"),
        format!("{{\"VendorID\":1,\"{source_name}\":10}}\n"),
    )
    .unwrap();
    let document = format!(
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.ndjson"
format = "ndjson"
write_disposition = "append"
trust = "governed"
schema = {{ fields = [
  {{ name = "VendorID", type = "int64", nullable = false }},
  {{ name = "{source_name}", type = "int64", nullable = false }},
] }}
"#,
    );
    compile_test_file_resource(root, &document)
}

pub(super) fn rest_resource() -> cdf_declarative::CompiledResource {
    let document = cdf_declarative::parse_toml(REST_RESOURCE).unwrap();
    cdf_declarative::compile_document(&rest_compile_registry(), &document)
        .unwrap()
        .remove(0)
}

pub(super) fn rest_runtime_resource() -> cdf_declarative::CompiledResource {
    let document = cdf_declarative::parse_toml(REST_RUNTIME_RESOURCE).unwrap();
    cdf_declarative::compile_document(&rest_compile_registry(), &document)
        .unwrap()
        .remove(0)
}

pub(super) fn run_rest_project(
    root: &Path,
    run_id: &str,
) -> (ProjectRunReport, RecordingTransport) {
    let (report, transport, _) = run_rest_project_with_jobs(root, run_id, None);
    (report, transport)
}

pub(super) fn assert_jobs_invariant_receipt(actual: &Receipt, expected: &Receipt) {
    assert_eq!(actual.receipt_id, expected.receipt_id);
    assert_eq!(actual.destination, expected.destination);
    assert_eq!(actual.target, expected.target);
    assert_eq!(actual.package_hash, expected.package_hash);
    assert_eq!(actual.segment_acks, expected.segment_acks);
    assert_eq!(actual.disposition, expected.disposition);
    assert_eq!(actual.idempotency_token, expected.idempotency_token);
    assert_eq!(actual.counts, expected.counts);
    assert_eq!(actual.schema_hash, expected.schema_hash);
    assert_eq!(actual.migrations, expected.migrations);
    assert_eq!(actual.verify, expected.verify);
}

pub(super) fn run_rest_project_with_jobs(
    root: &Path,
    run_id: &str,
    jobs: Option<u16>,
) -> (ProjectRunReport, RecordingTransport, u16) {
    let compiled = rest_runtime_resource();
    let services = test_execution_services();
    let host_jobs = services.capabilities().logical_cpu_slots;
    let services = services
        .with_run_job_ceiling(jobs.unwrap_or(host_jobs))
        .unwrap();
    let transport = RecordingTransport::new([
        json_response(
            r#"{ "next_token": "page-2", "items": [
                { "id": 1, "updated_at": 10 }
            ] }"#,
        ),
        json_response(
            r#"{ "items": [
                { "id": 2, "updated_at": 20 }
            ] }"#,
        ),
    ]);
    let resource = resolve_rest_resource(
        &compiled,
        transport.clone(),
        Arc::new(StaticSecretProvider::new([(
            "secret://env/API_TOKEN",
            "token-1",
        )])),
        &services,
    );
    let package_id = "pkg-general-rest-runtime";
    let package_root = root.join(".cdf/packages");
    let state_path = root.join(".cdf/state.db");
    let duckdb_path = root.join(".cdf/dev.duckdb");

    let source = compiled.source_plan().clone();
    let destination =
        crate::test_destinations::duckdb(duckdb_path, TargetName::new("items").unwrap()).unwrap();
    let plan = live_plan(&resource, package_id)
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(&source, &destination.runtime_capabilities())
        .unwrap();
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        plan.scan.partition_count().unwrap(),
        &source.execution_capabilities,
        &destination.runtime_capabilities(),
        &services,
        jobs,
    )
    .unwrap();
    services
        .tighten_run_job_ceiling(scheduler.effective_jobs.jobs)
        .unwrap();
    let effective_jobs = scheduler.effective_jobs.jobs;
    let report = futures_executor::block_on(run_project_with_scheduler_and_telemetry(
        ProjectRunRequest {
            resource: ProjectRunSource::new(&resource),
            plan,
            package_root,
            state_store_path: state_path,
            state_store_path_ownership: crate::StateStorePathOwnership::Configured,
            pipeline_id: PipelineId::new("pipeline-live").unwrap(),
            package_id: package_id.to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-general-rest-runtime").unwrap(),
            destination,
            run_id: Some(RunId::new(run_id).unwrap()),
            event_sink: None,
            after_receipt_verified: None,
        },
        &services,
        Some(scheduler),
        RunTelemetryConfig::disabled(),
    ))
    .unwrap()
    .into_committed()
    .unwrap();
    (report, transport, effective_jobs)
}

pub(super) fn run_postgres_project_with_jobs(
    compiled: &cdf_declarative::CompiledResource,
    database_url: &str,
    root: &Path,
    jobs: Option<u16>,
) -> (ProjectRunReport, u16) {
    let services = test_execution_services();
    let host_jobs = services.capabilities().logical_cpu_slots;
    let services = services
        .with_run_job_ceiling(jobs.unwrap_or(host_jobs))
        .unwrap();
    let resource = resolve_postgres_resource(compiled, database_url, &services);
    let package_id = "pkg-general-sql-runtime";
    let destination = crate::test_destinations::duckdb(
        root.join(".cdf/dev.duckdb"),
        TargetName::new("orders").unwrap(),
    )
    .unwrap();
    let source = compiled.source_plan().clone();
    let plan = live_plan(&resource, package_id)
        .bind_compiled_source(&source)
        .unwrap()
        .bind_operator_graph(&source, &destination.runtime_capabilities())
        .unwrap();
    let scheduler = cdf_runtime::resolve_runtime_scheduler(
        plan.scan.partition_count().unwrap(),
        &source.execution_capabilities,
        &destination.runtime_capabilities(),
        &services,
        jobs,
    )
    .unwrap();
    services
        .tighten_run_job_ceiling(scheduler.effective_jobs.jobs)
        .unwrap();
    let effective_jobs = scheduler.effective_jobs.jobs;
    let report = futures_executor::block_on(run_project_with_scheduler_and_telemetry(
        ProjectRunRequest {
            resource: ProjectRunSource::new(&resource),
            plan,
            package_root: root.join(".cdf/packages"),
            state_store_path: root.join(".cdf/state.db"),
            state_store_path_ownership: crate::StateStorePathOwnership::Configured,
            pipeline_id: PipelineId::new("pipeline-live").unwrap(),
            package_id: package_id.to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-general-sql-runtime").unwrap(),
            destination,
            run_id: Some(RunId::new("run-general-sql-runtime").unwrap()),
            event_sink: None,
            after_receipt_verified: None,
        },
        &services,
        Some(scheduler),
        RunTelemetryConfig::disabled(),
    ))
    .unwrap()
    .into_committed()
    .unwrap();
    (report, effective_jobs)
}

#[test]
fn destination_planning_facade_previews_duckdb_schema_commit_without_writes() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let database_path = temp.path().join("planned.duckdb");
    let mut destination =
        crate::test_destinations::duckdb(&database_path, TargetName::new("events").unwrap())
            .unwrap();

    let engine_plan = live_plan(&resource, "pkg-plan-preview-duckdb");
    let plan = destination
        .plan_resource_commit(&resource, &engine_plan)
        .unwrap();

    assert_eq!(plan.description.destination_id.as_str(), "duckdb");
    assert_eq!(plan.target.as_str(), "events");
    assert_eq!(
        plan.commit_plan.delivery_guarantee,
        DeliveryGuarantee::EffectivelyOncePerPackage
    );
    assert_eq!(
        plan.commit_plan.idempotency,
        IdempotencySupport::PackageToken
    );
    assert_eq!(plan.synthetic.package_hash.as_str(), "sha256:plan-preview");
    assert_eq!(plan.synthetic.segment_ids.len(), 1);
    assert!(
        plan.commit_plan
            .migrations
            .iter()
            .any(|migration| migration.description.contains("CREATE TABLE"))
    );
    assert!(
        !database_path.exists(),
        "DuckDB plan preview must not create destination data"
    );
}

#[test]
fn destination_planning_preflights_zoned_timestamps_before_any_write() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(
        temp.path(),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.ndjson"
format = "ndjson"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "observed_at", type = "timestamp(us, America/Phoenix)", nullable = true },
] }
"#,
    );
    let engine_plan = live_plan(&resource, "pkg-plan-preview-zoned");

    let duckdb_path = temp.path().join("zoned.duckdb");
    let mut duckdb =
        crate::test_destinations::duckdb(&duckdb_path, TargetName::new("events").unwrap()).unwrap();
    duckdb
        .plan_resource_commit(&resource, &engine_plan)
        .unwrap();
    assert!(!duckdb_path.exists());

    let parquet_root = temp.path().join("parquet-zoned");
    let mut parquet = crate::test_destinations::parquet_filesystem(
        &parquet_root,
        TargetName::new("events").unwrap(),
    )
    .unwrap();
    parquet
        .plan_resource_commit(&resource, &engine_plan)
        .unwrap();
    assert!(!parquet_root.exists());
}

#[test]
fn shared_destination_mapping_rejects_zoned_nanoseconds_before_duckdb_write() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(
        temp.path(),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "events.ndjson"
format = "ndjson"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "observed_at", type = "timestamp(ns, UTC)", nullable = true },
] }
"#,
    );
    let engine_plan = live_plan(&resource, "pkg-plan-preview-zoned-nanos");
    let duckdb_path = temp.path().join("zoned-nanos.duckdb");
    let mut destination =
        crate::test_destinations::duckdb(&duckdb_path, TargetName::new("events").unwrap()).unwrap();

    let error = destination
        .plan_resource_commit(&resource, &engine_plan)
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("observed_at"), "{message}");
    assert!(message.contains("Timestamp(ns, \"UTC\")"), "{message}");
    assert!(message.contains("TIMESTAMPTZ"), "{message}");
    assert!(message.contains("unsupported"), "{message}");
    assert!(!duckdb_path.exists());
}

#[test]
fn destination_planning_rejects_capability_sheet_drift() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let mock = MockDestination::new();
    let mut destination = ResolvedProjectDestination::new(
        Box::new(MockProjectDestinationRuntime::with_sheet_drift(
            mock.clone(),
        )),
        TargetName::new("events").unwrap(),
    );
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = destination.column_identifier_policy().unwrap().unwrap();
    let engine_plan = live_plan_with_exact_policy(&resource, "pkg-plan-sheet-drift", &policy);

    let error = destination
        .plan_resource_commit(&resource, &engine_plan)
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("changed its capability sheet between schema mapping and commit planning"),
        "{error}"
    );
    assert_eq!(mock.write_count(), 0);
}

#[test]
fn repeated_destination_binding_reuses_the_exact_execution_authority() {
    const BUDGET_BYTES: u64 = 64 * 1024 * 1024;

    let temp = tempfile::tempdir().unwrap();
    let (_, execution) = cdf_engine::StandaloneExecutionHost::default_services_with_spill(
        BUDGET_BYTES,
        BUDGET_BYTES,
    )
    .unwrap();
    let mut registry = ProjectDestinationRegistry::new();
    registry
        .register(cdf_dest_duckdb::DuckDbRuntimeDriver)
        .unwrap();
    let target = TargetName::new("events").unwrap();
    let context = ProjectResolutionContext::for_project_run(temp.path(), &target)
        .with_execution_services(&execution);
    let mut destination =
        resolve_project_run_destination(&registry, "duckdb://bounded.duckdb", &context).unwrap();
    execution
        .ensure_blocking_lanes(&destination.runtime_capabilities().blocking_lanes)
        .unwrap();
    let spill = execution.spill();
    let first = spill.snapshot();
    destination.bind_execution_services(execution).unwrap();
    let second = spill.snapshot();

    assert_eq!(first.current_bytes, BUDGET_BYTES);
    assert_eq!(second.current_bytes, first.current_bytes);
    assert_eq!(second.reservation_failures, first.reservation_failures);
}

#[test]
fn failed_destination_rebind_invalidates_cached_execution_authority() {
    const BUDGET_BYTES: u64 = 64 * 1024 * 1024;

    let temp = tempfile::tempdir().unwrap();
    let (_, execution_a) = cdf_engine::StandaloneExecutionHost::default_services_with_spill(
        BUDGET_BYTES,
        BUDGET_BYTES,
    )
    .unwrap();
    let (_, execution_b) = cdf_engine::StandaloneExecutionHost::default_services_with_spill(
        BUDGET_BYTES,
        BUDGET_BYTES,
    )
    .unwrap();
    execution_b
        .ensure_blocking_lanes(&[cdf_runtime::BlockingLaneSpec {
            lane_id: "duckdb.final_binding".to_owned(),
            binding: cdf_runtime::BlockingLaneBinding::Static,
            maximum_concurrency: 1,
            cpu_slot_cost: 1,
            native_internal_parallelism: 1,
            affinity: cdf_runtime::LaneAffinity::Shared,
            interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
        }])
        .unwrap();

    let mut registry = ProjectDestinationRegistry::new();
    registry
        .register(cdf_dest_duckdb::DuckDbRuntimeDriver)
        .unwrap();
    let target = TargetName::new("events").unwrap();
    let context = ProjectResolutionContext::for_project_run(temp.path(), &target)
        .with_execution_services(&execution_a);
    let mut destination =
        resolve_project_run_destination(&registry, "duckdb://retry.duckdb", &context).unwrap();

    let error = destination
        .bind_execution_services(execution_b.clone())
        .unwrap_err();
    assert!(error.message.contains("conflicts"), "{error}");
    destination
        .bind_execution_services(execution_a.clone())
        .unwrap();

    assert_eq!(execution_a.spill().snapshot().current_bytes, BUDGET_BYTES);
    assert_eq!(execution_b.spill().snapshot().current_bytes, 0);
}

#[test]
fn same_host_destination_rebind_reuses_initialized_native_resources() {
    const BUDGET_BYTES: u64 = 64 * 1024 * 1024;

    let temp = tempfile::tempdir().unwrap();
    let (_, execution) = cdf_engine::StandaloneExecutionHost::default_services_with_spill(
        BUDGET_BYTES,
        BUDGET_BYTES,
    )
    .unwrap();
    let mut registry = ProjectDestinationRegistry::new();
    registry
        .register(cdf_dest_duckdb::DuckDbRuntimeDriver)
        .unwrap();
    let target = TargetName::new("events").unwrap();
    let context = ProjectResolutionContext::for_project_run(temp.path(), &target)
        .with_execution_services(&execution);
    let mut destination =
        resolve_project_run_destination(&registry, "duckdb://derived.duckdb", &context).unwrap();
    let derived = execution.with_run_job_ceiling(1).unwrap();
    let before = execution.spill().snapshot();

    destination.bind_execution_services(derived).unwrap();

    let after = execution.spill().snapshot();
    assert_eq!(after.current_bytes, before.current_bytes);
    assert_eq!(after.reservation_failures, before.reservation_failures);
}

#[test]
fn destination_planning_facade_rejects_parquet_merge_without_writes() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_MERGE);
    let parquet_root = temp.path().join("parquet");
    let mut destination = crate::test_destinations::parquet_filesystem(
        &parquet_root,
        TargetName::new("events").unwrap(),
    )
    .unwrap();

    let identifier_policy = destination.column_identifier_policy().unwrap().unwrap();
    assert_eq!(identifier_policy.version, "namecase-v1");
    assert_eq!(identifier_policy.max_length, None);
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.normalization.identifier = identifier_policy;
    let engine_plan = live_plan_with_exact_policy(&resource, "pkg-plan-preview-parquet", &policy);
    let error = destination
        .plan_resource_commit(&resource, &engine_plan)
        .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("Parquet destination does not support Merge"),
        "{error}"
    );
    assert!(
        !parquet_root.exists(),
        "Parquet plan preview must not create destination root"
    );
}

#[test]
fn merge_dedup_live_run_records_deduped_package_replay_identity_and_duplicate_redrive() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_MERGE);
    let source_path = temp.path().join("data/events.ndjson");
    fs::write(
        &source_path,
        "{\"id\":1,\"name\":\"one-first\"}\n\
         {\"id\":2,\"name\":\"two\"}\n\
         {\"id\":1,\"name\":\"one-last\"}\n",
    )
    .unwrap();
    let package_id = "pkg-merge-dedup-live-replay";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut plan = live_plan(&resource, package_id);
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.rows.rules = vec![RowRule::Dedup {
        keys: vec!["id".to_owned()],
        keep: DedupKeep::Last,
    }];
    let validation_program =
        live_plan_with_policy(&resource, package_id, &policy).validation_program;
    plan.rebind_validation_program(validation_program, resource.schema().as_ref())
        .unwrap();
    let mut request = project_run_request(
        &resource,
        package_id,
        &package_root,
        &duckdb_path,
        &state_path,
        "run-merge-dedup-live-replay",
    );
    request.plan = plan;

    let report = futures_executor::block_on(run_project(request)).unwrap();

    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(report.row_count, 2);
    assert_eq!(report.segment_count, 1);
    assert_eq!(report.receipt.disposition, WriteDisposition::Merge);
    assert_eq!(report.receipt.counts.row_write_outcome(), Some(2));
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: true
        }
    );

    let reader = PackageReader::open(&package_dir).unwrap();
    reader.verify().unwrap();
    let identity_segments = package_identity_segments(&reader);
    assert_eq!(identity_segments.len(), 1);
    assert_eq!(identity_segments[0].row_count, 2);
    assert_eq!(
        package_id_name_rows(&reader),
        vec![
            (2, Some("two".to_owned())),
            (1, Some("one-last".to_owned()))
        ]
    );
    assert!(package_identity_file_paths(&reader).contains(DEDUP_SUMMARY_FILE));
    let summary = reader.read_dedup_summary_json().unwrap().unwrap();
    assert_eq!(summary["rule_id"], "row-rule-0000-dedup");
    assert_eq!(summary["keys"], serde_json::json!(["id"]));
    assert_eq!(summary["keep"], "last");
    assert_eq!(summary["input_rows"], 3);
    assert_eq!(summary["output_rows"], 2);
    assert_eq!(summary["duplicate_key_count"], 1);
    assert_eq!(summary["dropped_row_count"], 1);
    let mut dedup_provenance = Vec::new();
    reader
        .for_each_dedup_dropped_provenance(&mut |dropped, kept| {
            dedup_provenance.push((dropped, kept));
            Ok(())
        })
        .unwrap();
    assert_eq!(dedup_provenance, vec![(0, 2)]);
    let replay_inputs = reader.replay_inputs().unwrap();
    assert_eq!(
        replay_inputs.destination_commit.disposition,
        WriteDisposition::Merge
    );
    assert_eq!(replay_inputs.merge_keys, vec!["id".to_owned()]);
    assert_eq!(
        replay_inputs
            .destination_commit
            .segments
            .iter()
            .map(|segment| segment.row_count)
            .sum::<u64>(),
        2
    );

    fs::remove_file(&source_path).unwrap();
    let replay_duckdb_path = temp.path().join(".cdf/replay.duckdb");
    let replay_destination = destination(&replay_duckdb_path);
    let replay_store =
        SqliteCheckpointStore::open(temp.path().join(".cdf/replay-state.db")).unwrap();
    let replay = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: resolved_duckdb_destination(
            &replay_destination,
            replay_inputs.destination_commit.target.clone(),
        ),
        checkpoint_store: &replay_store,
        after_receipt_verified: None,
    })
    .unwrap();

    assert_eq!(replay.checkpoint.delta, report.checkpoint.delta);
    assert_eq!(replay.receipt.disposition, WriteDisposition::Merge);
    assert_eq!(replay.receipt.counts.row_write_outcome(), Some(2));
    assert_eq!(
        replay
            .receipt
            .segment_acks
            .iter()
            .map(|ack| ack.row_count)
            .sum::<u64>(),
        2
    );
    assert!(matches!(
        replay.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: false
        }
    ));
    let replay_snapshot = replay_destination.read_mirror_snapshot_read_only().unwrap();
    assert_eq!(replay_snapshot.loads.len(), 1);
    assert_eq!(replay_snapshot.state.len(), 1);
    assert_eq!(replay_snapshot.state[0].row_count, 2);

    let duplicate_store =
        SqliteCheckpointStore::open(temp.path().join(".cdf/replay-duplicate-state.db")).unwrap();
    let duplicate = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: resolved_duckdb_destination(
            &replay_destination,
            replay_inputs.destination_commit.target,
        ),
        checkpoint_store: &duplicate_store,
        after_receipt_verified: None,
    })
    .unwrap();
    let duplicate_snapshot = replay_destination.read_mirror_snapshot_read_only().unwrap();

    assert_eq!(duplicate_snapshot, replay_snapshot);
    assert_eq!(duplicate.checkpoint.delta, report.checkpoint.delta);
    assert_eq!(duplicate.receipt, replay.receipt);
    assert_eq!(
        duplicate.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: true,
            package_receipt_recorded: false
        }
    );
}

#[test]
fn general_project_run_commits_file_resource_to_parquet_with_ledger_order() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-parquet";
    let package_root = temp.path().join(".cdf/packages");
    let parquet_root = temp.path().join(".cdf/lake");
    let state_path = temp.path().join(".cdf/state.db");

    let report = futures_executor::block_on(run_project(parquet_project_run_request(
        &resource,
        package_id,
        &package_root,
        &parquet_root,
        &state_path,
        "run-general-parquet",
    )))
    .unwrap();

    let kinds = report
        .ledger_snapshot
        .events
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            RunEventKind::RunStarted,
            RunEventKind::PlanRecorded,
            RunEventKind::PackageStarted,
            RunEventKind::PackageSegmentRecorded,
            RunEventKind::PackageFinalized,
            RunEventKind::ValidationDepthTransitionRecorded,
            RunEventKind::CheckpointProposed,
            RunEventKind::DestinationCommitStarted,
            RunEventKind::DestinationSegmentAcknowledged,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::CheckpointCommitted,
            RunEventKind::PackageStatusUpdated,
            RunEventKind::RunSucceeded,
        ]
    );
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(report.row_count, 2);
    assert_eq!(report.receipt.destination.as_str(), "parquet_object_store");
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommit {
            duplicate: false,
            package_receipt_recorded: true
        }
    );
    let destination =
        ParquetDestination::new_filesystem(&parquet_root, test_execution_services()).unwrap();
    assert!(
        destination
            .verify_receipt(&report.receipt)
            .unwrap()
            .verified
    );
}

#[test]
fn general_project_run_commits_file_resource_to_postgres_with_ledger_order() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-general-postgres";
    let package_root = temp.path().join(".cdf/packages");
    let state_path = temp.path().join(".cdf/state.db");
    let target = PostgresTarget::new(Some(&postgres.schema), "events").unwrap();

    let report = futures_executor::block_on(run_project(postgres_project_run_request(
        &resource,
        package_id,
        &package_root,
        &postgres.url,
        target.clone(),
        &state_path,
        "run-general-postgres",
    )))
    .unwrap();

    let kinds = report
        .ledger_snapshot
        .events
        .iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            RunEventKind::RunStarted,
            RunEventKind::PlanRecorded,
            RunEventKind::PackageStarted,
            RunEventKind::PackageSegmentRecorded,
            RunEventKind::PackageFinalized,
            RunEventKind::ValidationDepthTransitionRecorded,
            RunEventKind::CheckpointProposed,
            RunEventKind::DestinationCommitStarted,
            RunEventKind::DestinationSegmentAcknowledged,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::CheckpointCommitted,
            RunEventKind::PackageStatusUpdated,
            RunEventKind::RunSucceeded,
        ]
    );
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(report.row_count, 2);
    assert_eq!(report.receipt.destination.as_str(), "postgres");
    assert_eq!(
        report.receipt_source,
        ProjectReceiptSource::DestinationCommitReceiptOnly {
            package_receipt_recorded: true
        }
    );
    let destination = PostgresDestination::connect(postgres.url.clone()).unwrap();
    assert!(
        destination
            .verify_receipt(&report.receipt)
            .unwrap()
            .verified
    );
    let mut client = postgres.client();
    let rows: i64 = client
        .query_one(
            &format!("SELECT COUNT(*)::bigint FROM {}", postgres.table("events")),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 2);
}

#[test]
fn postgres_destination_policy_truncates_package_and_committed_column_identically() {
    const LONG_SOURCE: &str =
        "this_is_a_very_long_vendor_identifier_column_name_that_exceeds_sixty_three_bytes_total";
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let resource = long_identifier_file_resource(temp.path(), LONG_SOURCE);
    let package_id = "pkg-postgres-destination-normalization";
    let package_root = temp.path().join(".cdf/packages");
    let state_path = temp.path().join(".cdf/state.db");
    let target = PostgresTarget::new(Some(&postgres.schema), "normalized_events").unwrap();
    let destination =
        crate::test_destinations::postgres(postgres.url.clone(), target, None).unwrap();
    let identifier_policy = destination.column_identifier_policy().unwrap().unwrap();
    let expected = cdf_contract::normalize_identifier(LONG_SOURCE, &identifier_policy).unwrap();
    assert_eq!(expected.len(), 63);
    assert_eq!(
        expected,
        cdf_contract::normalize_identifier(LONG_SOURCE, &identifier_policy).unwrap()
    );
    let mut contract = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    contract.normalization.identifier = identifier_policy.clone();

    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&resource),
        plan: live_plan_with_exact_policy(&resource, package_id, &contract),
        package_root,
        state_store_path: state_path,
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-postgres-destination-normalization").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-postgres-destination-normalization").unwrap(),
        destination,
        run_id: Some(RunId::new("run-postgres-destination-normalization").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    let validation: serde_json::Value = serde_json::from_slice(
        &fs::read(report.package_dir.join("plan/validation-program.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(validation["identifier_policy"]["max_length"], 63);
    let output: serde_json::Value =
        serde_json::from_slice(&fs::read(report.package_dir.join("schema/output.json")).unwrap())
            .unwrap();
    assert_eq!(output["fields"][0]["name"], "vendor_id");
    assert_eq!(output["fields"][1]["name"], expected);
    assert_eq!(
        output["fields"][1]["metadata"]["cdf:source_name"],
        LONG_SOURCE
    );

    let mut client = postgres.client();
    let columns = client
        .query(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = 'normalized_events' ORDER BY ordinal_position",
            &[&postgres.schema],
        )
        .unwrap()
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect::<Vec<_>>();
    assert_eq!(&columns[..2], &["vendor_id".to_owned(), expected]);
}

#[test]
fn general_project_run_executes_deterministic_rest_resource_stream() {
    let first_root = tempfile::tempdir().unwrap();
    let second_root = tempfile::tempdir().unwrap();

    let (first, first_transport) = run_rest_project(first_root.path(), "run-general-rest-runtime");
    let (second, second_transport) =
        run_rest_project(second_root.path(), "run-general-rest-runtime");

    assert_eq!(first.row_count, 2);
    assert_eq!(first.segment_count, 1);
    assert_eq!(first.package_status, PackageStatus::Checkpointed);
    assert_eq!(first.checkpoint.status, CheckpointStatus::Committed);
    assert_eq!(first.package_hash, second.package_hash);
    assert_eq!(first_transport.requests().len(), 2);
    assert_eq!(second_transport.requests().len(), 2);
    let SourcePosition::Cursor(cursor) = &first.checkpoint.delta.output_position else {
        panic!("expected REST run to checkpoint a cursor position");
    };
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
}

#[test]
fn rest_source_jobs_matrix_preserves_package_receipt_and_checkpoint_identity() {
    let mut roots = Vec::new();
    let mut runs = Vec::new();
    for (label, jobs) in [
        ("one", Some(1)),
        ("two", Some(2)),
        ("auto", None),
        ("four", Some(4)),
    ] {
        let root = tempfile::tempdir().unwrap();
        let (report, transport, effective_jobs) =
            run_rest_project_with_jobs(root.path(), "run-general-rest-jobs-matrix", jobs);
        assert_eq!(transport.requests().len(), 2, "{label}");
        assert_eq!(effective_jobs, 1, "single REST cursor partition at {label}");
        roots.push(root);
        runs.push(report);
    }

    for report in &runs[1..] {
        assert_eq!(report.package_hash, runs[0].package_hash);
        assert_jobs_invariant_receipt(&report.receipt, &runs[0].receipt);
        assert_eq!(
            report.checkpoint.delta.segments,
            runs[0].checkpoint.delta.segments
        );
        assert_eq!(
            report.checkpoint.delta.output_position,
            runs[0].checkpoint.delta.output_position
        );
    }
}

#[test]
fn general_project_run_executes_rest_with_discovered_snapshot_hash() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = rest_runtime_resource();
    let schema = compiled.schema();
    let schema_hash = SchemaHash::new("sha256:rest-discovered-runtime").unwrap();
    let compiled = compiled.with_schema_source_and_schema(
        SchemaSource::Discovered {
            snapshot: cdf_kernel::SchemaSnapshotReference {
                schema_hash: schema_hash.clone(),
                path: ".cdf/schemas/api.items@sha256:rest-discovered-runtime.json".to_owned(),
                metadata: BTreeMap::from([("probe".to_owned(), "rest-sample-page".to_owned())]),
            },
        },
        schema,
    );
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "id": 1, "updated_at": 10 },
            { "id": 2, "updated_at": 20 }
        ] }"#,
    )]);
    let services = test_execution_services();
    let resource = resolve_rest_resource(
        &compiled,
        transport.clone(),
        Arc::new(StaticSecretProvider::new([(
            "secret://env/API_TOKEN",
            "token-1",
        )])),
        &services,
    );
    let package_id = "pkg-general-rest-discovered-runtime";
    let state_path = temp.path().join(".cdf/state.db");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");

    let plan = live_plan(&resource, package_id)
        .bind_compiled_source(compiled.source_plan())
        .unwrap();
    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&resource),
        plan,
        package_root: temp.path().join(".cdf/packages"),
        state_store_path: state_path,
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-discovered-runtime").unwrap(),
        destination: crate::test_destinations::duckdb(
            duckdb_path,
            TargetName::new("items").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-rest-discovered-runtime").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    assert_eq!(report.row_count, 2);
    assert_eq!(report.checkpoint.delta.schema_hash, schema_hash);
    assert_eq!(report.receipt.schema_hash, schema_hash);
    assert_eq!(transport.requests().len(), 1);
}

#[test]
fn general_project_run_rejects_unsupported_parquet_disposition_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_MERGE);
    let package_id = "pkg-general-parquet-merge-rejected";
    let package_root = temp.path().join(".cdf/packages");
    let parquet_root = temp.path().join(".cdf/lake");
    let state_path = temp.path().join(".cdf/state.db");

    let error = futures_executor::block_on(run_project(parquet_project_run_request(
        &resource,
        package_id,
        &package_root,
        &parquet_root,
        &state_path,
        "run-general-parquet-merge-rejected",
    )))
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("parquet_object_store destination")
    );
    assert!(!package_root.join(package_id).exists());
    assert!(!parquet_root.exists());
    assert!(!state_path.exists());
}

#[test]
fn general_project_run_rejects_unallowed_lossy_postgres_schema_before_writes() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let resource = BackfillMockResource::postgres_unallowed_lossy_schema();
    let package_id = "pkg-general-postgres-unsupported-schema";
    let package_root = temp.path().join(".cdf/packages");
    let state_path = temp.path().join(".cdf/state.db");
    let target = PostgresTarget::new(Some(&postgres.schema), "events_unsupported").unwrap();

    let error = futures_executor::block_on(run_project(postgres_project_run_request(
        &resource,
        package_id,
        &package_root,
        &postgres.url,
        target.clone(),
        &state_path,
        "run-general-postgres-unsupported-schema",
    )))
    .unwrap_err();

    let message = error.to_string();
    assert!(message.contains("seen_at"), "{error}");
    assert!(message.contains("Duration(ns)"), "{error}");
    assert!(message.contains("allow_lossy_mapping"), "{error}");
    assert!(message.contains("postgres"), "{error}");
    assert!(!package_root.join(package_id).exists());
    assert!(!state_path.exists());
    let mut client = postgres.client();
    let target_exists: Option<String> = client
        .query_one(
            "SELECT to_regclass($1)::text",
            &[&format!("{}.events_unsupported", postgres.schema)],
        )
        .unwrap()
        .get(0);
    let loads_exists: Option<String> = client
        .query_one(
            "SELECT to_regclass($1)::text",
            &[&format!("{}._cdf_loads", postgres.schema)],
        )
        .unwrap()
        .get(0);
    assert!(target_exists.is_none());
    assert!(loads_exists.is_none());
    assert_eq!(resource.open_count.load(Ordering::SeqCst), 0);
}

#[test]
fn general_project_run_rejects_rest_missing_secret_value_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = rest_runtime_resource();
    let transport = RecordingTransport::new([json_response(r#"{ "items": [] }"#)]);
    let services = test_execution_services();
    let resource = resolve_rest_resource(
        &compiled,
        transport.clone(),
        Arc::new(StaticSecretProvider::new(std::iter::empty::<(&str, &str)>())),
        &services,
    );
    let package_id = "pkg-general-rest-missing-secret-value";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let error = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&resource),
        plan: live_plan(&resource, package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-missing-secret-value").unwrap(),
        destination: crate::test_destinations::duckdb(
            duckdb_path.clone(),
            TargetName::new("items").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-rest-missing-secret-value").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap_err();

    assert!(error.to_string().contains("missing test secret"));
    assert_eq!(transport.requests().len(), 0);
    assert!(!package_root.join(package_id).exists());
    assert!(!duckdb_path.exists());
    assert!(!state_path.exists());
}

#[test]
fn general_project_run_rejects_rest_without_cursor_before_writes() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = rest_resource();
    let transport = RecordingTransport::new([json_response(r#"[{ "id": 1 }]"#)]);
    let services = test_execution_services();
    let resource = resolve_rest_resource(
        &compiled,
        transport.clone(),
        Arc::new(StaticSecretProvider::new(std::iter::empty::<(&str, &str)>())),
        &services,
    );
    let package_id = "pkg-general-rest-no-cursor";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let error = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&resource),
        plan: live_plan(&resource, package_id),
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-no-cursor").unwrap(),
        destination: crate::test_destinations::duckdb(
            duckdb_path.clone(),
            TargetName::new("items").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-rest-no-cursor").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap_err();

    assert!(error.to_string().contains("ordered cursor"));
    assert_eq!(transport.requests().len(), 0);
    assert!(!package_root.join(package_id).exists());
    assert!(!duckdb_path.exists());
    assert!(!state_path.exists());
}

#[test]
fn general_project_run_rejects_postgres_empty_secret_before_destination() {
    let temp = tempfile::tempdir().unwrap();
    let compiled = postgres_runtime_resource("public.orders");
    let services = test_execution_services();
    let resource = resolve_postgres_resource(&compiled, "", &services);
    let package_id = "pkg-general-postgres-empty-secret";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let error = futures_executor::block_on(run_project_fixture(
        ProjectRunRequest {
            resource: ProjectRunSource::new(&resource),
            plan: live_plan(&resource, package_id),
            package_root: package_root.clone(),
            state_store_path: state_path.clone(),
            state_store_path_ownership: crate::StateStorePathOwnership::Configured,
            pipeline_id: PipelineId::new("pipeline-live").unwrap(),
            package_id: package_id.to_owned(),
            checkpoint_id: CheckpointId::new("checkpoint-general-postgres-empty-secret").unwrap(),
            destination: crate::test_destinations::duckdb(
                duckdb_path.clone(),
                TargetName::new("orders").unwrap(),
            )
            .unwrap(),
            run_id: Some(RunId::new("run-general-postgres-empty-secret").unwrap()),
            event_sink: None,
            after_receipt_verified: None,
        },
        &services,
        RunTelemetryConfig::disabled(),
    ))
    .unwrap_err();

    assert!(error.to_string().contains("empty value"), "{error}");
    assert!(
        !package_root.join(package_id).exists(),
        "invalid source runtime dependencies fail before package mutation"
    );
    assert!(!duckdb_path.exists());
    assert!(
        !state_path.exists(),
        "invalid source runtime dependencies fail before state mutation"
    );
}

#[test]
fn general_project_run_executes_postgres_table_resource_stream() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("source_orders");
    let mut client = postgres.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"id\" BIGINT NOT NULL,
                \"updated_at\" BIGINT NOT NULL
            );
            INSERT INTO {} (\"id\", \"updated_at\") VALUES (1, 10), (2, 20)",
            table, table
        ))
        .unwrap();

    let compiled = postgres_runtime_resource(&table);
    let mut roots = Vec::new();
    let mut runs = Vec::new();
    for (label, jobs) in [
        ("one", Some(1)),
        ("two", Some(2)),
        ("auto", None),
        ("four", Some(4)),
    ] {
        let root = tempfile::tempdir().unwrap();
        let (report, effective_jobs) =
            run_postgres_project_with_jobs(&compiled, &postgres.url, root.path(), jobs);
        assert_eq!(
            effective_jobs, 1,
            "single Postgres table partition at {label}"
        );
        assert_eq!(report.row_count, 2);
        assert_eq!(report.segment_count, 1);
        assert_eq!(report.package_status, PackageStatus::Checkpointed);
        assert_eq!(report.checkpoint.status, CheckpointStatus::Committed);
        roots.push(root);
        runs.push(report);
    }
    for report in &runs[1..] {
        assert_eq!(report.package_hash, runs[0].package_hash);
        assert_jobs_invariant_receipt(&report.receipt, &runs[0].receipt);
        assert_eq!(
            report.checkpoint.delta.segments,
            runs[0].checkpoint.delta.segments
        );
        assert_eq!(
            report.checkpoint.delta.output_position,
            runs[0].checkpoint.delta.output_position
        );
    }
    let SourcePosition::Cursor(cursor) = &runs[0].checkpoint.delta.output_position else {
        panic!("expected SQL run to checkpoint a cursor position");
    };
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
}

#[test]
fn failed_destination_bind_stage_can_restore_the_original_execution_authority() {
    const BUDGET_BYTES: u64 = 64 * 1024 * 1024;

    let temp = tempfile::tempdir().unwrap();
    let (_, execution_a) = cdf_engine::StandaloneExecutionHost::default_services_with_spill(
        BUDGET_BYTES,
        BUDGET_BYTES,
    )
    .unwrap();
    let (_, execution_b) = cdf_engine::StandaloneExecutionHost::default_services_with_spill(
        BUDGET_BYTES,
        BUDGET_BYTES,
    )
    .unwrap();
    let spill_b = execution_b.spill();
    let competing_reservation = spill_b
        .try_reserve(BUDGET_BYTES)
        .unwrap()
        .expect("the competing reservation fits the empty B budget");

    let mut registry = ProjectDestinationRegistry::new();
    registry
        .register(cdf_dest_duckdb::DuckDbRuntimeDriver)
        .unwrap();
    let target = TargetName::new("events").unwrap();
    let context = ProjectResolutionContext::for_project_run(temp.path(), &target)
        .with_execution_services(&execution_a);
    let mut destination =
        resolve_project_run_destination(&registry, "duckdb://retry-stage.duckdb", &context)
            .unwrap();

    let error = destination
        .bind_execution_services(execution_b)
        .unwrap_err();
    assert!(
        error.message.contains("spill budget is already committed"),
        "{error}"
    );
    destination
        .bind_execution_services(execution_a.clone())
        .unwrap();

    assert_eq!(execution_a.spill().snapshot().current_bytes, BUDGET_BYTES);
    drop(competing_reservation);
    assert_eq!(spill_b.snapshot().current_bytes, 0);
}

#[test]
fn project_run_records_non_mirror_outcome_for_unsupported_quarantine_sheet() {
    let temp = tempfile::tempdir().unwrap();
    let resource = simple_file_resource(temp.path(), SIMPLE_FILE_RESOURCE_APPEND);
    let package_id = "pkg-quarantine-duckdb-unsupported";
    let package_root = temp.path().join(".cdf/packages");
    let package_dir = package_root.join(package_id);
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");
    let mut plan = live_plan(&resource, package_id);
    let mut policy = ContractPolicy::for_trust(resource.descriptor().trust_level.clone());
    policy.rows.rules = vec![RowRule::Range {
        column: "id".to_owned(),
        min: None,
        max: Some("1".to_owned()),
    }];
    let validation_program =
        live_plan_with_policy(&resource, package_id, &policy).validation_program;
    plan.rebind_validation_program(validation_program, resource.schema().as_ref())
        .unwrap();
    let source = compiled_test_source_plan(&resource);
    plan = plan.bind_compiled_source(&source).unwrap();
    let bound = BoundTestResource {
        inner: &resource,
        compiled_source_plan_hash: source.compiled_source_plan_hash().unwrap(),
        replay_retention: None,
    };

    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&bound),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path,
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-quarantine-duckdb-unsupported").unwrap(),
        destination: crate::test_destinations::duckdb(
            duckdb_path,
            TargetName::new("events").unwrap(),
        )
        .unwrap(),
        run_id: None,
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    assert_eq!(report.row_count, 1);
    assert_eq!(report.segment_count, 1);
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    let reader = PackageReader::open(&package_dir).unwrap();
    let mut quarantine_record_count = 0_u64;
    reader
        .for_each_quarantine_record(&mut |_| {
            quarantine_record_count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(quarantine_record_count, 1);
    assert!(package_identity_file_paths(&reader).contains("destination/quarantine-mirror.json"));
    let mirror_outcome: serde_json::Value = serde_json::from_slice(
        &fs::read(package_dir.join("destination/quarantine-mirror.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(mirror_outcome["destination_id"], "duckdb");
    assert_eq!(mirror_outcome["quarantine_table_support"], "unsupported");
    assert_eq!(mirror_outcome["outcome"], "not_mirrored");
    assert_eq!(mirror_outcome["version"], 1);
    assert_eq!(mirror_outcome["quarantine_directory"], "quarantine/");
    assert_eq!(mirror_outcome["quarantine_part_count"], 1);
    assert_eq!(mirror_outcome["schema_observations_present"], false);
    assert!(mirror_outcome.get("quarantine_artifacts").is_none());
}

#[test]
fn general_project_run_window_closes_inexact_numeric_rest_cursor() {
    let temp = tempfile::tempdir().unwrap();
    let document = cdf_declarative::parse_toml(
        &REST_RUNTIME_RESOURCE
            .replace(r#"ordering = "exact""#, r#"ordering = "best_effort""#)
            .replace(r#"lag = "0ms""#, r#"lag = "5ms""#),
    )
    .unwrap();
    let compiled = cdf_declarative::compile_document(&rest_compile_registry(), &document)
        .unwrap()
        .remove(0);
    let transport = RecordingTransport::new([json_response(
        r#"{ "items": [
            { "id": 1, "updated_at": 10 },
            { "id": 2, "updated_at": 20 }
        ] }"#,
    )]);
    let services = test_execution_services();
    let resource = resolve_rest_resource(
        &compiled,
        transport.clone(),
        Arc::new(StaticSecretProvider::new([(
            "secret://env/API_TOKEN",
            "token-1",
        )])),
        &services,
    );
    let package_id = "pkg-general-rest-window-close-numeric";
    let package_root = temp.path().join(".cdf/packages");
    let duckdb_path = temp.path().join(".cdf/dev.duckdb");
    let state_path = temp.path().join(".cdf/state.db");

    let plan = live_plan(&resource, package_id)
        .bind_compiled_source(compiled.source_plan())
        .unwrap();
    let report = futures_executor::block_on(run_project(ProjectRunRequest {
        resource: ProjectRunSource::new(&resource),
        plan,
        package_root: package_root.clone(),
        state_store_path: state_path.clone(),
        state_store_path_ownership: crate::StateStorePathOwnership::Configured,
        pipeline_id: PipelineId::new("pipeline-live").unwrap(),
        package_id: package_id.to_owned(),
        checkpoint_id: CheckpointId::new("checkpoint-general-rest-window-close-numeric").unwrap(),
        destination: crate::test_destinations::duckdb(
            duckdb_path.clone(),
            TargetName::new("items").unwrap(),
        )
        .unwrap(),
        run_id: Some(RunId::new("run-general-rest-window-close-numeric").unwrap()),
        event_sink: None,
        after_receipt_verified: None,
    }))
    .unwrap();

    assert_eq!(transport.requests().len(), 1);
    assert_eq!(report.package_status, PackageStatus::Checkpointed);
    let SourcePosition::Cursor(cursor) = &report.checkpoint.delta.output_position else {
        panic!("expected REST run to checkpoint a cursor position");
    };
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(15));
}
