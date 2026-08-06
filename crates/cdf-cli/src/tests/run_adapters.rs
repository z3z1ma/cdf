use super::*;

#[test]
fn run_missing_resource_still_fails_before_writes() {
    let project = TestProject::new();
    let result = run(["cdf", "--json", "--project", project.root_str(), "run"]);

    assert_eq!(result.exit_code, 2, "stderr: {}", result.stderr);
    assert_no_run_writes(&project);
    let json = assert_json_error_code(&result, "CDF-RUN-ARGUMENT");
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("run requires RESOURCE")
    );
}

#[test]
fn run_rest_resource_fails_before_package_or_destination_writes() {
    let project = TestProject::new();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        Some("secret://env/CDF_CLI_TOKEN"),
        None,
    );

    let result = run_valid_run_resource(&project, "api.items");

    assert_eq!(
        result.exit_code, 4,
        "stdout: {}\nstderr: {}",
        result.stdout, result.stderr
    );
    assert_no_run_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], false);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("secret://env/CDF_CLI_TOKEN")
    );
}

#[test]
fn run_rest_resource_uses_http_transport_and_commits_checkpoint() {
    let project = TestProject::new();
    fs::write(project.root.join("rest-token"), "rest-token-secret\n").unwrap();
    let base_url = serve_json_once(
        r#"{ "items": [
            { "id": 1, "updated_at": 10 },
            { "id": 2, "updated_at": 20 }
        ] }"#,
    );
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );

    let result = run_valid_run_resource(&project, "api.items");

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "rest-token-secret");
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "api.items");
    assert_eq!(report["destination"]["kind"], "duckdb");
    assert_eq!(report["target"], "api.items");
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(report["row_count"], 2);
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");

    let package_dir = run_package_dir(&project, &result);
    let admission: CompiledStreamAdmissionEvidence = serde_json::from_slice(
        &fs::read(package_dir.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    let validation: cdf_contract::ValidationProgram = serde_json::from_slice(
        &fs::read(package_dir.join("plan/validation-program.json")).unwrap(),
    )
    .unwrap();
    assert!(validation.schema_coercion.is_none());
    assert_eq!(admission.observations.len(), 1);
    assert!(
        admission.observations[0]
            .coercion_plan
            .fields
            .iter()
            .all(|field| field.decision == cdf_contract::FieldCoercionDecision::Preserved)
    );

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM api.items", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("api.items").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed REST run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        report["checkpoint_id"].as_str().unwrap()
    );
}

#[test]
fn run_rest_runtime_defaults_cannot_authorize_parse_coercion() {
    let parse_project = TestProject::new();
    fs::write(
        parse_project.root.join("rest-token"),
        "parse-token-secret\n",
    )
    .unwrap();
    let (parse_url, _requests) = serve_json_sequence([
        r#"{ "items": [
            { "id": 1, "updated_at": 10 },
            { "id": 2, "updated_at": 20 }
        ] }"#,
        r#"{ "items": [
            { "id": 1, "updated_at": 10 },
            { "id": 2, "updated_at": "20" }
        ] }"#,
    ]);
    write_rest_project(
        &parse_project,
        "duckdb://.cdf/dev.duckdb",
        &parse_url,
        "secret://file/rest-token",
    );
    let compile = compile_resource(&parse_project, "api.items");
    assert_eq!(compile.exit_code, 0, "{}{}", compile.stdout, compile.stderr);

    let parse = run_valid_run_resource(&parse_project, "api.items");

    assert_eq!(parse.exit_code, 0, "{}", parse.stderr);
    assert_secret_absent(&parse, "parse-token-secret");
    let parse_report = stderr_or_stdout_json(&parse.stdout);
    assert_eq!(parse_report["result"]["row_count"], 1);
    let parse_package = run_package_dir(&parse_project, &parse);
    let quarantine_summary: serde_json::Value = serde_json::from_slice(
        &fs::read(parse_package.join("stats/quarantine-summary.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantine_summary["quarantined_rows"], 1);
    assert_eq!(
        quarantine_summary["artifacts"],
        serde_json::json!(["quarantine/part-000001.parquet"])
    );
    let parse_admission: CompiledStreamAdmissionEvidence = serde_json::from_slice(
        &fs::read(parse_package.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    assert!(parse_admission.observations.iter().all(|observation| {
        observation.coercion_plan.fields.iter().all(|field| {
            !matches!(
                field.decision,
                cdf_contract::FieldCoercionDecision::CoercedByPolicy
                    | cdf_contract::FieldCoercionDecision::LossyAllowed
            )
        })
    }));
}

#[test]
fn duckdb_destination_policy_normalizes_plan_preview_package_and_commit() {
    const LONG_SOURCE: &str =
        "this_is_a_very_long_vendor_identifier_column_name_that_exceeds_sixty_three_bytes_total";
    let project = TestProject::new();
    fs::write(
        project.root.join("data/events.ndjson"),
        format!("{{\"VendorID\":1,\"{LONG_SOURCE}\":10}}\n"),
    )
    .unwrap();

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(plan.exit_code, 0, "{}", plan.stderr);
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    assert_eq!(
        plan_json["result"]["normalization"]["version"],
        "namecase-v1"
    );
    assert_eq!(
        plan_json["result"]["normalization"]["max_length"],
        serde_json::Value::Null
    );
    assert_eq!(
        plan_json["result"]["normalization"]["allowed_pattern"],
        "^[a-z_][a-z0-9_]*$"
    );
    assert_eq!(
        plan_json["result"]["resource_schema"]["fields"][0]["name"],
        "vendor_id"
    );
    assert_eq!(
        plan_json["result"]["resource_schema"]["fields"][1]["name"],
        LONG_SOURCE
    );

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    let preview_json = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(
        preview_json["result"]["fields"],
        serde_json::json!(["vendor_id", LONG_SOURCE, "_cdf_variant"])
    );
    assert_eq!(
        preview_json["result"]["normalization"],
        plan_json["result"]["normalization"]
    );

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "{}", run_result.stderr);
    let package = run_package_dir(&project, &run_result);
    let validation: serde_json::Value =
        serde_json::from_slice(&fs::read(package.join("plan/validation-program.json")).unwrap())
            .unwrap();
    assert_eq!(
        validation["identifier_policy"],
        plan_json["result"]["normalization"]
    );
    let output: serde_json::Value =
        serde_json::from_slice(&fs::read(package.join("schema/output.json")).unwrap()).unwrap();
    assert_eq!(output["fields"][0]["name"], "vendor_id");
    assert_eq!(
        output["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );
    assert_eq!(output["fields"][1]["name"], LONG_SOURCE);

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let mut statement = conn.prepare("PRAGMA table_info('events')").unwrap();
    let columns = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, bool>(3)?,
            ))
        })
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        columns,
        vec![
            ("vendor_id".to_owned(), "BIGINT".to_owned(), false),
            (LONG_SOURCE.to_owned(), "BIGINT".to_owned(), false),
            ("_cdf_variant".to_owned(), "VARCHAR".to_owned(), false),
            ("_cdf_row_key".to_owned(), "UBIGINT".to_owned(), true),
        ]
    );
}

#[test]
fn destination_normalization_collision_fails_before_writes() {
    let project = TestProject::new();
    fs::write(
        project.root.join("data/events.ndjson"),
        "{\"VendorID\":1,\"vendor_id\":2}\n",
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);

    assert_ne!(result.exit_code, 0);
    let output = format!("{}{}", result.stdout, result.stderr);
    assert!(output.contains("VendorID"), "{output}");
    assert!(output.contains("vendor_id"), "{output}");
    assert!(output.contains("explicit rename"), "{output}");
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
}

#[test]
fn run_postgres_resource_missing_secret_fails_before_package_or_destination_writes() {
    let project = TestProject::new();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://env/CDF_CLI_POSTGRES"),
    );
    let result = run_valid_run_resource(&project, "warehouse.orders");

    assert_eq!(
        result.exit_code, 4,
        "stdout: {}\nstderr: {}",
        result.stdout, result.stderr
    );
    assert_no_run_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], false);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("secret://env/CDF_CLI_POSTGRES")
    );
}

#[test]
fn run_postgres_resource_resolves_secret_without_leaking_on_connection_failure() {
    let project = TestProject::new();
    fs::write(
        project.root.join("postgres-dsn"),
        "postgres://user:postgres-secret@localhost/db\n",
    )
    .unwrap();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/postgres-dsn"),
    );

    let result = run_valid_run_resource(&project, "warehouse.orders");

    assert_ne!(result.exit_code, 0);
    assert_no_run_writes(&project);
    assert_secret_absent(&result, "postgres-secret");
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(json["error"]["message"].is_string());
}

#[test]
fn run_postgres_resource_with_ordered_cursor_commits_checkpoint() {
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

    let project = TestProject::new();
    let source_dsn = postgres.url.replacen(
        "postgresql://cdf@",
        "postgresql://cdf:source-postgres-secret@",
        1,
    );
    fs::write(project.root.join("postgres-dsn"), format!("{source_dsn}\n")).unwrap();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/postgres-dsn"),
    );
    fs::write(
        project.root.join("cdf/warehouse/orders.cdf.sql"),
        postgres_resource_sql(&table, true),
    )
    .unwrap();

    let result = run_valid_run_resource(&project, "warehouse.orders");

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &source_dsn);
    assert_secret_absent(&result, "source-postgres-secret");
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "warehouse.orders");
    assert_eq!(report["target"], "warehouse.orders");
    assert_eq!(report["destination"]["kind"], "duckdb");
    assert_eq!(report["destination"]["destination_id"], "duckdb");
    assert_eq!(report["row_count"], 2);
    assert_eq!(report["segment_count"], 1);
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(report["receipt"]["segment_ack_count"], 1);
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["checkpoint"]["committed"], true);
    assert_eq!(report["checkpoint"]["is_head"], true);
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");
    assert_eq!(
        report["ledger_events"]["events"]
            .as_array()
            .unwrap()
            .last()
            .unwrap()["kind"],
        "run_succeeded"
    );

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let mut statement = conn
        .prepare("SELECT id, updated_at FROM warehouse.orders ORDER BY id")
        .unwrap();
    let rows = statement
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, vec![(1, 10), (2, 20)]);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("warehouse.orders").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed SQL run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        report["checkpoint_id"].as_str().unwrap()
    );
    let SourcePosition::Cursor(cursor) = &head.delta.output_position else {
        panic!("expected SQL run checkpoint head to use a cursor position");
    };
    assert_eq!(cursor.field, "updated_at");
    assert_eq!(cursor.value, CursorValue::I64(20));
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
    );
}

#[test]
fn run_parquet_destination_writes_filesystem_root() {
    let project = TestProject::new();
    write_project_destination(&project, "parquet://.cdf/parquet");

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["destination"]["kind"], "parquet");
    assert_eq!(
        report["destination"]["destination_id"],
        "parquet_object_store"
    );
    assert!(
        report["destination"]["root"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/parquet")
    );
    assert_eq!(report["target"], "events");
    assert_eq!(report["receipt"]["destination_id"], "parquet_object_store");
    assert_eq!(report["receipt"]["target"], "events");
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(report["receipt_source"]["kind"], "destination_commit");
    assert_eq!(report["receipt_source"]["duplicate"], false);
    assert_eq!(report["receipt_source"]["no_op"], false);
    assert_eq!(report["package_status"], "checkpointed");
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");
    assert!(project.root.join(".cdf/parquet").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed Parquet run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        report["checkpoint_id"].as_str().unwrap()
    );
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
    );
}

#[test]
fn run_parquet_malformed_uri_fails_before_writes() {
    for uri in ["parquet://", "parquet://s3://bucket"] {
        let project = TestProject::new();
        write_project_destination(&project, uri);

        let result = run_valid_run_args(&project);

        assert_eq!(result.exit_code, 3, "uri {uri}: {}", result.stderr);
        assert_no_run_writes(&project);
        let json = stderr_or_stdout_json(&result.stderr);
        assert_eq!(json["error"]["kind"], "contract");
        assert_eq!(json["error"]["not_supported"], false);
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("malformed or non-local")
        );
    }
}

#[test]
fn run_postgres_destination_resolves_secret_and_commits_checkpoint() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        format!(
            "{}?options=-csearch_path%3D{}\n",
            postgres.url, postgres.schema
        ),
    )
    .unwrap();
    write_project_destination(&project, "postgres://secret://file/destination-dsn");
    let target = "events";

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &postgres.url);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["destination"]["kind"], "postgres");
    assert_eq!(report["destination"]["destination_id"], "postgres");
    assert_eq!(report["destination"]["target"], target);
    assert_eq!(report["target"], target);
    assert_eq!(report["receipt"]["destination_id"], "postgres");
    assert_eq!(report["receipt"]["target"], target);
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(
        report["receipt_source"]["kind"],
        "destination_commit_receipt_only"
    );
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["package_status"], "checkpointed");
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed Postgres run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        report["checkpoint_id"].as_str().unwrap()
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
fn run_local_parquet_discover_autopins_and_commits_pinned_schema() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    remove_resource_format(&project, "parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    let snapshot_path = single_schema_snapshot_path(&project);
    let snapshot = read_snapshot_json(&project, &snapshot_path);
    let snapshot_hash = snapshot["schema_hash"].as_str().unwrap();
    assert_eq!(report["schema_hash"], snapshot_hash);
    assert_eq!(report["schema_snapshot"]["schema_hash"], snapshot_hash);
    assert_eq!(snapshot["schema"]["fields"][0]["name"], "vendor_id");
    assert_eq!(
        snapshot["schema"]["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed Parquet discover run head");
    assert_eq!(head.delta.schema_hash.as_str(), report["schema_hash"]);
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        report["checkpoint_id"].as_str().unwrap()
    );

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows = conn
        .prepare("SELECT vendor_id FROM local.events ORDER BY vendor_id")
        .unwrap()
        .query_map([], |row| row.get::<_, i32>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, vec![1, 2]);
}

#[test]
fn pinned_multi_file_parquet_preview_attests_unopened_observed_partitions() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/a.parquet"));
    write_vendor_parquet(&project.root.join("data/b.parquet"));

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(plan.exit_code, 0, "{}", plan.stderr);
    let before_preview = project_tree_snapshot(&project.root);

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
        "--limit",
        "1",
    ]);

    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    let report = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(report["result"]["planned_partition_count"], 2);
    assert_eq!(report["result"]["payload_opened_partition_count"], 1);
    assert_eq!(report["result"]["payload_uninspected_partition_count"], 1);
    assert_eq!(report["result"]["attested_partition_count"], 1);
    assert_eq!(report["result"]["row_count"], 1);
    assert_project_tree_unchanged(&project.root, &before_preview);
}

#[test]
fn pinned_multi_file_parquet_keeps_fixed_schema_and_admits_new_physical_schemas_in_stream() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/a.parquet"));

    let baseline_plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(baseline_plan.exit_code, 0, "{}", baseline_plan.stderr);
    let baseline_report = stderr_or_stdout_json(&baseline_plan.stdout);
    let baseline_hash = baseline_report["result"]["schema_snapshot"]["schema_hash"]
        .as_str()
        .unwrap()
        .to_owned();
    let snapshot_path = baseline_report["result"]["schema_snapshot"]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let lock_before = fs::read(project.root.join("cdf.lock")).unwrap();
    let snapshot_before = fs::read(project.root.join(&snapshot_path)).unwrap();
    let snapshot = read_snapshot_json(&project, &snapshot_path);
    let manifest_path = snapshot["metadata"]["cdf:discovery_manifest_path"]
        .as_str()
        .unwrap();
    let discovery_manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(project.root.join(manifest_path)).unwrap()).unwrap();
    assert_eq!(
        discovery_manifest["candidates"].as_array().unwrap().len(),
        1
    );

    write_vendor_score_parquet(&project.root.join("data/b.parquet"));
    write_empty_vendor_parquet(&project.root.join("data/c.parquet"));

    let pinned_plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(pinned_plan.exit_code, 0, "{}", pinned_plan.stderr);
    let pinned_report = stderr_or_stdout_json(&pinned_plan.stdout);
    let schema = &pinned_report["result"]["resource_schema"];
    assert_eq!(schema["schema_hash"], baseline_hash);
    assert!(schema.get("baseline_schema_hash").is_none());
    assert!(schema.get("effective_schema_hash").is_none());
    assert!(schema.get("effective_arrow_schema_hash").is_none());
    assert!(
        schema["fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| { field["name"] == "vendor_id" && field["data_type"] == "Int32" })
    );
    assert!(
        schema["fields"]
            .as_array()
            .unwrap()
            .iter()
            .all(|field| field["name"] != "score")
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
    assert_eq!(
        fs::read(project.root.join(&snapshot_path)).unwrap(),
        snapshot_before
    );
    assert_eq!(single_schema_snapshot_path(&project), snapshot_path);

    let before_preview = project_tree_snapshot(&project.root);
    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    let preview_report = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_report["result"]["planned_partition_count"], 3);
    assert_eq!(
        preview_report["result"]["payload_opened_partition_count"],
        3
    );
    assert_eq!(preview_report["result"]["attested_partition_count"], 0);
    assert_eq!(preview_report["result"]["inspected_partition_count"], 3);
    assert_eq!(
        preview_report["result"]["inspected_batch_count"], 3,
        "the empty Parquet partition carries its physical schema in a zero-row batch"
    );
    assert_eq!(preview_report["result"]["row_count"], 4);
    assert_eq!(preview_report["result"]["terminal_quarantine_count"], 0);
    assert!(
        preview_report["result"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "_cdf_variant")
    );
    assert_project_tree_unchanged(&project.root, &before_preview);

    let limited_preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
        "--limit",
        "1",
    ]);
    assert_eq!(limited_preview.exit_code, 0, "{}", limited_preview.stderr);
    let limited_report = stderr_or_stdout_json(&limited_preview.stdout);
    assert_eq!(limited_report["result"]["planned_partition_count"], 3);
    assert_eq!(limited_report["result"]["selected_partition_count"], 3);
    assert_eq!(
        limited_report["result"]["payload_opened_partition_count"],
        1
    );
    assert_eq!(limited_report["result"]["inspected_partition_count"], 1);
    assert_eq!(
        limited_report["result"]["payload_uninspected_partition_count"],
        2
    );
    assert_eq!(limited_report["result"]["row_count"], 1);
    assert_eq!(limited_report["result"]["limits"]["max_rows"], 1);
    assert_eq!(limited_report["result"]["truncated"], true);
    assert_eq!(project_tree_snapshot(&project.root), before_preview);

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 0, "{}", result.stderr);
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
    assert_eq!(
        fs::read(project.root.join(&snapshot_path)).unwrap(),
        snapshot_before
    );
    let package_dir = run_package_dir(&project, &result);
    assert!(
        !package_dir
            .join("schema/effective-schema-evidence.json")
            .exists()
    );
    let stream_admission: serde_json::Value = serde_json::from_slice(
        &fs::read(package_dir.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    let observations = stream_admission["observations"].as_array().unwrap();
    assert_eq!(observations.len(), 3);
    assert!(observations.iter().any(|observation| {
        observation["coercion_plan"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field["decision"] == "extra")
    }));

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed multi-file Parquet head");
    let SourcePosition::FileManifest(runtime_manifest) = &head.delta.output_position else {
        panic!("multi-file run must commit exact FileManifest identity");
    };
    // Schema evidence and processed-file checkpoint advancement cover the same
    // three-file manifest, including the zero-row file.
    assert_eq!(runtime_manifest.files.len(), 3);
    assert!(
        runtime_manifest
            .files
            .iter()
            .all(|file| file.sha256.is_some())
    );
    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM local.events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 4);
    let residual_rows: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM local.events WHERE _cdf_variant IS NOT NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(residual_rows, 2);

    drop(conn);
    fs::remove_file(project.root.join("data/a.parquet")).unwrap();
    fs::remove_file(project.root.join("data/b.parquet")).unwrap();
    fs::remove_file(project.root.join("data/c.parquet")).unwrap();
    fs::remove_file(project.root.join(".cdf/state.db")).unwrap();
    fs::remove_file(project.root.join(".cdf/dev.duckdb")).unwrap();
    let replay = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "replay",
        "package",
        package_dir.to_str().unwrap(),
    ]);
    assert_eq!(replay.exit_code, 0, "{}", replay.stderr);
    fs::write(
        package_dir.join("schema/stream-admission-evidence.json"),
        b"{\"tampered\":true}",
    )
    .unwrap();
    let verify = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "package",
        "verify",
        package_dir.to_str().unwrap(),
    ]);
    assert_ne!(verify.exit_code, 0);
}

#[test]
fn governed_evolve_quarantines_incompatible_file_with_exact_arrow_field_evidence() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    let path = project.root.join("data/a.parquet");
    write_vendor_parquet(&path);
    let baseline = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(baseline.exit_code, 0, "{}", baseline.stderr);

    write_string_vendor_parquet(&path);
    let result = run_valid_run_args(&project);
    assert_eq!(result.exit_code, 0, "{}", result.stderr);
    let report = stderr_or_stdout_json(&result.stdout);
    let rendered = &report["result"]["terminal_schema_quarantines"][0];
    assert_eq!(rendered["observation_id"], "a.parquet");
    assert_eq!(rendered["rule_id"], "schema-observation:incompatible");
    assert_eq!(rendered["fields"][0]["scope"]["path"][0], "VendorID");
    assert_eq!(
        rendered["fields"][0]["observed_field"]["data_type"]["kind"],
        "utf8"
    );
    assert_eq!(
        rendered["fields"][0]["effective_field"]["data_type"]["kind"],
        "int"
    );
    assert!(rendered["remediation"].as_str().unwrap().contains("schema"));
    let package = run_package_dir(&project, &result);
    let quarantine: serde_json::Value = serde_json::from_slice(
        &fs::read(package.join("quarantine/schema-observations.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(quarantine[0]["policy"], "evolve");
    assert_eq!(quarantine[0]["rule_id"], "schema-observation:incompatible");
    let field = &quarantine[0]["fields"][0];
    assert_eq!(field["scope"]["kind"], "field_path");
    assert_eq!(field["observed_field"]["name"], "VendorID");
    assert_eq!(field["observed_field"]["data_type"]["kind"], "utf8");
    assert_eq!(field["effective_field"]["name"], "vendor_id");
    assert_eq!(field["effective_field"]["data_type"]["kind"], "int");
    let package_reader = cdf_package::PackageReader::open(&package).unwrap();
    let mut segment_count = 0_u64;
    package_reader
        .for_each_identity_segment(&mut |_| {
            segment_count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(segment_count, 0);

    write_string_vendor_parquet(&project.root.join("data/b.parquet"));
    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "run",
        "local.events",
    ]);
    assert_eq!(human.exit_code, 0, "{}", human.stderr);
    assert!(human.stdout.contains("b.parquet"));
    assert!(human.stdout.contains("VendorID"));
    assert!(human.stdout.contains("Utf8"));
    assert!(
        human.stdout.contains("publish a compat"),
        "{}",
        human.stdout
    );
}

#[test]
fn run_ndjson_discover_schema_resource_autopins_and_commits() {
    let project = TestProject::new();
    write_discovered_schema_resource(&project);

    let result = run_valid_run_args(&project);

    assert_eq!(result.exit_code, 0, "{}", result.stderr);
    assert!(project.root.join(".cdf/schemas").exists());
    assert!(
        run_package_dir(&project, &result)
            .join("manifest.json")
            .exists()
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource_id"], "local.events");
    assert!(project.root.join(".cdf/state.db").exists());
}

#[test]
fn run_loop_remains_unsupported_without_writes() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "--loop",
    ]);

    assert_eq!(result.exit_code, 78);
    assert_no_run_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], true);
    assert!(json["error"]["message"].as_str().unwrap().contains("loop"));
}
