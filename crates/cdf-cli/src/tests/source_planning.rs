use super::*;

#[test]
fn plan_local_parquet_discover_autopins_snapshot_and_reports_hash() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["schema_snapshot"]["outcome"], "added");
    assert_eq!(report["schema_snapshot"]["snapshot_written"], true);
    assert_eq!(report["schema_snapshot"]["lockfile_written"], true);
    assert_eq!(report["resource_schema"]["schema_source"], "discovered");
    let snapshot_path = report["resource_schema"]["snapshot_path"].as_str().unwrap();
    assert!(snapshot_path.starts_with(".cdf/schemas/local.events@sha256:"));
    let snapshot = read_snapshot_json(&project, snapshot_path);
    assert_eq!(
        report["resource_schema"]["schema_hash"],
        snapshot["schema_hash"]
    );
    assert_eq!(
        report["resource_schema"]["baseline_schema_hash"],
        snapshot["schema_hash"]
    );
    assert_eq!(
        report["resource_schema"]["effective_schema_hash"],
        report["resource_schema"]["schema_hash"]
    );
    assert_eq!(
        report["schema_snapshot"]["schema_hash"],
        snapshot["schema_hash"]
    );
    assert_eq!(
        report["resource_schema"]["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    assert_eq!(snapshot["schema"]["fields"][0]["name"], "vendor_id");
    assert_eq!(
        snapshot["schema"]["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );
    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    assert_eq!(
        lock.resources["local.events"]
            .schema_snapshot
            .as_ref()
            .unwrap()
            .schema_hash
            .as_str(),
        snapshot["schema_hash"].as_str().unwrap()
    );
}

#[test]
fn keyless_append_file_validate_plan_preview_run_has_no_key_nudge() {
    let project = TestProject::new();

    let validate = run(["cdf", "--json", "--project", project.root_str(), "validate"]);
    assert_eq!(validate.exit_code, 0, "stderr: {}", validate.stderr);
    assert_no_key_nudge(&validate);

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    assert_no_key_nudge(&plan);
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    assert_eq!(plan_json["result"]["destination"]["disposition"], "append");
    assert_eq!(
        plan_json["result"]["delivery_guarantee"],
        "effectively_once_per_package"
    );

    let human_plan = run([
        "cdf",
        "--color",
        "never",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(human_plan.exit_code, 0, "stderr: {}", human_plan.stderr);
    assert!(human_plan.stdout.contains("disposition"));
    assert!(human_plan.stdout.contains("append"));
    assert_no_key_nudge(&human_plan);

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    assert_no_key_nudge(&preview);

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    assert_no_key_nudge(&run_result);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(run_json["result"]["receipt"]["disposition"], "append");
    assert_eq!(run_json["result"]["row_count"], 2);
}

#[test]
fn keyless_append_rest_validate_plan_preview_run_has_no_key_nudge() {
    let project = TestProject::new();
    fs::write(project.root.join("rest-token"), "keyless-rest-token\n").unwrap();
    let body = r#"{ "items": [
        { "id": 1, "updated_at": 10 },
        { "id": 2, "updated_at": 20 }
    ] }"#;
    let (base_url, requests) = serve_json_sequence([body, body]);
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );
    let validate = run(["cdf", "--json", "--project", project.root_str(), "validate"]);
    assert_eq!(validate.exit_code, 0, "stderr: {}", validate.stderr);
    assert_no_key_nudge(&validate);

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "api.items",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    assert_no_key_nudge(&plan);

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "api.items",
    ]);
    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    assert_no_key_nudge(&preview);

    let run_result = run_valid_run_resource(&project, "api.items");
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    assert_no_key_nudge(&run_result);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(run_json["result"]["receipt"]["disposition"], "append");
    assert_eq!(run_json["result"]["row_count"], 2);
    assert_eq!(requests.lock().unwrap().len(), 2);
}

#[test]
fn multi_file_parquet_no_pin_and_autopin_are_all_file_metadata_and_byte_stable() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/a.parquet"));
    write_vendor_score_parquet(&project.root.join("data/b.parquet"));

    let inspection = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--no-pin",
    ]);
    assert_eq!(inspection.exit_code, 0, "{}", inspection.stderr);
    assert_eq!(
        stderr_or_stdout_json(&inspection.stdout)["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());

    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(first.exit_code, 0, "{}", first.stderr);
    let first_report = stderr_or_stdout_json(&first.stdout);
    assert_eq!(
        first_report["result"]["schema_snapshot"]["outcome"],
        "added"
    );
    let snapshot_path = first_report["result"]["schema_snapshot"]["path"]
        .as_str()
        .unwrap();
    let snapshot_before = fs::read(project.root.join(snapshot_path)).unwrap();
    let snapshot_json: serde_json::Value = serde_json::from_slice(&snapshot_before).unwrap();
    let manifest_path = snapshot_json["metadata"]["cdf:discovery_manifest_path"]
        .as_str()
        .unwrap();
    let manifest_before = fs::read(project.root.join(manifest_path)).unwrap();
    let manifest_json: serde_json::Value = serde_json::from_slice(&manifest_before).unwrap();
    assert_eq!(manifest_json["file_coverage"], "all_files");
    assert_eq!(manifest_json["within_file_coverage"], "format_metadata");
    assert_eq!(manifest_json["candidates"].as_array().unwrap().len(), 2);
    assert!(
        manifest_json["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .all(|candidate| candidate["participation"] == "observed")
    );
    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(second.exit_code, 0, "{}", second.stderr);
    assert_eq!(
        stderr_or_stdout_json(&second.stdout)["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(
        fs::read(project.root.join(snapshot_path)).unwrap(),
        snapshot_before
    );
    assert_eq!(
        fs::read(project.root.join(manifest_path)).unwrap(),
        manifest_before
    );
}

#[test]
fn plan_discover_autopin_is_byte_stable_and_preserves_unrelated_semantic_locks() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    let first_report = stderr_or_stdout_json(&first.stdout);
    assert_eq!(
        first_report["result"]["schema_snapshot"]["outcome"],
        "added"
    );
    let lock_before = fs::read(project.root.join("cdf.lock")).unwrap();
    let snapshot_path = first_report["result"]["schema_snapshot"]["path"]
        .as_str()
        .unwrap();
    let snapshot_before = fs::read(project.root.join(snapshot_path)).unwrap();

    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    let second_report = stderr_or_stdout_json(&second.stdout);
    assert_eq!(
        second_report["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(
        second_report["result"]["schema_snapshot"]["snapshot_written"],
        false
    );
    assert_eq!(
        second_report["result"]["schema_snapshot"]["lockfile_written"],
        false
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
    assert_eq!(
        fs::read(project.root.join(snapshot_path)).unwrap(),
        snapshot_before
    );
    let human = run([
        "cdf",
        "--color",
        "never",
        "--project",
        project.root_str(),
        "-v",
        "plan",
        "local.events",
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(human.stdout.contains("Schema Snapshot"), "{}", human.stdout);
    assert!(human.stdout.contains("outcome"), "{}", human.stdout);
    assert!(human.stdout.contains("unchanged"), "{}", human.stdout);

    let mut lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    let mut unrelated = lock.resources["local.events"].clone();
    unrelated.descriptor.resource_id = ResourceId::new("unrelated.events").unwrap();
    lock.resources
        .insert("unrelated.events".to_owned(), unrelated.clone());
    fs::write(
        project.root.join("cdf.lock"),
        cdf_project::lock_to_toml(&lock).unwrap(),
    )
    .unwrap();
    let third = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(third.exit_code, 0, "stderr: {}", third.stderr);
    let updated = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    assert_eq!(updated.resources["unrelated.events"], unrelated);

    write_vendor_score_parquet(&project.root.join("data/vendors.parquet"));
    let locked_before_drift = fs::read(project.root.join("cdf.lock")).unwrap();
    let snapshots_before_drift = schema_snapshot_paths(&project);
    let pinned_hash = first_report["result"]["schema_snapshot"]["schema_hash"]
        .as_str()
        .unwrap();
    let pinned_plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(pinned_plan.exit_code, 0, "stderr: {}", pinned_plan.stderr);
    let pinned_plan_report = stderr_or_stdout_json(&pinned_plan.stdout);
    assert_eq!(
        pinned_plan_report["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(
        pinned_plan_report["result"]["schema_snapshot"]["schema_hash"],
        pinned_hash
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        locked_before_drift
    );
    assert_eq!(schema_snapshot_paths(&project), snapshots_before_drift);

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        locked_before_drift
    );
    assert_eq!(schema_snapshot_paths(&project), snapshots_before_drift);

    let run_result = run_valid_run_resource(&project, "local.events");
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_report = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(
        run_report["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(run_report["result"]["schema_hash"], pinned_hash);
    assert_eq!(
        run_report["result"]["schema_snapshot"]["schema_hash"],
        pinned_hash
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        locked_before_drift
    );
    assert_eq!(schema_snapshot_paths(&project), snapshots_before_drift);

    let inspection = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--no-pin",
    ]);
    assert_eq!(inspection.exit_code, 0, "stderr: {}", inspection.stderr);
    let inspection_report = stderr_or_stdout_json(&inspection.stdout);
    assert_eq!(
        inspection_report["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert_ne!(
        inspection_report["result"]["schema_snapshot"]["schema_hash"],
        pinned_hash
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        locked_before_drift
    );
    assert_eq!(schema_snapshot_paths(&project), snapshots_before_drift);

    let explicit_pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "pin",
        "local.events",
    ]);
    assert_eq!(explicit_pin.exit_code, 0, "stderr: {}", explicit_pin.stderr);
    let explicit_pin_report = stderr_or_stdout_json(&explicit_pin.stdout);
    assert_eq!(explicit_pin_report["result"]["status"], "refreshed");
    assert_ne!(explicit_pin_report["result"]["schema_hash"], pinned_hash);
    let refreshed_lock =
        parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    assert_eq!(refreshed_lock.resources["unrelated.events"], unrelated);
}

#[test]
fn plan_and_explain_no_pin_discover_without_project_writes() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));
    let before = project_tree_snapshot(&project.root);

    for command in ["plan", "explain"] {
        let result = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            command,
            "local.events",
            "--no-pin",
        ]);
        assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
        let report = stderr_or_stdout_json(&result.stdout);
        assert_eq!(
            report["result"]["schema_snapshot"]["outcome"],
            "inspection_only"
        );
        assert_eq!(
            report["result"]["schema_snapshot"]["snapshot_written"],
            false
        );
        assert_eq!(
            report["result"]["schema_snapshot"]["lockfile_written"],
            false
        );
        assert_eq!(project_tree_snapshot(&project.root), before);
    }
}

#[test]
fn ordinary_plan_fails_closed_when_locked_snapshot_artifact_is_missing() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));
    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    let report = stderr_or_stdout_json(&first.stdout);
    let snapshot_path = report["result"]["schema_snapshot"]["path"]
        .as_str()
        .unwrap();
    let lock_before = fs::read(project.root.join("cdf.lock")).unwrap();
    fs::remove_file(project.root.join(snapshot_path)).unwrap();

    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);

    assert_ne!(second.exit_code, 0);
    let output = format!("{}{}", second.stdout, second.stderr);
    assert!(output.contains(snapshot_path), "{output}");
    assert!(!project.root.join(snapshot_path).exists());
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );

    let inspection = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--no-pin",
    ]);
    assert_eq!(inspection.exit_code, 0, "stderr: {}", inspection.stderr);
    let inspection_report = stderr_or_stdout_json(&inspection.stdout);
    assert_eq!(
        inspection_report["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert!(!project.root.join(snapshot_path).exists());
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
}

#[test]
fn no_pin_is_documented_for_plan_and_explain_but_rejected_by_run() {
    for command in ["plan", "explain"] {
        let help = run(["cdf", "help", command]);
        assert_eq!(help.exit_code, 0, "stderr: {}", help.stderr);
        assert!(help.stdout.contains("--no-pin"), "{}", help.stdout);
    }

    let run_help = run(["cdf", "help", "run"]);
    assert_eq!(run_help.exit_code, 0, "stderr: {}", run_help.stderr);
    assert!(!run_help.stdout.contains("--no-pin"));
    let rejected = run(["cdf", "run", "local.events", "--no-pin"]);
    assert_eq!(rejected.exit_code, 2);
    assert!(rejected.stderr.contains("unexpected argument '--no-pin'"));
}

#[test]
fn rest_plan_no_pin_is_write_free_and_redacts_resolved_secret() {
    let project = TestProject::new();
    fs::write(project.root.join("rest-token"), "rest-no-pin-secret\n").unwrap();
    let (base_url, requests) =
        serve_json_sequence([r#"{ "items": [{ "VendorID": 1, "updated_at": 10 }] }"#]);
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );
    fs::write(
        project.root.join("cdf/api/items.cdf.sql"),
        rest_resource_sql("exact"),
    )
    .unwrap();
    let before = project_tree_snapshot(&project.root);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "api.items",
        "--no-pin",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "rest-no-pin-secret");
    let report = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        report["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert_eq!(project_tree_snapshot(&project.root), before);
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[test]
fn postgres_discover_mode_plan_preview_run_autopins_through_file_secret_without_leaks() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("discover_run_orders");
    let mut client = postgres.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"VendorID\" BIGINT NOT NULL,
                \"updated_at\" BIGINT NOT NULL
            );
            INSERT INTO {} (\"VendorID\", \"updated_at\") VALUES (1, 10), (2, 20), (3, 30)",
            table, table
        ))
        .unwrap();

    let project = TestProject::new();
    let source_dsn = postgres.url.replacen(
        "postgresql://cdf@",
        "postgresql://cdf:source-discover-run-secret@",
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
        postgres_resource_sql_with_vendor_cursor(&table),
    )
    .unwrap();

    let before_no_pin = project_tree_snapshot(&project.root);
    let no_pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "warehouse.orders",
        "--no-pin",
    ]);
    assert_eq!(no_pin.exit_code, 0, "stderr: {}", no_pin.stderr);
    assert_secret_absent(&no_pin, &source_dsn);
    assert_secret_absent(&no_pin, "source-discover-run-secret");
    let no_pin_report = stderr_or_stdout_json(&no_pin.stdout);
    assert_eq!(
        no_pin_report["result"]["schema_snapshot"]["outcome"],
        "inspection_only"
    );
    assert_eq!(project_tree_snapshot(&project.root), before_no_pin);

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "warehouse.orders",
    ]);

    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    assert_secret_absent(&plan, &source_dsn);
    assert_secret_absent(&plan, "source-discover-run-secret");
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    let plan_report = &plan_json["result"];
    assert_eq!(
        plan_report["resource_schema"]["schema_source"],
        "discovered"
    );
    assert_eq!(
        plan_report["resource_schema"]["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    let snapshot_path = plan_report["resource_schema"]["snapshot_path"]
        .as_str()
        .unwrap();
    let snapshot = read_snapshot_json(&project, snapshot_path);
    let snapshot_text = snapshot.to_string();
    assert!(!snapshot_text.contains(&source_dsn));
    assert!(!snapshot_text.contains("source-discover-run-secret"));
    let lock_text = fs::read_to_string(project.root.join("cdf.lock")).unwrap();
    assert!(!lock_text.contains(&source_dsn));
    assert!(!lock_text.contains("source-discover-run-secret"));
    assert_eq!(snapshot["schema"]["fields"][0]["name"], "vendor_id");
    assert_eq!(
        snapshot["schema"]["fields"][0]["metadata"]["cdf:source_name"],
        "VendorID"
    );

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "warehouse.orders",
        "--filter",
        "vendor_id >= 2",
        "--limit",
        "1",
    ]);

    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    assert_secret_absent(&preview, &source_dsn);
    assert_secret_absent(&preview, "source-discover-run-secret");
    assert_no_preview_writes(&project);
    let preview_json = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_json["result"]["resource"], "warehouse.orders");
    assert_eq!(preview_json["result"]["partition"], "sql");
    assert_eq!(preview_json["result"]["row_count"], 1);

    let run_result = run_valid_run_resource(&project, "warehouse.orders");

    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    assert_secret_absent(&run_result, &source_dsn);
    assert_secret_absent(&run_result, "source-discover-run-secret");
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_report = &run_json["result"];
    assert_eq!(run_report["resource_id"], "warehouse.orders");
    assert_eq!(run_report["schema_snapshot"]["outcome"], "unchanged");
    assert_eq!(run_report["schema_snapshot"]["snapshot_written"], false);
    assert_eq!(run_report["schema_snapshot"]["lockfile_written"], false);
    assert_eq!(run_report["target"], "orders");
    assert_eq!(run_report["schema_hash"], snapshot["schema_hash"]);
    assert_eq!(run_report["row_count"], 3);
    assert_eq!(run_report["checkpoint"]["status"], "committed");
    let package_dir = run_package_dir(&project, &run_result);
    let admission_plan: cdf_engine::CompiledSchemaAdmissionPlan =
        serde_json::from_slice(&fs::read(package_dir.join("plan/schema-admission.json")).unwrap())
            .unwrap();
    let admission_evidence: CompiledStreamAdmissionEvidence = serde_json::from_slice(
        &fs::read(package_dir.join("schema/stream-admission-evidence.json")).unwrap(),
    )
    .unwrap();
    admission_evidence.validate(&admission_plan).unwrap();
    assert_eq!(admission_evidence.observations.len(), 1);

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows = conn
        .prepare("SELECT vendor_id, updated_at FROM orders ORDER BY vendor_id")
        .unwrap()
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, vec![(1, 10), (2, 20), (3, 30)]);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("warehouse.orders").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed Postgres discover run head");
    assert_eq!(
        head.delta.schema_hash.as_str(),
        snapshot["schema_hash"].as_str().unwrap()
    );
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        run_report["checkpoint_id"].as_str().unwrap()
    );
}

#[test]
fn postgres_add_pins_private_secret_and_runs_discovered_table() {
    use std::os::unix::fs::PermissionsExt;

    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("orders_add");
    postgres
        .client()
        .batch_execute(&format!(
            "CREATE TABLE {} (id BIGSERIAL PRIMARY KEY, updated_at TIMESTAMP NOT NULL, amount BIGINT); INSERT INTO {} (updated_at, amount) VALUES (NOW(), 10), (NOW(), 20)",
            table,
            table,
        ))
        .unwrap();
    let project = TestProject::new();
    let source_url = postgres.url.replacen(
        "postgresql://cdf@",
        "postgresql://cdf:s4-private-password@",
        1,
    );
    let location = format!("{}/{}", source_url.trim_end_matches('/'), table);

    let dry = TestProject::new();
    let dry_run = run([
        "cdf",
        "--json",
        "--project",
        dry.root_str(),
        "add",
        "warehouse.orders",
        &location,
        "--dry-run",
    ]);
    assert_eq!(dry_run.exit_code, 0, "{}", dry_run.stderr);
    assert!(!dry.root.join("cdf/warehouse/orders.cdf.sql").exists());
    assert!(!dry.root.join(".cdf/secrets").exists());
    assert!(!dry.root.join("cdf.lock").exists());

    let add = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "warehouse.orders",
        &location,
    ]);
    assert_eq!(add.exit_code, 0, "{}", add.stderr);
    assert_secret_absent(&add, "s4-private-password");
    let report = stderr_or_stdout_json(&add.stdout);
    assert_eq!(report["result"]["resource_id"], "warehouse.orders");
    assert_eq!(
        report["result"]["resource_path"],
        "cdf/warehouse/orders.cdf.sql"
    );
    assert_eq!(report["result"]["writes"]["resource_sql"], true);
    assert_eq!(report["result"]["writes"]["configured_source"], true);
    assert_eq!(report["result"]["writes"]["private_source_state"], true);
    assert_eq!(report["result"]["writes"]["lockfile"], false);
    let project_config = fs::read_to_string(project.root.join("cdf.toml")).unwrap();
    assert!(project_config.contains("[sources.warehouse]"));
    assert!(
        project_config
            .contains("connection = \"secret://file/.cdf/secrets/sources/warehouse.dsn\"")
    );
    assert!(!project_config.contains("s4-private-password"));
    let resource = fs::read_to_string(project.root.join("cdf/warehouse/orders.cdf.sql")).unwrap();
    assert!(resource.contains(&format!("table => '{table}'")));
    let secret = project.root.join(".cdf/secrets/sources/warehouse.dsn");
    assert_eq!(
        fs::metadata(&secret).unwrap().permissions().mode() & 0o777,
        0o600
    );

    for command in ["plan", "preview"] {
        let result = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            command,
            "warehouse.orders",
        ]);
        assert_eq!(result.exit_code, 0, "{command}: {}", result.stderr);
    }
    let resource_path = project.root.join("cdf/warehouse/orders.cdf.sql");
    let with_cursor = fs::read_to_string(&resource_path)
        .unwrap()
        .replace("TRUST GOVERNED", "CURSOR updated_at\nTRUST GOVERNED");
    fs::write(resource_path, with_cursor).unwrap();
    let run_result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "warehouse.orders",
    ]);
    assert_eq!(run_result.exit_code, 0, "{}", run_result.stderr);
    assert_eq!(
        stderr_or_stdout_json(&run_result.stdout)["result"]["row_count"],
        2
    );
}

#[test]
fn rest_discover_mode_plan_preview_run_autopins_through_file_secret_without_leaks() {
    let project = TestProject::new();
    fs::write(project.root.join("rest-token"), "rest-autopin-secret\n").unwrap();
    let body = r#"{ "items": [
        { "VendorID": 1, "updated_at": 10 },
        { "VendorID": 2, "updated_at": 20 }
    ] }"#;
    let (base_url, requests) = serve_json_sequence([body, body, body, body, body]);
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );
    fs::write(
        project.root.join("cdf/api/items.cdf.sql"),
        rest_resource_sql("exact"),
    )
    .unwrap();

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "api.items",
    ]);

    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    assert_secret_absent(&plan, "rest-autopin-secret");
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let plan_json = stderr_or_stdout_json(&plan.stdout);
    let plan_report = &plan_json["result"];
    assert_eq!(
        plan_report["resource_schema"]["schema_source"],
        "discovered"
    );
    assert_eq!(
        plan_report["resource_schema"]["snapshot_metadata"]["probe"],
        "registered-source-discovery"
    );
    let snapshot_path = plan_report["resource_schema"]["snapshot_path"]
        .as_str()
        .unwrap();
    let snapshot = read_snapshot_json(&project, snapshot_path);
    let snapshot_text = snapshot.to_string();
    assert!(!snapshot_text.contains("rest-autopin-secret"));
    assert!(
        !fs::read_to_string(project.root.join("cdf.lock"))
            .unwrap()
            .contains("rest-autopin-secret")
    );
    let snapshot_fields = snapshot["schema"]["fields"].as_array().unwrap();
    assert!(
        snapshot_fields
            .iter()
            .any(|field| field["name"] == "updated_at")
    );
    let vendor = snapshot_fields
        .iter()
        .find(|field| field["name"] == "vendor_id")
        .unwrap();
    assert_eq!(vendor["metadata"]["cdf:source_name"], "VendorID");

    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "api.items",
    ]);

    assert_eq!(preview.exit_code, 0, "stderr: {}", preview.stderr);
    assert_secret_absent(&preview, "rest-autopin-secret");
    assert_no_preview_writes(&project);
    let preview_json = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_json["result"]["resource"], "api.items");
    assert_eq!(preview_json["result"]["partition"], "rest");
    assert_eq!(preview_json["result"]["row_count"], 2);

    let run_result = run_valid_run_resource(&project, "api.items");

    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    assert_secret_absent(&run_result, "rest-autopin-secret");
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_report = &run_json["result"];
    assert_eq!(run_report["resource_id"], "api.items");
    assert_eq!(run_report["schema_snapshot"]["outcome"], "unchanged");
    assert_eq!(run_report["schema_snapshot"]["snapshot_written"], false);
    assert_eq!(run_report["schema_snapshot"]["lockfile_written"], false);
    assert_eq!(run_report["schema_hash"], snapshot["schema_hash"]);
    assert_eq!(run_report["row_count"], 2);
    assert_eq!(run_report["checkpoint"]["status"], "committed");

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows = conn
        .prepare("SELECT vendor_id, updated_at FROM items ORDER BY vendor_id")
        .unwrap()
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows, vec![(1, 10), (2, 20)]);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("api.items").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed REST discover run head");
    assert_eq!(
        head.delta.schema_hash.as_str(),
        snapshot["schema_hash"].as_str().unwrap()
    );
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        run_report["checkpoint_id"].as_str().unwrap()
    );

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(
        requests
            .iter()
            .all(|request| request.contains("authorization: Bearer rest-autopin-secret"))
    );
}

#[test]
fn cold_rest_run_reuses_the_discovery_page_without_a_second_request() {
    let project = TestProject::new();
    fs::write(
        project.root.join("rest-token"),
        "rest-single-request-secret\n",
    )
    .unwrap();
    let base_url = serve_json_once(
        r#"{ "items": [
            { "VendorID": 1, "updated_at": 10 },
            { "VendorID": 2, "updated_at": 20 }
        ] }"#,
    );
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );
    fs::write(
        project.root.join("cdf/api/items.cdf.sql"),
        rest_resource_sql("exact"),
    )
    .unwrap();

    let result = run_valid_run_resource(&project, "api.items");

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "rest-single-request-secret");
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 2);
    assert_eq!(json["result"]["schema_snapshot"]["outcome"], "added");
    assert_eq!(json["result"]["checkpoint"]["status"], "committed");
}
