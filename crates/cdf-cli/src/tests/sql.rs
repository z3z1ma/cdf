use super::*;

#[test]
fn sql_mounts_checkpoint_package_and_receipt_tables_as_json_rows() {
    let project = TestProject::new();
    compile_test_project(&project);
    let fixture = create_system_sql_fixture(&project);
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select p.package_hash, p.status, s.segment_id, c.checkpoint_id, c.status as checkpoint_status, r.receipt_id, c.output_position_json from packages p join package_segments s using (package_hash) join checkpoints c using (package_hash) join package_receipts r using (package_hash) order by p.package_id",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "sql");
    let result = json["result"].as_object().unwrap();
    assert_eq!(result.len(), 3);
    assert!(result.contains_key("columns"));
    assert!(result.contains_key("rows"));
    assert!(result.contains_key("tables"));
    assert_eq!(
        json["result"]["columns"],
        json!([
            "package_hash",
            "status",
            "segment_id",
            "checkpoint_id",
            "checkpoint_status",
            "receipt_id",
            "output_position_json"
        ])
    );
    assert_eq!(json["result"]["rows"].as_array().unwrap().len(), 1);
    let row = &json["result"]["rows"][0];
    assert_eq!(row[0], fixture.package_hash);
    assert_eq!(row[1], "checkpointed");
    assert_eq!(row[2], "seg-000001");
    assert_eq!(row[3], "checkpoint-sql-1");
    assert_eq!(row[4], "committed");
    assert_eq!(row[5], "receipt-sql-1");
    let position: serde_json::Value = serde_json::from_str(row[6].as_str().unwrap()).unwrap();
    assert_eq!(position["kind"], "cursor");
    assert_eq!(position["version"], cdf_kernel::SOURCE_POSITION_VERSION);
    assert!(
        json["result"]["tables"]
            .as_array()
            .unwrap()
            .iter()
            .any(|table| table == "package_files")
    );
}

#[test]
fn sql_human_output_is_concise_for_scheduler_logs() {
    let project = TestProject::new();
    compile_test_project(&project);
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "sql",
        "select count(*) as package_count from packages",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(
        result
            .stdout
            .contains("OK sql returned 1 row(s) from verified project artifacts")
    );
    assert!(result.stdout.contains("System SQL"));
    assert!(result.stdout.contains("package_count"));
    assert!(result.stdout.contains("\n0"));
    assert!(
        result
            .stdout
            .contains("Next: cdf sql \"select * from packages limit 5\"")
    );
}

#[test]
fn sql_read_only_query_does_not_create_local_artifacts() {
    let project = TestProject::new();
    compile_test_project(&project);
    let state_path = project.root.join(".cdf/state.db");
    let package_root = project.root.join(".cdf/packages");
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select count(*) as checkpoint_count from checkpoints",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["columns"], json!(["checkpoint_count"]));
    assert_eq!(json["result"]["rows"][0][0], 0);
    assert!(!state_path.exists(), "sql must not create the state DB");
    assert!(
        !package_root.exists(),
        "sql must not create the package root"
    );
}

#[test]
fn compile_publishes_independent_artifact_index_and_locked_rebuild() {
    let project = TestProject::new();
    let prepared = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "local.events",
    ]);
    assert_eq!(
        prepared.exit_code, 0,
        "stdout: {}\nstderr: {}",
        prepared.stdout, prepared.stderr
    );
    let prepared = stderr_or_stdout_json(&prepared.stdout);
    assert_eq!(prepared["command"], "compile");
    assert_eq!(prepared["result"]["counts"]["compiled"], 1);
    assert_eq!(
        prepared["result"]["resources"][0]["discovered_schema"],
        true
    );
    let artifact_path = prepared["result"]["resources"][0]["artifact_path"]
        .as_str()
        .unwrap();
    assert!(project.root.join(artifact_path).is_file());

    let lock_before = fs::read(project.root.join("cdf.lock")).unwrap();
    let index_before = fs::read(project.root.join(".cdf/manifest.json")).unwrap();
    let locked = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "local.events",
        "--locked",
    ]);
    assert_eq!(
        locked.exit_code, 0,
        "stdout: {}\nstderr: {}",
        locked.stdout, locked.stderr
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
    assert_eq!(
        fs::read(project.root.join(".cdf/manifest.json")).unwrap(),
        index_before
    );
}

#[test]
fn selected_config_bindings_ignore_unrelated_sources_and_stale_on_relevant_changes() {
    let project = TestProject::new();
    let prepared = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "local.events",
    ]);
    assert_eq!(prepared.exit_code, 0, "stderr: {}", prepared.stderr);
    let prepared = stderr_or_stdout_json(&prepared.stdout);
    let artifact_hash = prepared["result"]["resources"][0]["artifact_hash"]
        .as_str()
        .unwrap();

    let config_path = project.root.join("cdf.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        format!("{config}\n[sources.unrelated]\ntype = \"files\"\nroot = \"unrelated-data\"\n"),
    )
    .unwrap();
    let locked = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "local.events",
        "--locked",
    ]);
    assert_eq!(locked.exit_code, 0, "stderr: {}", locked.stderr);
    let locked = stderr_or_stdout_json(&locked.stdout);
    assert_eq!(
        locked["result"]["resources"][0]["artifact_hash"],
        artifact_hash
    );

    let config = fs::read_to_string(&config_path).unwrap().replacen(
        "root = \"data\"",
        "root = \"changed-data\"",
        1,
    );
    fs::write(&config_path, config).unwrap();
    let status = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select status from compilation_resources where resource_id = 'local.events'",
    ]);
    assert_eq!(status.exit_code, 0, "stderr: {}", status.stderr);
    assert_eq!(
        stderr_or_stdout_json(&status.stdout)["result"]["rows"],
        json!([["stale"]])
    );
}

#[test]
fn project_defaults_stale_only_resources_that_resolve_through_them() {
    let project = TestProject::new();
    fs::write(
        project.root.join("cdf/local/events.cdf.sql"),
        RESOURCE.replace("TRUST GOVERNED\n", ""),
    )
    .unwrap();
    let prepared = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "local.events",
    ]);
    assert_eq!(prepared.exit_code, 0, "stderr: {}", prepared.stderr);

    let config_path = project.root.join("cdf.toml");
    let config = fs::read_to_string(&config_path).unwrap();
    fs::write(
        &config_path,
        format!("{config}\n[defaults]\ntrust = \"governed\"\n"),
    )
    .unwrap();
    let status = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select status from compilation_resources where resource_id = 'local.events'",
    ]);
    assert_eq!(status.exit_code, 0, "stderr: {}", status.stderr);
    assert_eq!(
        stderr_or_stdout_json(&status.stdout)["result"]["rows"],
        json!([["stale"]])
    );
}

#[test]
fn compile_selectors_isolate_resources_and_aggregate_partial_success() {
    let project = TestProject::new();
    fs::create_dir_all(project.root.join("cdf/broken")).unwrap();
    fs::write(
        project.root.join("cdf/broken/unknown.cdf.sql"),
        RESOURCE.replace("source => 'local'", "source => 'missing'"),
    )
    .unwrap();

    let selected = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "local.*",
    ]);
    assert_eq!(selected.exit_code, 0, "stderr: {}", selected.stderr);
    let selected = stderr_or_stdout_json(&selected.stdout);
    assert_eq!(selected["result"]["selection"], json!(["local.events"]));

    let aggregate = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "*",
    ]);
    assert_eq!(aggregate.exit_code, 1);
    let aggregate = stderr_or_stdout_json(&aggregate.stdout);
    assert_eq!(aggregate["result"]["counts"]["compiled"], 1);
    assert_eq!(aggregate["result"]["counts"]["failed"], 1);
    assert_eq!(
        aggregate["result"]["resources"][0]["resource_id"],
        "broken.unknown"
    );
    assert_eq!(aggregate["result"]["resources"][0]["status"], "failed");
    assert_eq!(
        aggregate["result"]["resources"][0]["error"]["code"],
        "CDF-SOURCE-UNKNOWN"
    );
    assert!(
        !aggregate["result"]["resources"][0]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("[CDF-SOURCE-UNKNOWN]")
    );
    assert_eq!(aggregate["result"]["resources"][1]["status"], "compiled");

    let index = cdf_project::parse_compilation_index(
        &fs::read(project.root.join(".cdf/manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        index.resources["broken.unknown"].status,
        cdf_project::CompilationStatus::Failed
    );
    assert_eq!(
        index.resources["local.events"].status,
        cdf_project::CompilationStatus::Current
    );
}

#[test]
fn unscoped_compile_marks_deleted_known_resources_absent() {
    let project = TestProject::new();
    compile_test_project(&project);
    fs::remove_file(project.root.join("cdf/local/events.cdf.sql")).unwrap();
    fs::remove_dir(project.root.join("cdf/local")).unwrap();

    let result = run(["cdf", "--json", "--project", project.root_str(), "compile"]);
    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let index = cdf_project::parse_compilation_index(
        &fs::read(project.root.join(".cdf/manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        index.resources["local.events"].status,
        cdf_project::CompilationStatus::Absent
    );
    assert!(index.resources["local.events"].artifact.is_none());
}

#[test]
fn locked_compile_reports_missing_selected_authority_and_indexes_failure() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "local.events",
        "--locked",
    ]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["counts"]["failed"], 1);
    assert_eq!(json["result"]["resources"][0]["error"]["kind"], "contract");
    assert!(project.root.join(".cdf/manifest.json").is_file());
    assert!(!project.root.join("cdf.lock").exists());
}

#[test]
fn compile_preserves_per_resource_data_and_auth_diagnostics() {
    let data_project = TestProject::new();
    let prepared = run([
        "cdf",
        "--project",
        data_project.root_str(),
        "compile",
        "local.events",
    ]);
    assert_eq!(prepared.exit_code, 0, "stderr: {}", prepared.stderr);
    fs::remove_file(data_project.root.join(".cdf/manifest.json")).unwrap();
    fs::create_dir(data_project.root.join(".cdf/manifest.json")).unwrap();

    let data = run([
        "cdf",
        "--json",
        "--project",
        data_project.root_str(),
        "compile",
    ]);
    assert_eq!(data.exit_code, 1);
    let data_json = stderr_or_stdout_json(&data.stdout);
    assert_eq!(
        data_json["result"]["resources"][0]["error"]["kind"], "data",
        "{data_json}"
    );
    let data_message = data_json["result"]["resources"][0]["error"]["message"]
        .as_str()
        .unwrap();
    assert!(
        data_message.contains("must be a regular non-symlink file"),
        "{data_message}"
    );

    let auth_project = TestProject::new();
    fs::write(
        auth_project.root.join("cdf.toml"),
        r#"
[project]
name = "cli_test"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[sources.local]
type = "files"
root = "https://example.com/data"
egress_allowlist = ["not-example.com"]
"#,
    )
    .unwrap();
    fs::write(
        auth_project.root.join("cdf/local/events.cdf.sql"),
        RESOURCE.replace("*.ndjson", "events.ndjson"),
    )
    .unwrap();
    let auth = run([
        "cdf",
        "--json",
        "--project",
        auth_project.root_str(),
        "compile",
        "local.events",
    ]);
    assert_eq!(auth.exit_code, 1);
    let auth_json = stderr_or_stdout_json(&auth.stdout);
    assert_eq!(auth_json["result"]["resources"][0]["error"]["kind"], "auth");
    let auth_message = auth_json["result"]["resources"][0]["error"]["message"]
        .as_str()
        .unwrap();
    assert!(auth_message.contains("egress"), "{auth_message}");
}

#[test]
fn compile_discovers_only_selected_source_and_publishes_schema_authority() {
    let project = TestProject::new();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "local.events",
    ]);
    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resources"][0]["discovered_schema"], true);
    assert!(project.root.join(".cdf/schemas").is_dir());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    assert!(lock.resources["local.events"].schema_snapshot.is_some());
}

#[test]
fn compile_binds_destination_uri_aliases_to_canonical_ids() {
    for (uri, expected_id) in [
        ("postgresql://localhost/cdf", "postgres"),
        ("clickhouses://localhost:8443/default", "clickhouse"),
        ("parquet://.cdf/parquet", "parquet_object_store"),
    ] {
        let project = TestProject::new();
        write_project_destination(&project, uri);
        let result = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "compile",
            "local.events",
        ]);
        assert_eq!(result.exit_code, 0, "{uri}: {}", result.stderr);
        let index = cdf_project::parse_compilation_index(
            &fs::read(project.root.join(".cdf/manifest.json")).unwrap(),
        )
        .unwrap();
        let artifact_ref = index.resources["local.events"].artifact.as_ref().unwrap();
        let artifact = cdf_project::parse_compiled_resource_artifact(
            &fs::read(project.root.join(&artifact_ref.path)).unwrap(),
        )
        .unwrap();
        assert_eq!(artifact.resource.destination.destination_id, expected_id);
        assert_eq!(
            artifact
                .resource
                .destination
                .sheet
                .sheet
                .destination
                .as_str(),
            expected_id
        );
    }
}

#[test]
fn sql_mounts_current_artifacts_and_downgrades_stale_authored_inputs() {
    let project = TestProject::new();
    compile_test_project(&project);
    let mounted = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select r.resource_id, r.source_plan_json, f.path from manifest_resources r join manifest_fields f using (resource_id) order by f.ordinal",
    ]);
    assert_eq!(mounted.exit_code, 0, "stderr: {}", mounted.stderr);
    let json = stderr_or_stdout_json(&mounted.stdout);
    assert_eq!(json["result"]["rows"].as_array().unwrap().len(), 2);
    assert_eq!(json["result"]["rows"][0][0], "local.events");
    let source_plan = json["result"]["rows"][0][1].as_str().unwrap();
    let _: serde_json::Value = serde_json::from_str(source_plan).unwrap();
    assert_eq!(json["result"]["rows"][0][2], "id");
    assert!(
        json["result"]["tables"]
            .as_array()
            .unwrap()
            .iter()
            .any(|table| table == "manifest_semantics")
    );

    fs::write(
        project.root.join("cdf/local/events.cdf.sql"),
        "this input changed after compilation",
    )
    .unwrap();

    let before = project_tree_snapshot(&project.root);
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select status from compilation_resources where resource_id = 'local.events'",
    ]);
    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["rows"], json!([["stale"]]));
    assert_eq!(project_tree_snapshot(&project.root), before);
}

#[test]
fn sql_keeps_system_tables_available_when_compilation_index_is_tampered() {
    let project = TestProject::new();
    compile_test_project(&project);
    let manifest_path = project.root.join(".cdf/manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["project_name"] = json!("tampered");
    let tampered = serde_json::to_vec_pretty(&manifest).unwrap();
    fs::write(&manifest_path, &tampered).unwrap();
    let before = project_tree_snapshot(&project.root);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select count(*) from packages",
    ]);
    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["rows"], json!([[0]]));
    assert_eq!(project_tree_snapshot(&project.root), before);
}

#[test]
fn sql_bounds_oversized_private_compilation_files_before_serving_facts() {
    let project = TestProject::new();
    let prepared = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "local.events",
    ]);
    assert_eq!(prepared.exit_code, 0, "stderr: {}", prepared.stderr);
    let prepared = stderr_or_stdout_json(&prepared.stdout);
    let artifact_path = prepared["result"]["resources"][0]["artifact_path"]
        .as_str()
        .unwrap();
    fs::OpenOptions::new()
        .write(true)
        .open(project.root.join(artifact_path))
        .unwrap()
        .set_len(64 * 1024 * 1024 + 1)
        .unwrap();

    let stale = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select status from compilation_resources where resource_id = 'local.events'",
    ]);
    assert_eq!(stale.exit_code, 0, "stderr: {}", stale.stderr);
    assert_eq!(
        stderr_or_stdout_json(&stale.stdout)["result"]["rows"],
        json!([["stale"]])
    );

    fs::OpenOptions::new()
        .write(true)
        .open(project.root.join(".cdf/manifest.json"))
        .unwrap()
        .set_len(16 * 1024 * 1024 + 1)
        .unwrap();
    let system = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select count(*) from packages",
    ]);
    assert_eq!(system.exit_code, 0, "stderr: {}", system.stderr);
    assert_eq!(
        stderr_or_stdout_json(&system.stdout)["result"]["rows"],
        json!([[0]])
    );
}

fn compile_test_project(project: &TestProject) {
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "compile",
        "local.events",
    ]);
    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
}

#[test]
fn sql_rejects_non_readonly_before_artifact_access() {
    let project = TestProject::new();
    let state_path = project.root.join(".cdf/state.db");
    let package_root = project.root.join(".cdf/packages");
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "delete from packages",
    ]);

    assert_eq!(result.exit_code, 2);
    let json = assert_json_error_code(&result, "CDF-SQL-QUERY");
    assert_eq!(json["error"]["kind"], "contract");
    assert!(!result.stderr.contains("delete from packages"));
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("read-only")
    );
    assert!(
        !state_path.exists(),
        "rejected sql must not create state DB"
    );
    assert!(
        !package_root.exists(),
        "rejected sql must not create package root"
    );
}
