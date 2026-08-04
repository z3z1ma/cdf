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
        "select p.package_hash, p.status, s.segment_id, c.checkpoint_id, c.status as checkpoint_status, r.receipt_id from packages p join package_segments s using (package_hash) join checkpoints c using (package_hash) join package_receipts r using (package_hash) order by p.package_id",
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
            "receipt_id"
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
fn compile_refresh_and_offline_compile_publish_one_typed_report() {
    let project = TestProject::new();
    let refresh = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "--refresh",
    ]);
    assert_eq!(refresh.exit_code, 0, "stderr: {}", refresh.stderr);
    let refresh = stderr_or_stdout_json(&refresh.stdout);
    assert_eq!(refresh["command"], "compile");
    assert_eq!(refresh["result"]["mode"], "refresh");
    assert_eq!(refresh["result"]["resources"], 1);
    assert_eq!(refresh["result"]["source_observations"], 0);
    assert_eq!(refresh["result"]["writes"]["manifest"], true);
    assert_eq!(refresh["result"]["writes"]["lockfile"], true);
    for external in ["destination", "state", "package", "receipt", "checkpoint"] {
        assert_eq!(refresh["result"]["writes"][external], false);
    }

    let lock_before = fs::read(project.root.join("cdf.lock")).unwrap();
    let offline = run(["cdf", "--json", "--project", project.root_str(), "compile"]);
    assert_eq!(offline.exit_code, 0, "stderr: {}", offline.stderr);
    let offline = stderr_or_stdout_json(&offline.stdout);
    assert_eq!(offline["result"]["mode"], "locked_offline");
    assert_eq!(offline["result"]["source_observations"], 0);
    assert_eq!(offline["result"]["writes"]["lockfile"], false);
    let offline_manifest = fs::read(project.root.join(".cdf/manifest.json")).unwrap();
    let repeated = run(["cdf", "--json", "--project", project.root_str(), "compile"]);
    assert_eq!(repeated.exit_code, 0, "stderr: {}", repeated.stderr);
    let repeated = stderr_or_stdout_json(&repeated.stdout);
    assert_eq!(repeated["result"]["writes"]["manifest"], false);
    assert_eq!(
        fs::read(project.root.join(".cdf/manifest.json")).unwrap(),
        offline_manifest
    );
    assert_eq!(
        fs::read(project.root.join("cdf.lock")).unwrap(),
        lock_before
    );
}

#[test]
fn offline_compile_requires_lock_and_names_refresh_without_publishing() {
    let project = TestProject::new();
    let result = run(["cdf", "--json", "--project", project.root_str(), "compile"]);

    assert_ne!(result.exit_code, 0);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("cdf compile --refresh")
    );
    assert!(!project.root.join(".cdf/manifest.json").exists());
    assert!(!project.root.join("cdf.lock").exists());
}

#[test]
fn compile_refresh_observes_only_refreshable_sources_and_publishes_schema_authority() {
    let project = TestProject::new();
    fs::write(
        project.root.join("resources/files.toml"),
        r#"
[source.local]
kind = "files"
root = "data"

[resource.events]
glob = "*.ndjson"
format = "ndjson"
primary_key = ["id"]
write_disposition = "append"
trust = "governed"
"#,
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "--refresh",
    ]);
    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["source_observations"], 1);
    assert!(
        json["result"]["writes"]["schema_artifacts"]
            .as_u64()
            .unwrap()
            >= 1
    );
    assert!(project.root.join(".cdf/schemas").is_dir());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    assert!(lock.resources["local.events"].schema_snapshot.is_some());
}

#[test]
fn sql_mounts_verified_manifest_tables_with_canonical_nested_json() {
    let project = TestProject::new();
    compile_test_project(&project);
    fs::write(
        project.root.join("resources/files.toml"),
        "this input changed after compilation",
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select r.resource_id, r.source_plan_json, f.path from manifest_resources r join manifest_fields f using (resource_id) order by f.ordinal",
    ]);
    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
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
}

#[test]
fn sql_fails_closed_on_tampered_manifest_without_republishing() {
    let project = TestProject::new();
    compile_test_project(&project);
    let manifest_path = project.root.join(".cdf/manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["header"]["normalizer"] = json!("tampered");
    let tampered = serde_json::to_vec_pretty(&manifest).unwrap();
    fs::write(&manifest_path, &tampered).unwrap();
    let before = project_tree_snapshot(&project.root);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "sql",
        "select * from manifest_project",
    ]);
    assert_ne!(result.exit_code, 0);
    assert!(result.stderr.contains("cdf compile"));
    assert_eq!(project_tree_snapshot(&project.root), before);
}

fn compile_test_project(project: &TestProject) {
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "compile",
        "--refresh",
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
