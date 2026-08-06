use super::*;

fn single_plan(json: &serde_json::Value) -> &serde_json::Value {
    &json["result"]["resources"][0]["report"]
}

fn single_run(json: &serde_json::Value) -> &serde_json::Value {
    &json["result"]["resources"][0]["result"]
}

#[test]
fn first_use_plan_discovers_candidate_schema_without_project_writes() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));
    let before = project_tree_snapshot(&project.root);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "{}{}", result.stdout, result.stderr);
    assert_project_tree_unchanged(&project.root, &before);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = single_plan(&json);
    assert_eq!(report["schema_snapshot"]["outcome"], "inspection_only");
    assert_eq!(report["schema_snapshot"]["snapshot_written"], false);
    assert_eq!(report["schema_snapshot"]["lockfile_written"], false);
    assert_eq!(report["resource_schema"]["schema_source"], "discovered");
    assert_eq!(report["resource_schema"]["fields"][0]["name"], "vendor_id");
}

#[test]
fn first_use_run_commits_schema_and_executes_without_compile_folklore() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));

    let result = run_valid_run_resource(&project, "local.events");

    assert_eq!(result.exit_code, 0, "{}{}", result.stdout, result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = single_run(&json);
    assert_eq!(report["row_count"], 2);
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert!(project.root.join("cdf.lock").exists());
    assert!(project.root.join(".cdf/manifest.json").exists());
}

#[test]
fn locked_plan_reports_missing_snapshot_without_repairing_it() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));
    let compile = compile_resource(&project, "local.events");
    assert_eq!(compile.exit_code, 0, "{}{}", compile.stdout, compile.stderr);
    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    let snapshot_path = lock.resources["local.events"]
        .schema_snapshot
        .as_ref()
        .unwrap()
        .path
        .clone();
    fs::remove_file(project.root.join(&snapshot_path)).unwrap();
    let before = project_tree_snapshot(&project.root);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);

    assert_ne!(result.exit_code, 0);
    assert_project_tree_unchanged(&project.root, &before);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resources"][0]["status"], "failed");
    assert!(
        json["result"]["resources"][0]["error"]["message"]
            .as_str()
            .unwrap()
            .contains(&snapshot_path)
    );
}

#[test]
fn cold_rest_run_reuses_discovery_payload_for_execution() {
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

    assert_eq!(result.exit_code, 0, "{}{}", result.stdout, result.stderr);
    assert_secret_absent(&result, "rest-single-request-secret");
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(single_run(&json)["row_count"], 2);
}
