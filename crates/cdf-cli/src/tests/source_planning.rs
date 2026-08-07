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
    assert_eq!(report["schema_authority"]["status"], "proposed_first_use");
    assert_eq!(report["schema_authority"]["generation"], 1);
    assert_eq!(report["schema_authority"]["precondition"]["kind"], "absent");
    assert_eq!(report["schema_authority"]["drift"], "none");
    assert_eq!(
        report["admission"]["observation_strength"],
        "bounded_first_use_discovery"
    );
    assert_eq!(
        report["admission"]["dispositions"]["field"],
        "capture_variant"
    );
    assert_eq!(report["admission"]["source_schema_migrations"], 0);
}

#[test]
fn first_use_run_commits_schema_and_executes_without_compile_folklore() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));
    fs::write(project.root.join("cdf.lock"), "not schema authority").unwrap();

    let result = run_valid_run_resource(&project, "local.events");

    assert_eq!(result.exit_code, 0, "{}{}", result.stdout, result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = single_run(&json);
    assert_eq!(report["row_count"], 2);
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["schema_authority"]["status"], "established");
    assert_eq!(report["schema_authority"]["generation"], 1);
    assert_eq!(
        report["schema_authority"]["prepared_precondition"]["kind"],
        "absent"
    );
    assert!(project.root.join(".cdf/state.db").exists());
    assert_eq!(
        fs::read_to_string(project.root.join("cdf.lock")).unwrap(),
        "not schema authority"
    );
    assert!(!project.root.join(".cdf/manifest.json").exists());
}

#[test]
fn active_plan_enforces_exact_state_authority_without_project_writes() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    write_vendor_parquet(&project.root.join("data/vendors.parquet"));
    let compile = compile_resource(&project, "local.events");
    assert_eq!(compile.exit_code, 0, "{}{}", compile.stdout, compile.stderr);
    write_string_vendor_parquet(&project.root.join("data/vendors.parquet"));
    fs::write(project.root.join("cdf.lock"), "not schema authority").unwrap();
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
    assert_eq!(report["schema_authority"]["status"], "active");
    assert_eq!(report["schema_authority"]["generation"], 1);
    assert_eq!(report["schema_authority"]["precondition"]["kind"], "exact");
    assert_eq!(
        report["admission"]["observation_strength"],
        "runtime_stream"
    );
    assert_eq!(report["admission"]["source_schema_migrations"], 0);
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
