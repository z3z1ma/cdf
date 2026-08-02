use super::*;

#[test]
fn add_local_parquet_pins_schema_and_writes_resource_config() {
    let project = TestProject::new();
    write_vendor_parquet(&project.root.join("data/yellow.parquet"));

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "tlc.yellow",
        project.root.join("data/yellow.parquet").to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "add");
    assert_eq!(report["resource_id"], "tlc.yellow");
    assert_eq!(report["config_path"], "resources/tlc.toml");
    assert_eq!(report["location"], "data");
    assert_eq!(report["selection"], "yellow.parquet");
    assert_eq!(report["write_disposition"], "append");
    assert_eq!(report["schema_source"], "discovered");
    assert_eq!(report["next_command"], "cdf run tlc.yellow");
    assert_eq!(report["writes"]["resource_config"], true);
    assert_eq!(report["writes"]["project_config"], true);
    assert_eq!(report["writes"]["schema_snapshot"], true);
    assert_eq!(report["writes"]["lockfile"], true);
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);
    assert!(
        report["schema_snapshot_path"]
            .as_str()
            .unwrap()
            .starts_with(".cdf/schemas/tlc.yellow@sha256:")
    );
    assert_eq!(report["fields"][0]["name"], "vendor_id");
    assert_eq!(report["fields"][0]["source_name"], "VendorID");

    let resource_toml = fs::read_to_string(project.root.join("resources/tlc.toml")).unwrap();
    assert!(resource_toml.contains("[source.tlc]"));
    assert!(resource_toml.contains("kind = \"files\""));
    assert!(resource_toml.contains("root = \"data\""));
    assert!(resource_toml.contains("[resource.yellow]"));
    assert!(resource_toml.contains("glob = \"yellow.parquet\""));
    assert!(resource_toml.contains("format = \"parquet\""));
    assert!(resource_toml.contains("write_disposition = \"append\""));
    assert!(!resource_toml.contains("primary_key"));
    assert!(!resource_toml.contains("merge_key"));
    assert!(!resource_toml.contains("schema ="));

    let project_toml = fs::read_to_string(project.root.join("cdf.toml")).unwrap();
    assert!(project_toml.contains("[resources.\"tlc.yellow\"]"));
    assert!(project_toml.contains("source = \"resources/tlc.toml\""));
    assert!(
        project
            .root
            .join(report["schema_snapshot_path"].as_str().unwrap())
            .is_file()
    );

    let lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    let locked = lock.resources.get("tlc.yellow").unwrap();
    assert!(locked.schema_snapshot.is_some());
    assert_eq!(
        locked.schema_snapshot.as_ref().unwrap().path,
        report["schema_snapshot_path"].as_str().unwrap()
    );

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "tlc.yellow",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
}

#[test]
fn add_local_parquet_dry_run_writes_nothing() {
    let project = TestProject::new();
    write_vendor_parquet(&project.root.join("data/yellow.parquet"));

    let before_project = fs::read_to_string(project.root.join("cdf.toml")).unwrap();
    let before_tree = project_tree_snapshot(&project.root);
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "tlc.yellow",
        project.root.join("data/yellow.parquet").to_str().unwrap(),
        "--dry-run",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["writes"]["resource_config"], false);
    assert_eq!(report["writes"]["project_config"], false);
    assert_eq!(report["writes"]["schema_snapshot"], false);
    assert_eq!(report["writes"]["lockfile"], false);
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);
    assert_eq!(report["next_command"], "cdf run tlc.yellow");
    assert_eq!(
        fs::read_to_string(project.root.join("cdf.toml")).unwrap(),
        before_project
    );
    assert!(!project.root.join("resources/tlc.toml").exists());
    assert!(!project.root.join(".cdf/schemas").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    assert_project_tree_unchanged(&project.root, &before_tree);
}

#[test]
fn add_local_ndjson_uses_the_registered_file_driver_without_cli_format_wiring() {
    let project = TestProject::new();
    let source = project.root.join("data/events.ndjson");
    fs::write(
        &source,
        "{\"id\":1,\"occurred_at\":1783296000000000}\n{\"id\":2,\"occurred_at\":1783296000000001}\n",
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "ingest.events",
        source.to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let report = stderr_or_stdout_json(&result.stdout);
    assert_eq!(report["result"]["source_driver"], "files");
    assert_eq!(report["result"]["selection"], "events.ndjson");
    assert!(
        report["result"]["cursor_candidates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|candidate| candidate == "occurred_at")
    );
    let resource = fs::read_to_string(project.root.join("resources/ingest.toml")).unwrap();
    assert!(resource.contains("format = \"ndjson\""));
    assert!(resource.contains("write_disposition = \"append\""));
}

#[test]
fn add_rest_requires_explicit_selector_and_cursor_then_pins_sample() {
    let project = TestProject::new();
    let base_url =
        serve_json_once(r#"{"items":[{"id":1,"updated_at":1783296000000000,"name":"first"}]}"#);
    let endpoint = format!("{base_url}/items");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "api.items",
        &endpoint,
        "--option",
        "records=$.items",
        "--option",
        "cursor=updated_at",
        "--option",
        "cursor_param=since",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource_id"], "api.items");
    assert_eq!(json["result"]["selection"], "/items");
    assert_eq!(json["result"]["cursor"], "updated_at");
    assert_eq!(json["result"]["cursor_candidates"][0], "id");
    assert_eq!(json["result"]["writes"]["schema_snapshot"], true);
    let resource = fs::read_to_string(project.root.join("resources/api.toml")).unwrap();
    assert!(resource.contains("kind = \"rest\""));
    assert!(resource.contains(&format!("base_url = {base_url:?}")));
    assert!(resource.contains("path = \"/items\""));
    assert!(resource.contains("records = \"$.items\""));
    assert!(resource.contains("field = \"updated_at\""));
    assert!(resource.contains("param = \"since\""));
    assert!(resource.contains("ordering = \"best_effort\""));
    assert!(!resource.contains("schema ="));

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "api.items",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
}

#[test]
fn add_rest_rejects_partial_semantics_before_network_or_writes() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "api.items",
        "https://api.example.test/items",
        "--option",
        "records=$.items",
    ]);

    assert_eq!(result.exit_code, 2);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("requires options `records`, `cursor`, and `cursor_param` together")
    );
    assert!(!project.root.join("resources/api.toml").exists());
    assert!(!project.root.join("cdf.lock").exists());
}

#[test]
fn p2_s1_add_http_parquet_pins_and_runs_with_zero_typed_fields() {
    let project = TestProject::new();
    write_vendor_parquet(&project.root.join("data/yellow.parquet"));
    let parquet = fs::read(project.root.join("data/yellow.parquet")).unwrap();
    let (base_url, requests) = serve_parquet_file(parquet, 256);
    let url = format!("{base_url}/yellow.parquet");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "remote.yellow",
        &url,
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["resource_id"], "remote.yellow");
    assert_eq!(report["selection"], "yellow.parquet");
    assert_eq!(report["write_disposition"], "append");
    assert!(project.root.join("resources/remote.toml").is_file());
    let resource_toml = fs::read_to_string(project.root.join("resources/remote.toml")).unwrap();
    assert!(resource_toml.contains("[source.remote]"));
    assert!(resource_toml.contains("kind = \"files\""));
    assert!(resource_toml.contains("egress_allowlist = [\"127.0.0.1\"]"));
    assert!(resource_toml.contains("glob = \"yellow.parquet\""));
    assert!(!resource_toml.contains("primary_key"));
    assert!(!resource_toml.contains("merge_key"));

    let before_no_pin = project_tree_snapshot(&project.root);
    let no_pin = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "remote.yellow",
        "--no-pin",
    ]);
    assert_eq!(no_pin.exit_code, 0, "stderr: {}", no_pin.stderr);
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
        "remote.yellow",
    ]);
    assert_eq!(plan.exit_code, 0, "stderr: {}", plan.stderr);
    let plan_report = stderr_or_stdout_json(&plan.stdout);
    assert_eq!(
        plan_report["result"]["schema_snapshot"]["outcome"],
        "unchanged"
    );
    assert_eq!(
        plan_report["result"]["schema_snapshot"]["lockfile_written"],
        false
    );

    let run_result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "remote.yellow",
    ]);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_report = stderr_or_stdout_json(&run_result.stdout);
    assert_eq!(run_report["result"]["resource_id"], "remote.yellow");
    assert_eq!(run_report["result"]["row_count"], 2);
    assert!(
        run_package_dir(&project, &run_result)
            .join("manifest.json")
            .is_file()
    );

    let requests = requests.lock().unwrap();
    assert!(
        requests
            .iter()
            .any(|request| request.starts_with("HEAD /yellow.parquet HTTP/1.1")),
        "expected metadata HEAD request, got {requests:?}"
    );
    assert!(
        requests.iter().any(
            |request| request.starts_with("GET /yellow.parquet HTTP/1.1")
                && request.to_ascii_lowercase().contains("range: bytes=")
        ),
        "expected bounded range GET request, got {requests:?}"
    );
}

#[test]
fn p2_s2_http_month_glob_is_incremental_and_no_change_is_a_noop() {
    let project = TestProject::new();
    let files = BTreeMap::from([
        (
            "/yellow_tripdata_2024-01.parquet".to_owned(),
            vendor_parquet_bytes(&[1, 2]),
        ),
        (
            "/yellow_tripdata_2024-02.parquet".to_owned(),
            vendor_parquet_bytes(&[3, 4]),
        ),
    ]);
    let (base_url, files, _) = serve_parquet_files(files, 2_000);
    fs::write(
        project.root.join("resources/tlc.toml"),
        format!(
            r#"
[source.tlc]
kind = "files"
root = "{base_url}"
egress_allowlist = ["127.0.0.1"]

[resource.yellow]
glob = "yellow_tripdata_2024-*.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
"#
        ),
    )
    .unwrap();
    let project_path = project.root.join("cdf.toml");
    let mut project_toml = fs::read_to_string(&project_path).unwrap();
    project_toml.push_str("\n[resources.\"tlc.yellow\"]\nsource = \"resources/tlc.toml\"\n");
    fs::write(project_path, project_toml).unwrap();

    let plan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "tlc.yellow",
    ]);
    assert_eq!(plan.exit_code, 0, "{}", plan.stderr);
    let before_preview = project_tree_snapshot(&project.root);
    let preview = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "tlc.yellow",
    ]);
    assert_eq!(preview.exit_code, 0, "{}", preview.stderr);
    let preview_report = stderr_or_stdout_json(&preview.stdout);
    assert_eq!(preview_report["result"]["planned_partition_count"], 2);
    assert_eq!(preview_report["result"]["row_count"], 4);
    assert_eq!(project_tree_snapshot(&project.root), before_preview);

    let first = run_http_monthly_resource(&project, "p2-s2-first", "checkpoint-p2-s2-first");
    assert_eq!(first.exit_code, 0, "{}", first.stderr);
    let first_report = stderr_or_stdout_json(&first.stdout);
    assert_eq!(first_report["result"]["row_count"], 4);
    assert_eq!(
        first_report["result"]["file_manifest"]["changed_file_count"],
        2
    );

    let unchanged =
        run_http_monthly_resource(&project, "p2-s2-unchanged", "checkpoint-p2-s2-unchanged");
    assert_eq!(unchanged.exit_code, 0, "{}", unchanged.stderr);
    let unchanged_report = stderr_or_stdout_json(&unchanged.stdout);
    assert_eq!(unchanged_report["result"]["row_count"], 0);
    assert_eq!(
        unchanged_report["result"]["file_manifest"]["changed_file_count"],
        0
    );
    assert_eq!(unchanged_report["result"]["writes"]["package"], false);

    files.lock().unwrap().insert(
        "/yellow_tripdata_2024-03.parquet".to_owned(),
        vendor_parquet_bytes(&[5, 6]),
    );
    let third = run_http_monthly_resource(&project, "p2-s2-third", "checkpoint-p2-s2-third");
    assert_eq!(third.exit_code, 0, "{}", third.stderr);
    let third_report = stderr_or_stdout_json(&third.stdout);
    assert_eq!(third_report["result"]["row_count"], 2);
    assert_eq!(
        third_report["result"]["file_manifest"]["changed_file_count"],
        1
    );
    let connection = duckdb::Connection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let total: i64 = connection
        .query_row("SELECT COUNT(*) FROM yellow", [], |row| row.get(0))
        .unwrap();
    assert_eq!(total, 6);
}

fn run_http_monthly_resource(
    project: &TestProject,
    _package_id: &str,
    _checkpoint_id: &str,
) -> cdf_cli_core::output::InvocationResult {
    run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "tlc.yellow",
    ])
}

#[test]
fn add_rejects_signed_url_without_leaking_secret_query() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "remote.yellow",
        "https://data.example.test/yellow.parquet?sig=super-secret-token",
    ]);

    assert_ne!(result.exit_code, 0);
    assert_secret_absent(&result, "super-secret-token");
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["code"], "CDF-CLI-USAGE");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("<redacted>")
    );
    assert!(!project.root.join("resources/remote.toml").exists());
    assert!(!project.root.join("cdf.lock").exists());
    assert!(!project.root.join(".cdf/schemas").exists());
}
