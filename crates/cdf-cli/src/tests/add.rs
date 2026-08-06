use super::*;

#[test]
fn add_local_parquet_writes_query_resource_and_shared_source_configuration() {
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
    assert_eq!(report["namespace"], "tlc");
    assert_eq!(report["resource"], "yellow");
    assert_eq!(report["configured_source"], "tlc");
    assert_eq!(report["source_driver"], "files");
    assert_eq!(report["resource_path"], "cdf/tlc/yellow.cdf.sql");
    assert_eq!(report["location"], "data");
    assert_eq!(report["selection"], "yellow.parquet");
    assert_eq!(report["policy"], "project defaults");
    assert_eq!(report["writes"]["resource_sql"], true);
    assert_eq!(report["writes"]["configured_source"], true);
    assert_eq!(report["writes"]["lockfile"], false);
    assert_eq!(report["next_command"], "cdf plan tlc.yellow");

    let sql = fs::read_to_string(project.root.join("cdf/tlc/yellow.cdf.sql")).unwrap();
    assert!(!sql.contains("DISPOSITION"));
    assert!(sql.starts_with("SELECT *\nFROM upstream("));
    assert!(sql.contains("source => 'tlc'"));
    assert!(sql.contains("glob => 'yellow.parquet'"));
    assert!(sql.contains("format => 'parquet'"));

    let project_toml = fs::read_to_string(project.root.join("cdf.toml")).unwrap();
    assert!(project_toml.contains("[sources.tlc]"));
    assert!(project_toml.contains("type = \"files\""));
    assert!(project_toml.contains("root = \"data\""));
    assert!(!project.root.join("cdf.lock").exists());

    let compile = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "compile",
        "tlc.yellow",
    ]);
    assert_eq!(compile.exit_code, 0, "stderr: {}", compile.stderr);
}

#[test]
fn add_reuses_matching_configured_source_without_rewriting_project_configuration() {
    let project = TestProject::new();
    fs::write(
        project.root.join("data/more.ndjson"),
        "{\"id\":3,\"updated_at\":1783296060000001}\n",
    )
    .unwrap();
    let before = fs::read_to_string(project.root.join("cdf.toml")).unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "local.more",
        project.root.join("data/more.ndjson").to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let report = stderr_or_stdout_json(&result.stdout);
    assert_eq!(report["result"]["writes"]["resource_sql"], true);
    assert_eq!(report["result"]["writes"]["configured_source"], false);
    assert_eq!(
        fs::read_to_string(project.root.join("cdf.toml")).unwrap(),
        before
    );
    let sql = fs::read_to_string(project.root.join("cdf/local/more.cdf.sql")).unwrap();
    assert!(sql.contains("source => 'local'"));
    assert!(sql.contains("glob => 'more.ndjson'"));
}

#[test]
fn add_keeps_resource_namespace_distinct_from_explicit_configured_source() {
    let project = TestProject::new();
    fs::write(
        project.root.join("data/more.ndjson"),
        "{\"id\":3,\"updated_at\":1783296060000001}\n",
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "raw.more",
        project.root.join("data/more.ndjson").to_str().unwrap(),
        "--source",
        "local",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let report = stderr_or_stdout_json(&result.stdout);
    assert_eq!(report["result"]["namespace"], "raw");
    assert_eq!(report["result"]["configured_source"], "local");
    let sql = fs::read_to_string(project.root.join("cdf/raw/more.cdf.sql")).unwrap();
    assert!(sql.contains("source => 'local'"));
    assert!(!sql.contains("source => 'raw'"));
}

#[test]
fn add_does_not_compile_unrelated_resources() {
    let project = TestProject::new();
    fs::write(
        project.root.join("cdf/local/events.cdf.sql"),
        "this is intentionally invalid project SQL\n",
    )
    .unwrap();
    fs::write(
        project.root.join("data/more.ndjson"),
        "{\"id\":3,\"updated_at\":1783296060000001}\n",
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "add",
        "raw.more",
        project.root.join("data/more.ndjson").to_str().unwrap(),
        "--source",
        "local",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(project.root.join("cdf/raw/more.cdf.sql").exists());
}

#[test]
fn add_local_parquet_dry_run_writes_nothing() {
    let project = TestProject::new();
    write_vendor_parquet(&project.root.join("data/yellow.parquet"));
    let before = project_tree_snapshot(&project.root);

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
    let report = stderr_or_stdout_json(&result.stdout);
    assert_eq!(report["result"]["writes"]["resource_sql"], false);
    assert_eq!(report["result"]["writes"]["configured_source"], false);
    assert_eq!(report["result"]["writes"]["private_source_state"], false);
    assert_eq!(report["result"]["writes"]["lockfile"], false);
    assert!(!project.root.join("cdf/tlc/yellow.cdf.sql").exists());
    assert_project_tree_unchanged(&project.root, &before);
}

#[test]
fn add_rest_requires_complete_resource_selection_without_writes() {
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
    assert!(!project.root.join("cdf/api/items.cdf.sql").exists());
    assert!(
        !fs::read_to_string(project.root.join("cdf.toml"))
            .unwrap()
            .contains("[sources.api]")
    );
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
    assert!(!project.root.join("cdf/remote/yellow.cdf.sql").exists());
}
