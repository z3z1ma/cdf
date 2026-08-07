use super::*;

#[test]
fn source_discovery_is_read_only_and_reports_inferred_fields() {
    let project = TestProject::new();
    let before = project_tree_snapshot(&project.root);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "discover",
        "source",
        "local",
        "events.ndjson",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["scope"], "source");
    assert_eq!(report["configured_source"], "local");
    assert_eq!(report["identity_space"], "file_path");
    assert_eq!(report["candidates"][0]["relation_id"], "events.ndjson");
    assert_eq!(report["candidates"][0]["schema_fields"], 2);
    assert_project_tree_unchanged(&project.root, &before);
}

#[test]
fn source_generation_uses_explicit_projection_and_retains_independent_success() {
    let project = TestProject::new();
    fs::write(
        project.root.join("data/more.ndjson"),
        "{\"id\":3,\"updated_at\":1783296060000001}\n",
    )
    .unwrap();
    fs::create_dir_all(project.root.join("cdf/raw")).unwrap();
    fs::write(project.root.join("cdf/raw/events.cdf.sql"), "SELECT 1;\n").unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "discover",
        "source",
        "local",
        "*.ndjson",
        "--generate",
        "--namespace",
        "raw",
    ]);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let effects = json["result"]["effects"].as_array().unwrap();
    assert!(effects.iter().any(|effect| {
        effect["relation_id"] == "events.ndjson" && effect["outcome"] == "conflicted"
    }));
    assert!(effects.iter().any(|effect| {
        effect["relation_id"] == "more.ndjson" && effect["outcome"] == "created"
    }));
    assert_eq!(
        fs::read_to_string(project.root.join("cdf/raw/events.cdf.sql")).unwrap(),
        "SELECT 1;\n"
    );
    let generated = fs::read_to_string(project.root.join("cdf/raw/more.cdf.sql")).unwrap();
    assert!(generated.starts_with("SELECT\n  \"id\",\n  \"updated_at\"\nFROM upstream("));
    assert!(generated.contains("source => 'local'"));
    assert!(!generated.contains("RESOURCE"));
    assert!(!project.root.join(".cdf/manifest.json").exists());
}

#[test]
fn resource_discovery_uses_temporary_authority_and_writes_only_explicit_out() {
    let project = TestProject::new();
    let out = project.root.join("evidence/discovery.json");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "discover",
        "resource",
        "local.events",
        "--out",
        out.to_str().unwrap(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        json["result"]["resources"][0]["resource_id"],
        "local.events"
    );
    assert_eq!(
        json["result"]["resources"][0]["schema"]["fields"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(json["result"]["artifact"]["outcome"], "created");
    assert!(out.exists());
    let artifact_bytes = fs::read(&out).unwrap();
    let unchanged = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "discover",
        "resource",
        "local.events",
        "--out",
        out.to_str().unwrap(),
    ]);
    assert_eq!(unchanged.exit_code, 0, "stderr: {}", unchanged.stderr);
    assert_eq!(
        stderr_or_stdout_json(&unchanged.stdout)["result"]["artifact"]["outcome"],
        "unchanged"
    );
    assert_eq!(fs::read(&out).unwrap(), artifact_bytes);

    fs::write(&out, "different\n").unwrap();
    let conflict = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "discover",
        "resource",
        "local.events",
        "--out",
        out.to_str().unwrap(),
    ]);
    assert_ne!(conflict.exit_code, 0);
    assert_eq!(fs::read_to_string(&out).unwrap(), "different\n");
    assert!(!project.root.join(".cdf/manifest.json").exists());
    assert!(!project.root.join(".cdf/schemas").exists());
}

#[test]
fn generation_reports_select_star_fallback_when_schema_is_unavailable() {
    let project = TestProject::new();
    fs::write(project.root.join("data/empty.ndjson"), "").unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "discover",
        "source",
        "local",
        "empty.ndjson",
        "--generate",
        "--namespace",
        "fallback",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let report = stderr_or_stdout_json(&result.stdout);
    assert_eq!(report["result"]["effects"][0]["projection_fallback"], true);
    assert!(report["result"]["candidates"][0]["schema_error"].is_string());
    let generated = fs::read_to_string(project.root.join("cdf/fallback/empty.cdf.sql")).unwrap();
    assert!(generated.starts_with("SELECT *\nFROM upstream("));
}

#[test]
fn resource_discovery_retains_success_when_another_selected_resource_fails() {
    let project = TestProject::new();
    fs::create_dir_all(project.root.join("cdf/bad")).unwrap();
    fs::write(
        project.root.join("cdf/bad/oops.cdf.sql"),
        "SELECT FROM intentionally invalid\n",
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "discover",
        "resource",
        "local.events",
        "bad.oops",
    ]);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let report = stderr_or_stdout_json(&result.stdout);
    let resources = report["result"]["resources"].as_array().unwrap();
    assert!(resources.iter().any(|resource| {
        resource["resource_id"] == "local.events" && resource["status"] == "discovered"
    }));
    assert!(resources.iter().any(|resource| {
        resource["resource_id"] == "bad.oops" && resource["status"] == "failed"
    }));
}

#[test]
fn sqlite_catalog_discovery_lists_tables_with_explicit_schema_projection() {
    let project = TestProject::new();
    let database_path = project.root.join("warehouse.sqlite");
    let database = rusqlite::Connection::open(&database_path).unwrap();
    database
        .execute_batch(
            "CREATE TABLE orders (order_id INTEGER NOT NULL, customer_name TEXT);\n\
             CREATE TABLE ignored (value REAL);",
        )
        .unwrap();
    drop(database);
    let mut config = fs::read_to_string(project.root.join("cdf.toml")).unwrap();
    config.push_str(
        "\n[sources.warehouse]\n\
         type = \"sqlite\"\n\
         location = \"sqlite://warehouse.sqlite\"\n",
    );
    fs::write(project.root.join("cdf.toml"), config).unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "discover",
        "source",
        "warehouse",
        "orders",
        "--generate",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let report = stderr_or_stdout_json(&result.stdout);
    assert_eq!(report["result"]["identity_space"], "sqlite_table");
    assert_eq!(report["result"]["candidates"][0]["schema_fields"], 2);
    let generated = fs::read_to_string(project.root.join("cdf/warehouse/orders.cdf.sql")).unwrap();
    assert!(generated.starts_with("SELECT\n  \"order_id\",\n  \"customer_name\"\nFROM upstream("));
    assert!(generated.contains("table => 'orders'"));
}
