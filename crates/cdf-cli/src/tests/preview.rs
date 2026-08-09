use super::*;

#[test]
fn preview_reads_single_ndjson_file_without_creating_runtime_artifacts() {
    let project = TestProject::new();
    let package_root = project.root.join(".cdf/packages");
    let state_path = project.root.join(".cdf/state.db");
    let duckdb_path = project.root.join(".cdf/dev.duckdb");
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(
        !package_root.exists(),
        "preview must not create the package root"
    );
    assert!(!state_path.exists(), "preview must not create state");
    assert!(
        !duckdb_path.exists(),
        "preview must not create destination data"
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["ok"], true);
    assert_eq!(json["command"], "preview");
    assert_eq!(json["result"]["resource"], "local.events");
    assert_eq!(json["result"]["partition"], "files");
    assert_eq!(json["result"]["resource_id"], "local.events");
    assert_eq!(json["result"]["partition_id"], "files");
    assert_eq!(json["result"]["row_count"], 2);
    assert!(
        json["result"]["batch"]
            .as_str()
            .unwrap()
            .starts_with("local-events-files-")
    );
    assert_eq!(json["result"]["batch"], json["result"]["batch_id"]);
    assert!(
        json["result"]["batch_id"]
            .as_str()
            .unwrap()
            .starts_with("local-events-files-")
    );
    assert!(json["result"]["byte_count"].as_u64().unwrap() > 0);
    assert_eq!(json["result"]["write_effects"]["package"], false);
    assert_eq!(json["result"]["write_effects"]["destination"], false);
    assert_eq!(json["result"]["write_effects"]["checkpoint"], false);
    assert_eq!(json["result"]["writes"]["package"], false);
    assert_eq!(json["result"]["writes"]["destination"], false);
    assert_eq!(json["result"]["writes"]["checkpoint"], false);

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(
        human
            .stdout
            .contains("OK Previewed 2 rows from local.events")
    );
    assert!(human.stdout.contains("Summary"));
    assert!(human.stdout.contains("writes      none"));
    assert!(!human.stdout.contains("payload partitions opened"));
    assert!(human.stdout.contains("Next: cdf plan local.events"));
    assert_no_preview_writes(&project);
}

#[test]
fn preview_succeeds_for_csv_json_parquet_and_arrow_ipc_file_resources() {
    for format in ["csv", "json", "parquet", "arrow_ipc"] {
        let project = TestProject::new();
        write_format_fixture(&project, format);

        let result = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "preview",
            "local.events",
        ]);

        assert_eq!(
            result.exit_code, 0,
            "format {format} stderr: {}",
            result.stderr
        );
        let json = stderr_or_stdout_json(&result.stdout);
        assert_eq!(json["result"]["resource"], "local.events");
        assert_eq!(json["result"]["partition"], "files");
        assert_eq!(json["result"]["resource_id"], "local.events");
        assert_eq!(json["result"]["row_count"], 2, "format {format}");
        assert_no_preview_writes(&project);
    }
}

#[test]
fn preview_rest_resource_uses_local_http_runtime_without_writes() {
    let project = TestProject::new();
    fs::write(project.root.join("rest-token"), "rest-preview-token\n").unwrap();
    let (base_url, request) = serve_json_once_capturing_request(
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
    fs::write(
        project.root.join("cdf/api/items.cdf.sql"),
        rest_resource_sql("exact"),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "api.items",
        "--filter",
        "updated_at >= 20",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "rest-preview-token");
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource"], "api.items");
    assert_eq!(json["result"]["partition"], "rest");
    assert_eq!(json["result"]["row_count"], 1);
    let request = request.lock().unwrap().clone().unwrap();
    assert!(request.starts_with("GET /items HTTP/1.1"));
}

#[test]
fn preview_postgres_table_resource_uses_postgres_runtime_without_writes() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("preview_source_orders");
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
        "postgresql://cdf:source-postgres-preview-secret@",
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

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "warehouse.orders",
        "--filter",
        "id > 1",
        "--limit",
        "1",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &source_dsn);
    assert_secret_absent(&result, "source-postgres-preview-secret");
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource"], "warehouse.orders");
    assert_eq!(json["result"]["partition"], "sql");
    assert_eq!(json["result"]["row_count"], 1);
}

#[test]
fn preview_postgres_query_resource_requires_credentials_without_writes() {
    let project = TestProject::new();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/postgres-dsn"),
    );
    fs::write(
        project.root.join("cdf/warehouse/orders.cdf.sql"),
        r#"RESOURCE
DISPOSITION APPEND
TRUST GOVERNED
EXECUTION BOUNDED
AS
SELECT *
FROM upstream(source => 'warehouse', query => 'SELECT * FROM public.orders');
"#,
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "warehouse.orders",
    ]);

    assert_ne!(result.exit_code, 0);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "auth");
    assert_eq!(json["error"]["message"], "file secret is not resolvable");
    assert!(!result.stderr.contains("secret://file/postgres-dsn"));
}

#[test]
fn preview_file_filter_runs_through_shared_engine_without_writes() {
    let project = TestProject::new();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
        "--filter",
        "id > 1",
    ]);

    assert_eq!(result.exit_code, 0, "{}", result.stderr);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 1);
    assert_eq!(json["result"]["planned_partition_count"], 1);
    assert_eq!(json["result"]["payload_opened_partition_count"], 1);
    assert_eq!(json["result"]["inspected_batch_count"], 1);
}

#[test]
fn preview_zero_match_file_glob_fails_closed_without_writes() {
    let project = TestProject::new();
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 5);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "data");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("matched no files"),
        "{}",
        result.stderr
    );
}

#[test]
fn preview_missing_file_source_root_fails_as_zero_match_without_writes() {
    let project = TestProject::new();
    fs::remove_dir_all(project.root.join("data")).unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 5);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "data");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("matched no files"),
        "{}",
        result.stderr
    );
}

#[test]
fn preview_missing_intermediate_literal_directory_fails_as_zero_match_without_writes() {
    let project = TestProject::new();
    write_resource_glob(&project, "missing/events.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 5);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "data");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("matched no files")
    );
}

#[test]
fn preview_multi_match_file_glob_reads_every_sorted_match_without_writes() {
    let project = TestProject::new();
    fs::write(
        project.root.join("data/zzz-events.ndjson"),
        "{\"id\":3,\"updated_at\":1783296120000000}\n",
    )
    .unwrap();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["resource"], "local.events");
    assert!(
        json["result"]["partition"]
            .as_str()
            .unwrap()
            .starts_with("file-")
    );
    assert_eq!(json["result"]["planned_partition_count"], 2);
    assert_eq!(json["result"]["payload_opened_partition_count"], 2);
    assert_eq!(json["result"]["inspected_partition_count"], 2);
    assert_eq!(json["result"]["row_count"], 3);
}

#[test]
fn preview_wildcard_directory_glob_requires_component_match() {
    let project = TestProject::new();
    fs::create_dir_all(project.root.join("data/match-a")).unwrap();
    fs::create_dir_all(project.root.join("data/other")).unwrap();
    fs::write(
        project.root.join("data/match-a/events.ndjson"),
        "{\"id\":1,\"updated_at\":1783296000000000}\n",
    )
    .unwrap();
    fs::write(
        project.root.join("data/other/events.ndjson"),
        "{\"id\":2,\"updated_at\":1783296060000000}\n",
    )
    .unwrap();
    write_resource_glob(&project, "match-*/events.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 1);
    assert_no_preview_writes(&project);
}

#[test]
fn preview_question_mark_glob_matches_exactly_one_character() {
    let project = TestProject::new();
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    fs::write(
        project.root.join("data/event1.ndjson"),
        "{\"id\":1,\"updated_at\":1783296000000000}\n",
    )
    .unwrap();
    fs::write(
        project.root.join("data/event12.ndjson"),
        "{\"id\":2,\"updated_at\":1783296060000000}\n",
    )
    .unwrap();
    write_resource_glob(&project, "event?.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 1);
    assert_no_preview_writes(&project);
}

#[test]
fn preview_double_star_glob_descends_into_physical_nested_directories() {
    let project = TestProject::new();
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    fs::create_dir_all(project.root.join("data/nested")).unwrap();
    fs::write(
        project.root.join("data/nested/events.ndjson"),
        "{\"id\":1,\"updated_at\":1783296000000000}\n",
    )
    .unwrap();
    write_resource_glob(&project, "**/*.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 1);
    assert_no_preview_writes(&project);
}

#[cfg(unix)]
#[test]
fn preview_double_star_glob_ignores_symlink_directory_loops() {
    let project = TestProject::new();
    std::os::unix::fs::symlink(project.root.join("data"), project.root.join("data/loop")).unwrap();
    write_resource_glob(&project, "**/*.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 2);
    assert_no_preview_writes(&project);
}

#[cfg(unix)]
#[test]
fn preview_wildcard_directory_glob_ignores_symlink_directories() {
    let project = TestProject::new();
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    fs::create_dir_all(project.root.join("data/real")).unwrap();
    fs::write(
        project.root.join("data/real/events.ndjson"),
        "{\"id\":1,\"updated_at\":1783296000000000}\n",
    )
    .unwrap();
    std::os::unix::fs::symlink(
        project.root.join("data/real"),
        project.root.join("data/alias"),
    )
    .unwrap();
    write_resource_glob(&project, "*/events.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["row_count"], 1);
    assert_no_preview_writes(&project);
}

#[cfg(unix)]
#[test]
fn preview_unreadable_glob_directory_reports_directory_read_error() {
    use std::os::unix::fs::PermissionsExt;

    let project = TestProject::new();
    let private = project.root.join("data/private");
    fs::create_dir_all(&private).unwrap();
    fs::set_permissions(&private, fs::Permissions::from_mode(0o000)).unwrap();
    write_resource_glob(&project, "private/*.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(result.exit_code, 5);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("read file source directory")
    );
}

#[cfg(unix)]
#[test]
fn preview_inaccessible_literal_child_reports_path_inspection_error() {
    use std::os::unix::fs::PermissionsExt;

    let project = TestProject::new();
    let private = project.root.join("data/private");
    fs::create_dir_all(&private).unwrap();
    fs::set_permissions(&private, fs::Permissions::from_mode(0o000)).unwrap();
    write_resource_glob(&project, "private/child/*.ndjson");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "preview",
        "local.events",
    ]);

    fs::set_permissions(&private, fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(result.exit_code, 5);
    assert_no_preview_writes(&project);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("inspect file source path")
    );
}
