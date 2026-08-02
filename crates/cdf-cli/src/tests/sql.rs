use super::*;

#[test]
fn sql_mounts_checkpoint_package_and_receipt_tables_as_json_rows() {
    let project = TestProject::new();
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
            .contains("OK sql returned 1 row(s) from local system history")
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
