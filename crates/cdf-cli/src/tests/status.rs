use super::*;

#[test]
fn status_ignores_non_serving_freshness_resources() {
    let project = TestProject::new();
    write_status_resource(&project, "governed", "1h");
    let state_path = project.root.join(".cdf/state.db");
    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(
        !state_path.exists(),
        "status must not create state DB when nothing is evaluable"
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "status");
    assert_eq!(json["result"]["summary"]["total"], 0);
    assert!(
        json["result"]["freshness_resources"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let human = run(["cdf", "--project", project.root_str(), "status"]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(
        human
            .stdout
            .contains("OK no freshness SLO resources to evaluate")
    );
    assert!(human.stdout.contains("Freshness"));
    assert!(human.stdout.contains("total          0"));
    assert!(human.stdout.contains("Next: cdf doctor"));
}

#[test]
fn status_reports_fresh_committed_head() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    commit_status_head(
        &project,
        "pipeline-1",
        "checkpoint-status-fresh",
        "package-status-fresh",
        "receipt-status-fresh",
        now_ms_for_test(),
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["fresh"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["resource_id"], "local.events");
    assert_eq!(resource["trust_level"], "serving");
    assert_eq!(resource["state_scope"], json!({ "kind": "resource" }));
    assert_eq!(resource["max_age_ms"], 3_600_000);
    assert_eq!(resource["freshness_state"], "fresh");
    assert_eq!(resource["receipt_freshness"]["state"], "missing_run_ledger");
    assert_eq!(
        resource["receipt_freshness"]["source"],
        "checkpoint_committed_head"
    );
    assert_eq!(
        resource["checkpoint"]["checkpoint_id"],
        "checkpoint-status-fresh"
    );
    assert_eq!(resource["checkpoint"]["pipeline_id"], "pipeline-1");
    assert!(resource["age_ms"].as_u64().unwrap() <= 3_600_000);
    let human = run(["cdf", "--project", project.root_str(), "status"]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(
        human
            .stdout
            .contains("OK freshness SLO status fresh: 1 resource(s)")
    );
    assert!(human.stdout.contains("resource      state  age"));
    assert!(human.stdout.contains("local.events  fresh"));
    assert!(human.stdout.contains("Next: cdf doctor"));
}

#[test]
fn status_reports_stale_committed_head() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1ms");
    commit_status_head(
        &project,
        "pipeline-1",
        "checkpoint-status-stale",
        "package-status-stale",
        "receipt-status-stale",
        1,
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["stale"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "stale");
    assert!(resource["age_ms"].as_u64().unwrap() > 1);
    let human = run(["cdf", "--project", project.root_str(), "status"]);
    assert_eq!(human.exit_code, 1, "stderr: {}", human.stderr);
    assert!(
        human
            .stdout
            .contains("ERR freshness SLO breach: 1 stale, 0 fresh, 0 non-evaluable")
    );
    assert!(human.stdout.contains("local.events  stale"));
    assert!(human.stdout.contains("Next: cdf doctor"));
}

#[test]
fn status_clamps_future_committed_head_age_to_zero() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1ms");
    commit_status_head(
        &project,
        "pipeline-1",
        "checkpoint-status-future",
        "package-status-future",
        "receipt-status-future",
        now_ms_for_test() + 3_600_000,
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "fresh");
    assert_eq!(resource["age_ms"], 0);
}

#[test]
fn status_reports_elapsed_age_from_committed_timestamp() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    commit_status_head(
        &project,
        "pipeline-1",
        "checkpoint-status-age",
        "package-status-age",
        "receipt-status-age",
        now_ms_for_test() - 120_000,
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let age_ms = json["result"]["freshness_resources"][0]["age_ms"]
        .as_u64()
        .unwrap();
    assert!(
        (120_000..180_000).contains(&age_ms),
        "unexpected age_ms: {age_ms}"
    );
}

#[test]
fn status_reports_missing_state_as_non_evaluable() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    let state_path = project.root.join(".cdf/state.db");
    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 78, "stderr: {}", result.stderr);
    assert!(
        !state_path.exists(),
        "status must not create missing state DB"
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["non_evaluable"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "non_evaluable");
    assert_eq!(resource["non_evaluable_reason"], "state_database_missing");
    let human = run(["cdf", "--project", project.root_str(), "status"]);
    assert_eq!(human.exit_code, 78, "stderr: {}", human.stderr);
    assert!(
        human
            .stdout
            .contains("WARN freshness SLO status non-evaluable: 1 resource(s), 0 fresh")
    );
    assert!(human.stdout.contains("local.events  non-evaluable"));
    assert!(human.stdout.contains("state_database_missing"));
}

#[test]
fn status_reports_missing_checkpoint_table_as_non_evaluable() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    fs::create_dir_all(project.root.join(".cdf")).unwrap();
    Connection::open(project.root.join(".cdf/state.db")).unwrap();

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 78, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["non_evaluable"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "non_evaluable");
    assert_eq!(resource["non_evaluable_reason"], "checkpoint_table_missing");
}

#[test]
fn status_reports_missing_run_ledger_as_non_evaluable_without_committed_head() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    initialize_status_state(&project);

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 78, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["non_evaluable"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "non_evaluable");
    assert_eq!(resource["non_evaluable_reason"], "run_ledger_missing");
    assert_eq!(resource["receipt_freshness"]["state"], "missing_run_ledger");
    assert_eq!(resource["receipt_freshness"]["source"], "run_ledger");
}

#[test]
fn status_reports_ambiguous_multiple_pipeline_heads_as_non_evaluable() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    let committed_at_ms = now_ms_for_test();
    commit_status_head(
        &project,
        "pipeline-1",
        "checkpoint-status-ambiguous-1",
        "package-status-ambiguous-1",
        "receipt-status-ambiguous-1",
        committed_at_ms,
    );
    commit_status_head(
        &project,
        "pipeline-2",
        "checkpoint-status-ambiguous-2",
        "package-status-ambiguous-2",
        "receipt-status-ambiguous-2",
        committed_at_ms,
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 78, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["non_evaluable"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "non_evaluable");
    assert_eq!(
        resource["non_evaluable_reason"],
        "ambiguous_committed_heads"
    );
    assert_eq!(resource["matching_committed_heads"], 2);
}

#[test]
fn status_reports_fresh_receipt_only_runtime_fact() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    initialize_status_state(&project);
    let committed_at_ms = now_ms_for_test();
    let (package_dir, package_hash) = write_status_package_receipt(
        &project,
        "pkg-status-receipt-fresh",
        "receipt-status-runtime-fresh",
        committed_at_ms,
    );
    record_status_receipt_event(
        &project,
        "run-status-receipt-fresh",
        &package_dir,
        &package_hash,
        "receipt-status-runtime-fresh",
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(
        result.exit_code, 0,
        "stdout: {} stderr: {}",
        result.stdout, result.stderr
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["fresh"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "fresh");
    assert!(resource["checkpoint"].is_null());
    assert_eq!(resource["receipt_freshness"]["state"], "fresh_receipt");
    assert_eq!(resource["receipt_freshness"]["source"], "package_receipt");
    assert_eq!(
        resource["receipt_freshness"]["receipt_id"],
        "receipt-status-runtime-fresh"
    );
    assert!(resource["age_ms"].as_u64().unwrap() <= 3_600_000);
}

#[test]
fn status_reports_stale_receipt_only_runtime_fact() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1ms");
    initialize_status_state(&project);
    let (package_dir, package_hash) =
        write_status_package_receipt(&project, "pkg-status-receipt-stale", "receipt-stale", 1);
    record_status_receipt_event(
        &project,
        "run-status-receipt-stale",
        &package_dir,
        &package_hash,
        "receipt-stale",
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(
        result.exit_code, 1,
        "stdout: {} stderr: {}",
        result.stdout, result.stderr
    );
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["stale"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "stale");
    assert_eq!(resource["receipt_freshness"]["state"], "stale_receipt");
    assert_eq!(resource["receipt_freshness"]["source"], "package_receipt");
}

#[test]
fn status_reports_missing_receipt_artifact_as_non_evaluable() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    initialize_status_state(&project);
    let (package_dir, package_hash) = write_status_package(&project, "pkg-status-missing-receipt");
    record_status_receipt_event(
        &project,
        "run-status-missing-receipt",
        &package_dir,
        &package_hash,
        "receipt-status-missing",
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 78, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["non_evaluable"], 1);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "non_evaluable");
    assert_eq!(resource["non_evaluable_reason"], "receipt_missing");
    assert_eq!(resource["receipt_freshness"]["state"], "missing_receipt");
    assert_eq!(
        resource["receipt_freshness"]["source"],
        "run_ledger_receipt"
    );
}

#[test]
fn status_committed_head_timestamp_takes_precedence_over_package_receipt() {
    let project = TestProject::new();
    write_status_resource(&project, "serving", "1h");
    let checkpoint_committed_at_ms = now_ms_for_test();
    let (package_dir, package_hash) = write_status_package_receipt(
        &project,
        "pkg-status-precedence",
        "receipt-status-precedence",
        1,
    );
    commit_status_head(
        &project,
        "pipeline-1",
        "checkpoint-status-precedence",
        &package_hash,
        "receipt-status-precedence",
        checkpoint_committed_at_ms,
    );
    record_status_receipt_event(
        &project,
        "run-status-precedence",
        &package_dir,
        &package_hash,
        "receipt-status-precedence",
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let resource = &json["result"]["freshness_resources"][0];
    assert_eq!(resource["freshness_state"], "fresh");
    assert!(resource["age_ms"].as_u64().unwrap() <= 3_600_000);
    assert_eq!(resource["receipt_freshness"]["state"], "corrupt_receipt");
    assert_eq!(resource["receipt_freshness"]["source"], "package_receipt");
    assert_eq!(
        resource["receipt_freshness"]["observed_at_ms"],
        checkpoint_committed_at_ms
    );
    assert_eq!(
        resource["receipt_freshness"]["package_receipt_committed_at_ms"],
        1
    );
}
