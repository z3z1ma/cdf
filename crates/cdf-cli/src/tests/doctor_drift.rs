use super::*;

#[test]
fn doctor_passes_clean_duckdb_ledger_mirror_drift_check() {
    let project = TestProject::new();
    create_duckdb_doctor_fixture(&project, DoctorDriftFixtureMode::Clean);
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "doctor",
        "all",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let drift = named_check(&json, "ledger_destination_drift");
    assert_eq!(drift["status"], "passed");
    assert_eq!(drift["details"]["counts"]["ledger_checkpoints"], 1);
    assert_eq!(drift["details"]["counts"]["expected_loads"], 1);
    assert_eq!(drift["details"]["counts"]["expected_state_rows"], 1);
    assert_eq!(drift["details"]["counts"]["mirror_loads"], 1);
    assert_eq!(drift["details"]["counts"]["mirror_state_rows"], 1);
    assert_eq!(drift["details"]["examples"].as_array().unwrap().len(), 0);
}

#[test]
fn doctor_accepts_historical_mirror_rows_from_multiple_committed_runs() {
    let project = TestProject::new();
    let first = run_valid_run_args(&project);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    fs::write(
        project.root.join("data/events.ndjson"),
        concat!(
            "{\"id\":1,\"updated_at\":1783296000000000}\n",
            "{\"id\":2,\"updated_at\":1783296060000000}\n",
            "{\"id\":3,\"updated_at\":1783296120000000}\n"
        ),
    )
    .unwrap();
    let second = run_valid_run_args(&project);
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "doctor",
        "resource",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let drift = named_check(&json, "ledger_destination_drift");
    assert_eq!(drift["status"], "passed");
    assert_eq!(drift["details"]["counts"]["ledger_checkpoints"], 2);
    assert_eq!(drift["details"]["counts"]["expected_loads"], 2);
    assert_eq!(drift["details"]["counts"]["expected_state_rows"], 2);
    assert_eq!(drift["details"]["counts"]["mirror_loads"], 2);
    assert_eq!(drift["details"]["counts"]["mirror_state_rows"], 2);
}

#[test]
fn doctor_fails_on_duckdb_state_mirror_drift() {
    let project = TestProject::new();
    create_duckdb_doctor_fixture(&project, DoctorDriftFixtureMode::StatePositionDrift);
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "doctor",
        "all",
    ]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let drift = named_check(&json, "ledger_destination_drift");
    assert_eq!(drift["status"], "failed");
    assert_eq!(drift["details"]["counts"]["mismatched_state_rows"], 1);
    assert_eq!(drift["details"]["examples"][0]["kind"], "mismatched_state");
    assert_eq!(
        drift["details"]["examples"][0]["field"],
        "output_position_json"
    );
}

#[test]
fn doctor_fails_on_missing_and_extra_duckdb_mirror_rows() {
    let project = TestProject::new();
    create_duckdb_doctor_fixture(&project, DoctorDriftFixtureMode::TargetDrift);
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "doctor",
        "all",
    ]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let drift = named_check(&json, "ledger_destination_drift");
    assert_eq!(drift["status"], "failed");
    assert_eq!(drift["details"]["counts"]["missing_loads"], 1);
    assert_eq!(drift["details"]["counts"]["extra_loads"], 1);
    assert_eq!(drift["details"]["counts"]["missing_state_rows"], 1);
    assert_eq!(drift["details"]["counts"]["extra_state_rows"], 1);
}
