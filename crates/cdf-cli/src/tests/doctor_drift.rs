use super::*;

#[test]
fn doctor_passes_clean_duckdb_ledger_mirror_drift_check() {
    let project = TestProject::new();
    create_duckdb_doctor_fixture(&project, DoctorDriftFixtureMode::Clean);
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let drift = named_check(&json, "ledger_destination_drift");
    assert_eq!(drift["status"], "passed");
    assert_eq!(drift["details"]["counts"]["ledger_heads"], 1);
    assert_eq!(drift["details"]["counts"]["expected_loads"], 1);
    assert_eq!(drift["details"]["counts"]["expected_state_rows"], 1);
    assert_eq!(drift["details"]["counts"]["mirror_loads"], 1);
    assert_eq!(drift["details"]["counts"]["mirror_state_rows"], 1);
    assert_eq!(drift["details"]["examples"].as_array().unwrap().len(), 0);
}

#[test]
fn doctor_fails_on_duckdb_state_mirror_drift() {
    let project = TestProject::new();
    create_duckdb_doctor_fixture(&project, DoctorDriftFixtureMode::StatePositionDrift);
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

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
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let drift = named_check(&json, "ledger_destination_drift");
    assert_eq!(drift["status"], "failed");
    assert_eq!(drift["details"]["counts"]["missing_loads"], 1);
    assert_eq!(drift["details"]["counts"]["extra_loads"], 1);
    assert_eq!(drift["details"]["counts"]["missing_state_rows"], 1);
    assert_eq!(drift["details"]["counts"]["extra_state_rows"], 1);
}
