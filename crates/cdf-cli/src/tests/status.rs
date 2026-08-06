use super::*;

#[test]
fn status_reports_no_freshness_resources_for_current_project_metadata() {
    let project = TestProject::new();

    let json_result = run(["cdf", "--json", "--project", project.root_str(), "status"]);
    assert_eq!(json_result.exit_code, 0, "stderr: {}", json_result.stderr);
    let json = stderr_or_stdout_json(&json_result.stdout);
    assert_eq!(
        json["result"]["freshness_resources"],
        json!([]),
        "current project SQL has no freshness declaration surface"
    );

    let human = run(["cdf", "--project", project.root_str(), "status"]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(
        human
            .stdout
            .contains("OK no freshness SLO resources to evaluate")
    );
}

#[test]
fn status_reads_durable_status_authority_without_parsing_authored_sql() {
    let project = TestProject::new();
    fs::write(
        project.root.join("cdf/local/events.cdf.sql"),
        "this is not resource SQL",
    )
    .unwrap();

    let result = run(["cdf", "--json", "--project", project.root_str(), "status"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["summary"]["total"], 0);
    assert!(!project.root.join(".cdf/state.db").exists());
}
