use super::*;

#[test]
fn contract_show_remains_project_free() {
    let result = run(["cdf", "--json", "contract", "show", "governed"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "contract show");
    assert_eq!(json["result"]["policy"], "governed");
    assert_eq!(
        json["result"]["contract"]["schema"]["review_artifact_required"],
        true
    );
}
