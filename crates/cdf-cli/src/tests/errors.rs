use super::*;

#[test]
fn migrated_command_family_errors_include_code_and_remediation() {
    let init = run_dynamic(vec![
        "cdf".to_owned(),
        "--json".to_owned(),
        "init".to_owned(),
        "--name".to_owned(),
        String::new(),
    ]);
    assert_json_error_code(&init, "CDF-PROJECT-INIT-ARGUMENT");

    let project = TestProject::new();
    let scan = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--order-by",
        "id:sideways",
    ]);
    assert_json_error_code(&scan, "CDF-RUN-SCAN-ARGUMENT");

    let run_result = run(["cdf", "--json", "run"]);
    assert_json_error_code(&run_result, "CDF-RUN-ARGUMENT");

    let run_loop = run(["cdf", "--json", "run", "local.events", "--loop"]);
    let run_loop_json = assert_json_error_code(&run_loop, "CDF-RUN-LOOP-NOT-SUPPORTED");
    assert_eq!(run_loop_json["error"]["not_supported"], true);

    let replay_project = TestProject::new();
    let package_dir = create_replay_package_fixture(&replay_project);
    let replay = replay_package_command_with_target(
        &replay_project,
        &package_dir,
        "postgres://localhost/db",
        None,
    );
    assert_json_error_code(&replay, "CDF-PACKAGE-REPLAY-ARGUMENT");

    let package = run([
        "cdf", "--json", "package", "archive", ".", "--format", "json",
    ]);
    assert_json_error_code(&package, "CDF-PACKAGE-ARGUMENT");

    let state = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "show",
        "local.events",
        "--scope",
        "bad",
    ]);
    assert_json_error_code(&state, "CDF-STATE-SCOPE-ARGUMENT");

    let sql = run(["cdf", "--json", "sql", "delete from packages"]);
    assert_json_error_code(&sql, "CDF-SQL-QUERY");
}

#[test]
fn unknown_command_returns_usage_exit_code() {
    let result = run(["cdf", "--json", "bogus"]);

    assert_eq!(result.exit_code, 2);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(json["error"]["message"].as_str().unwrap().contains("bogus"));
    assert_eq!(json["error"]["exit_code"], 2);
    assert_eq!(json["error"]["not_supported"], false);
    assert_eq!(json["error"]["code"], "CDF-CLI-USAGE");
    assert_eq!(
        json["error"]["remediation"]["summary"],
        "Correct the command arguments and run the command again."
    );
    assert!(json["error"]["remediation"]["steps"].is_array());
    assert!(json["error"]["suggestions"].is_null());
}

#[test]
fn unknown_command_and_subcommand_json_suggest_high_confidence_matches() {
    let command = run(["cdf", "--json", "staus"]);

    assert_eq!(command.exit_code, 2);
    let json = assert_json_error_code(&command, "CDF-CLI-USAGE");
    assert_eq!(json["error"]["suggestions"], json!(["cdf status"]));

    let subcommand = run(["cdf", "--json", "inspect", "resorce"]);

    assert_eq!(subcommand.exit_code, 2);
    let json = assert_json_error_code(&subcommand, "CDF-CLI-USAGE");
    assert_eq!(
        json["error"]["suggestions"],
        json!(["cdf inspect resource"])
    );
}

#[test]
fn unknown_resource_json_suggests_nearest_configured_resource_id() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "resource",
        "local.eventz",
    ]);

    assert_eq!(result.exit_code, 3, "stderr: {}", result.stderr);
    let json = assert_json_error_code(&result, "CDF-RESOURCE-NOT-COMPILED");
    assert_eq!(json["error"]["suggestions"], json!(["local.events"]));
}

#[test]
fn unknown_resource_json_omits_suggestions_without_inventory() {
    let project = TestProject::new();
    fs::write(
        project.root.join("cdf.toml"),
        r#"
[project]
name = "cli_test"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"
"#,
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "resource",
        "local.eventz",
    ]);

    assert_eq!(result.exit_code, 3, "stderr: {}", result.stderr);
    let json = assert_json_error_code(&result, "CDF-PROJECT-CONTRACT");
    assert!(json["error"]["suggestions"].is_null());
}

#[test]
fn unknown_destination_json_suggests_environment_or_uri_shape_without_secrets() {
    let project = TestProject::new();
    fs::write(
        project.root.join("cdf.toml"),
        r#"
[project]
name = "cli_test"
default_environment = "dev"
normalizer = "namecase-v1"

[environments.dev]
state = "sqlite://.cdf/state.db"
packages = ".cdf/packages"
destination = "duckdb://.cdf/dev.duckdb"

[environments.prod]
destination = "duckdb://.cdf/prod.duckdb"

[sources.local]
type = "files"
root = "data"
"#,
    )
    .unwrap();

    let typo = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--to",
        "prd",
    ]);

    assert_eq!(typo.exit_code, 78, "stderr: {}", typo.stderr);
    let json = assert_json_error_code(&typo, "CDF-DEST-NOT-SUPPORTED");
    assert_eq!(
        json["error"]["suggestions"],
        json!(["--env prod", "duckdb://path", "parquet://root"])
    );

    let package_dir = create_replay_package_fixture(&project);
    let redacted = replay_package_command(
        &project,
        &package_dir,
        "dckdb://user:destination-secret@localhost/db",
    );

    assert_eq!(redacted.exit_code, 78, "stderr: {}", redacted.stderr);
    assert_secret_absent(&redacted, "destination-secret");
    let json = assert_json_error_code(&redacted, "CDF-DEST-NOT-SUPPORTED");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("dckdb://[redacted]@localhost/db")
    );
    assert_eq!(
        json["error"]["suggestions"],
        json!([
            "duckdb://path",
            "parquet://root",
            "postgres://secret://env/NAME"
        ])
    );
}

#[test]
fn usage_error_human_output_keeps_message_and_adds_remediation() {
    let result = run(["cdf", "sql"]);

    assert_eq!(result.exit_code, 2);
    assert!(result.stderr.contains("error["));
    assert!(result.stderr.contains("sql requires a query string"));
    assert!(result.stderr.contains("help:"));
}

#[test]
fn not_supported_error_preserves_exit_code_and_json_compatibility() {
    let error = cdf_cli_core::output::CliError::not_supported(
        "preview",
        "query resources",
        "native scan runtime",
    );
    let result = cdf_cli_core::output::InvocationResult::from_error(true, error);

    assert_eq!(result.exit_code, 78);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["ok"], false);
    assert_eq!(json["error"]["kind"], "internal");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("preview")
    );
    assert_eq!(json["error"]["exit_code"], 78);
    assert_eq!(json["error"]["not_supported"], true);
    assert_eq!(json["error"]["code"], "CDF-CLI-NOT-SUPPORTED");
    assert_eq!(
        json["error"]["remediation"]["summary"],
        "Use a currently supported path or wait for the named lower layer to land."
    );
}

#[test]
fn generic_lower_layer_conversion_uses_documented_mapping() {
    let error =
        cdf_cli_core::output::CliError::from(CdfError::destination("destination refused commit"));
    let result = cdf_cli_core::output::InvocationResult::from_error(true, error);

    assert_eq!(result.exit_code, 6);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "destination");
    assert_eq!(json["error"]["message"], "destination refused commit");
    assert_eq!(json["error"]["exit_code"], 6);
    assert_eq!(json["error"]["not_supported"], false);
    assert_eq!(json["error"]["code"], "CDF-DEST-ERROR");
    assert_eq!(
        json["error"]["remediation"]["summary"],
        "Inspect the destination URI, target, policy, and destination health."
    );
}
