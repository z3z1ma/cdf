use super::*;

#[test]
fn doctor_skips_duckdb_drift_without_creating_missing_databases() {
    let project = TestProject::new();
    let state_path = project.root.join(".cdf/state.db");
    let duckdb_path = project.root.join(".cdf/dev.duckdb");
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!state_path.exists(), "doctor must not create state DB");
    assert!(
        !duckdb_path.exists(),
        "doctor drift probe must not create DuckDB DB"
    );
    let json = stderr_or_stdout_json(&result.stdout);
    let project_file = named_check(&json, "project_file");
    assert_eq!(project_file["details"]["project_root"], project.root_str());
    assert_eq!(project_file["details"]["selected_environment"], "dev");
    assert_eq!(project_file["details"]["compiled_resources"], 1);
    assert_eq!(project_file["details"]["lockfile_present"], false);
    let icu = named_check(&json, "duckdb_icu");
    assert_eq!(icu["status"], "skipped");
    assert_eq!(icu["details"]["database_exists"], false);
    assert_eq!(icu["details"]["probe"], "icu_sort_key");
    let drift = named_check(&json, "ledger_destination_drift");
    assert_eq!(drift["status"], "skipped");
    assert!(
        drift["message"]
            .as_str()
            .unwrap()
            .contains("SQLite state database is absent")
    );
}

#[test]
fn doctor_reports_lockfile_presence_when_lock_exists() {
    let project = TestProject::new();
    write_minimal_lockfile(&project);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let project_file = named_check(&json, "project_file");
    assert_eq!(project_file["details"]["lockfile_present"], true);
}

#[test]
fn doctor_registered_source_probe_fails_independently_before_network_or_writes() {
    let project = TestProject::new();
    fs::write(
        project.root.join("cdf/local/events.cdf.sql"),
        r#"
[source.local]
kind = "files"
root = "https://private.example.test/data"
egress_allowlist = ["allowed.example.test"]

[resource.events]
glob = "events.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
schema = { fields = [
  { name = "id", type = "int64", nullable = false },
] }
"#,
    )
    .unwrap();

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(
        result.exit_code, 1,
        "stdout: {}\nstderr: {}",
        result.stdout, result.stderr
    );
    let json = stderr_or_stdout_json(&result.stdout);
    let source = named_check(&json, "source.files.local.events");
    assert_eq!(source["status"], "failed");
    assert_eq!(source["details"]["resource_id"], "local.events");
    assert_eq!(source["details"]["error_kind"], "auth");
    assert_eq!(source["message"], "file source inventory probe failed");
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
}

#[test]
fn doctor_reports_resolved_secret_references_without_values() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        "resolved-destination-dsn-value\n",
    )
    .unwrap();
    fs::write(
        project.root.join("auth-token"),
        "resolved-auth-token-value\n",
    )
    .unwrap();
    fs::write(
        project.root.join("postgres-dsn"),
        "resolved-file-secret-value\n",
    )
    .unwrap();
    write_secret_project(
        &project,
        "postgres://secret://file/destination-dsn",
        Some("secret://file/auth-token"),
        Some("secret://file/postgres-dsn"),
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(
        result.exit_code, 1,
        "stdout: {}\nstderr: {}",
        result.stdout, result.stderr
    );
    assert_secret_absent(&result, "resolved-destination-dsn-value");
    assert_secret_absent(&result, "resolved-auth-token-value");
    assert_secret_absent(&result, "resolved-file-secret-value");
    let json = stderr_or_stdout_json(&result.stdout);
    let secrets = named_check(&json, "secrets");
    assert_eq!(secrets["status"], "passed");
    assert_eq!(secrets["details"]["count"], 3);
    let references = secrets["details"]["references"].as_array().unwrap();
    for reference in [
        "secret://file/destination-dsn".to_owned(),
        "secret://file/auth-token".to_owned(),
        "secret://file/postgres-dsn".to_owned(),
    ] {
        assert!(
            references.iter().any(|value| value == &reference),
            "missing secret reference {reference}"
        );
    }
    assert_eq!(json["result"]["failed"], 2);
    assert_eq!(
        named_check(&json, "source.postgres.warehouse.orders")["status"],
        "failed"
    );
    assert_eq!(
        named_check(&json, "source.rest.api.items")["status"],
        "failed"
    );
}

#[test]
fn doctor_later_secret_failure_does_not_leak_already_resolved_secrets() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-before-failure"),
        "already-resolved-destination-value\n",
    )
    .unwrap();
    fs::write(
        project.root.join("token-before-failure"),
        "already-resolved-token-value\n",
    )
    .unwrap();
    fs::write(
        project.root.join("resolved-file-secret"),
        "already-resolved-file-value\n",
    )
    .unwrap();
    write_secret_project(
        &project,
        "postgres://secret://file/destination-before-failure",
        Some("secret://file/token-before-failure"),
        Some("secret://env/CDF_CLI_MISSING_SQL_AFTER_RESOLVED"),
    );
    let project_file = project.root.join("cdf.toml");
    let project_text = fs::read_to_string(&project_file).unwrap().replace(
        "packages = \".cdf/packages\"",
        "packages = \"secret://file/resolved-file-secret\"",
    );
    fs::write(project_file, project_text).unwrap();

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    assert_secret_absent(&result, "already-resolved-destination-value");
    assert_secret_absent(&result, "already-resolved-token-value");
    assert_secret_absent(&result, "already-resolved-file-value");
    let json = stderr_or_stdout_json(&result.stdout);
    let secrets = named_check(&json, "secrets");
    assert_eq!(secrets["status"], "failed");
    assert!(
        secrets["message"]
            .as_str()
            .unwrap()
            .contains("secret://env/CDF_CLI_MISSING_SQL_AFTER_RESOLVED")
    );
}

#[test]
fn doctor_fails_missing_and_unavailable_secrets_without_leaking_values() {
    for case in [
        SecretFailureCase::EnvironmentDestination,
        SecretFailureCase::File,
        SecretFailureCase::DeclarativeAuthToken,
        SecretFailureCase::DeclarativeSqlConnection,
        SecretFailureCase::UnavailableProvider,
    ] {
        let project = TestProject::new();
        write_secret_failure_project(&project, case);

        let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

        assert_eq!(result.exit_code, 1, "case {case:?}");
        assert_secret_absent(&result, "would-be-token-value");
        assert_secret_absent(&result, "would-be-file-value");
        let json = stderr_or_stdout_json(&result.stdout);
        let secrets = named_check(&json, "secrets");
        assert_eq!(secrets["status"], "failed", "case {case:?}");
        assert!(secrets.as_object().unwrap().get("details").is_none());
    }
}

#[test]
fn doctor_runs_duckdb_icu_probe_for_existing_database_with_safe_details() {
    let project = TestProject::new();
    let duckdb_path = project.root.join(".cdf/dev.duckdb");
    DuckDbDestination::new(&duckdb_path)
        .unwrap()
        .probe_icu()
        .unwrap();

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert!(duckdb_path.exists(), "fixture should create the DuckDB DB");
    let json = stderr_or_stdout_json(&result.stdout);
    let icu = named_check(&json, "duckdb_icu");
    assert!(
        matches!(icu["status"].as_str(), Some("passed" | "failed")),
        "unexpected ICU status: {icu}"
    );
    assert_eq!(icu["details"]["database_exists"], true);
    assert_eq!(icu["details"]["probe"], "icu_sort_key");
    assert_eq!(
        icu["details"]["available"],
        icu["status"].as_str().unwrap() == "passed"
    );
    assert!(!icu.to_string().contains("resolved-api-token-value"));
}

#[test]
fn doctor_skips_python_without_interpreter_or_python_resources() {
    let project = TestProject::new();
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "skipped");
    assert_eq!(python["details"]["python_resources"], 0);
    assert_eq!(python["details"]["require_free_threaded"], false);
}

#[test]
fn doctor_passes_gil_enabled_python_interpreter_with_details() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json(&interpreter, 3, 12, 7, true, false),
    );
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "passed");
    assert_eq!(
        python["details"]["executable"],
        interpreter.canonicalize().unwrap().display().to_string()
    );
    assert_eq!(python["details"]["version"], "3.12.7");
    assert_eq!(python["details"]["implementation"], "CPython");
    assert_eq!(python["details"]["gil_enabled"], true);
    assert_eq!(python["details"]["free_threaded_build"], false);
    assert_eq!(python["details"]["can_parallelize_python"], false);
    assert_eq!(python["details"]["require_free_threaded"], false);
}

#[test]
fn doctor_passes_when_free_threaded_required_and_gil_disabled() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json(&interpreter, 3, 13, 1, false, true),
    );
    write_python_config_project(&project, "fake-python", true);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "passed");
    assert_eq!(python["details"]["gil_enabled"], false);
    assert_eq!(python["details"]["free_threaded_build"], true);
    assert_eq!(python["details"]["can_parallelize_python"], true);
    assert_eq!(python["details"]["require_free_threaded"], true);
}

#[test]
fn doctor_fails_when_free_threaded_required_but_gil_enabled() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json(&interpreter, 3, 12, 7, true, false),
    );
    write_python_config_project(&project, "fake-python", true);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("free-threaded")
    );
    assert_eq!(python["details"]["require_free_threaded"], true);
    assert_eq!(python["details"]["can_parallelize_python"], false);
}

#[test]
fn doctor_fails_when_free_threaded_build_still_has_gil_enabled() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json(&interpreter, 3, 13, 1, true, true),
    );
    write_python_config_project(&project, "fake-python", true);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "failed");
    assert_eq!(python["details"]["gil_enabled"], true);
    assert_eq!(python["details"]["free_threaded_build"], true);
    assert_eq!(python["details"]["can_parallelize_python"], false);
    assert_eq!(python["details"]["require_free_threaded"], true);
}

#[test]
fn doctor_fails_missing_python_interpreter() {
    let project = TestProject::new();
    write_python_config_project(&project, "absent-python", true);
    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("configured interpreter is missing")
    );
    assert!(
        python["details"]["executable"]
            .as_str()
            .unwrap()
            .ends_with("absent-python")
    );
    assert_eq!(python["details"]["require_free_threaded"], true);
}

#[cfg(unix)]
#[test]
fn doctor_fails_non_executable_python_interpreter() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    fs::write(&interpreter, "#!/bin/sh\nexit 0\n").unwrap();
    set_mode(&interpreter, 0o644);
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("not executable")
    );
}

#[test]
fn doctor_fails_unsuccessful_python_probe_without_echoing_output() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_failing_interpreter(&interpreter);
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    assert!(!result.stdout.contains("SUPER_SECRET"));
    assert!(!result.stderr.contains("SUPER_SECRET"));
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("exited unsuccessfully")
    );
}

#[test]
fn doctor_fails_invalid_python_probe_json_without_echoing_output() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(&interpreter, "not-json SUPER_SECRET");
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    assert!(!result.stdout.contains("SUPER_SECRET"));
    assert!(!result.stderr.contains("SUPER_SECRET"));
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("valid inspection JSON")
    );
}

#[test]
fn doctor_fails_probe_json_with_inconsistent_version_metadata() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json_from(FakePythonProbe {
            executable: &interpreter,
            version: "3.12.8",
            major: 3,
            minor: 12,
            micro: 7,
            gil_enabled: true,
            free_threaded_build: false,
            can_parallelize_python: false,
        }),
    );
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("inconsistent version metadata"),
        "{python}"
    );
}

#[test]
fn doctor_fails_probe_json_with_inconsistent_gil_metadata() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json_from(FakePythonProbe {
            executable: &interpreter,
            version: "3.12.7",
            major: 3,
            minor: 12,
            micro: 7,
            gil_enabled: false,
            free_threaded_build: true,
            can_parallelize_python: false,
        }),
    );
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("inconsistent GIL metadata"),
        "{python}"
    );
}

#[test]
fn doctor_fails_old_python_interpreter_version() {
    let project = TestProject::new();
    let interpreter = project.root.join("fake-python");
    write_fake_interpreter(
        &interpreter,
        &python_probe_json(&interpreter, 3, 11, 9, true, false),
    );
    write_python_config_project(&project, "fake-python", false);

    let result = run(["cdf", "--json", "--project", project.root_str(), "doctor"]);

    assert_eq!(result.exit_code, 1);
    let json = stderr_or_stdout_json(&result.stdout);
    let python = named_check(&json, "source.python.interpreter");
    assert_eq!(python["status"], "failed");
    assert!(
        python["message"]
            .as_str()
            .unwrap()
            .contains("older than required 3.12")
    );
    assert_eq!(python["details"]["version"], "3.11.9");
}
