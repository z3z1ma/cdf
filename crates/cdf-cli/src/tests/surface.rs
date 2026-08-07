use super::*;

#[test]
fn missing_current_directory_maps_through_the_cli_environment_boundary() {
    let error = super::context::project_location_with_current_dir(None, || {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "current directory was removed",
        ))
    })
    .unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Environment);
    let mapped = cdf_cli_core::output::CliError::from(error);
    assert_eq!(mapped.code, "CDF-ENV-HOST");
    assert_eq!(mapped.exit_code, 70);
    assert!(mapped.message.contains("absolute --project"));
}

#[test]
fn help_lists_required_command_surface() {
    let result = run(["cdf", "--help"]);

    assert_eq!(result.exit_code, 0);
    for command in [
        "help", "version", "init", "add", "discover", "compile", "validate", "plan", "explain",
        "run", "preview", "sql", "inspect", "schema", "contract", "state", "backfill", "package",
        "doctor", "status",
    ] {
        assert!(result.stdout.contains(command), "missing {command}");
    }
    for required in [
        "--progress <WHEN>",
        "--unicode <WHEN>",
        "Environment:",
        "Examples:",
    ] {
        assert!(result.stdout.contains(required), "missing {required}");
    }
}

#[test]
fn cx1_short_and_long_help_are_distinct_complete_and_placeholder_free() {
    let short = run(["cdf", "-h"]);
    assert_eq!(short.exit_code, 0);
    assert!(short.stdout.contains("--progress"));
    assert!(short.stdout.contains("--unicode"));
    assert!(!short.stdout.contains("Environment:"));

    let long = run(["cdf", "help"]);
    assert_eq!(long.exit_code, 0);
    for required in ["--progress", "--unicode", "Environment:", "Examples:"] {
        assert!(long.stdout.contains(required), "missing {required}");
    }

    let recover = run(["cdf", "state", "recover", "--help"]);
    for required in [
        "Package directory",
        "Receipt identifier",
        "Destination target or table",
    ] {
        assert!(recover.stdout.contains(required), "missing {required}");
    }
    assert!(!recover.stdout.contains("Command option"));
    assert!(!recover.stdout.contains("Command value"));
}

#[test]
fn parser_provides_subcommand_help_at_nested_layers() {
    let validate = run(["cdf", "validate", "--help"]);

    assert_eq!(validate.exit_code, 0);
    assert!(validate.stdout.contains("Usage: cdf validate"));
    assert!(validate.stdout.contains("RESOURCE_SELECTOR"));
    assert!(validate.stdout.contains("--exclude <RESOURCE_GLOB>"));
    assert!(!validate.stdout.contains("--deep"));

    let add = run(["cdf", "add", "--help"]);

    assert_eq!(add.exit_code, 0);
    assert!(add.stdout.contains("Usage: cdf add"));
    assert!(add.stdout.contains("RESOURCE_ID"));
    assert!(add.stdout.contains("URL_OR_PATH"));
    assert!(add.stdout.contains("--dry-run"));

    let plan = run(["cdf", "plan", "--help"]);

    assert_eq!(plan.exit_code, 0);
    assert!(plan.stdout.contains("Usage: cdf plan"));
    assert!(plan.stdout.contains("[RESOURCE_SELECTOR]"));
    assert!(plan.stdout.contains("--exclude <RESOURCE_GLOB>"));
    assert!(plan.stdout.contains("--to <DEST>"));
    assert!(!plan.stdout.contains("--resource"));
    assert!(!plan.stdout.contains("--target"));

    for removed in ["discover", "pin"] {
        let result = run(["cdf", "schema", removed, "--help"]);
        assert_ne!(result.exit_code, 0);
    }

    for subcommand in ["show", "diff", "promote"] {
        let result = run(["cdf", "schema", subcommand, "--help"]);

        assert_eq!(result.exit_code, 0);
        assert!(
            result
                .stdout
                .contains(&format!("Usage: cdf schema {subcommand}"))
        );
        assert!(result.stdout.contains("[RESOURCE]"));
        assert!(!result.stdout.contains("--resource"));
        if subcommand == "promote" {
            assert!(result.stdout.contains("--type <JSON_POINTER=ARROW_TYPE>"));
            assert!(result.stdout.contains("--execute"));
        }
    }

    let rewind = run(["cdf", "state", "rewind", "--help"]);

    assert_eq!(rewind.exit_code, 0);
    assert!(rewind.stdout.contains("Usage: cdf state rewind"));
    assert!(rewind.stdout.contains("--scope <KEY=VALUE>"));
    assert!(rewind.stdout.contains("--to <CHECKPOINT>"));
    assert!(!rewind.stdout.contains("--target-checkpoint"));
    assert!(!rewind.stdout.contains("--marker-checkpoint"));

    let state = run(["cdf", "state", "--help"]);

    assert_eq!(state.exit_code, 0);
    assert!(state.stdout.contains("show"));
    assert!(state.stdout.contains("recover"));
    assert!(!state.stdout.contains("migrate"));
}

#[test]
fn state_migrate_is_absent_until_a_supported_predecessor_exists() {
    let result = run(["cdf", "--json", "state", "migrate"]);

    assert_eq!(result.exit_code, 2);
    let json = assert_json_error_code(&result, "CDF-CLI-USAGE");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unrecognized subcommand 'migrate'")
    );
}

#[test]
fn parser_help_command_renders_requested_command_path() {
    let result = run(["cdf", "help", "state", "rewind"]);

    assert_eq!(result.exit_code, 0);
    assert!(result.stdout.contains("Usage: cdf state rewind"));
    assert!(result.stdout.contains("--scope <KEY=VALUE>"));
    assert!(result.stdout.contains("--scope-json <JSON>"));
}

#[test]
fn parser_preserves_json_anywhere_for_help_envelope() {
    let result = run(["cdf", "plan", "--help", "--json"]);

    assert_eq!(result.exit_code, 0);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "help");
    assert!(
        json["result"]["help"]
            .as_str()
            .unwrap()
            .contains("Usage: cdf plan")
    );
}

#[test]
fn parser_preserves_global_project_env_and_json_anywhere() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "validate",
        "--project",
        project.root_str(),
        "--env",
        "dev",
        "--json",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "validate");
}

#[test]
fn inspect_human_outputs_use_renderer_for_project_inventory() {
    let project = TestProject::new();

    let project_result = run(["cdf", "--project", project.root_str(), "inspect", "project"]);
    assert_eq!(
        project_result.exit_code, 0,
        "stderr: {}",
        project_result.stderr
    );
    assert!(
        project_result
            .stdout
            .contains("OK project cli_test env dev")
    );
    assert!(project_result.stdout.contains("Project"));
    assert!(
        project_result
            .stdout
            .contains("destination  duckdb://.cdf/dev.duckdb")
    );
    assert!(
        project_result
            .stdout
            .contains("Next: cdf inspect resources")
    );

    let resources = run([
        "cdf",
        "--project",
        project.root_str(),
        "inspect",
        "resources",
    ]);
    assert_eq!(resources.exit_code, 0, "stderr: {}", resources.stderr);
    assert!(resources.stdout.contains("OK 1 compiled resource(s)"));
    assert!(resources.stdout.contains("compiled id"));
    assert!(resources.stdout.contains("local.events"));
    assert!(resources.stdout.contains("local"));
    assert!(resources.stdout.contains("events"));
    assert!(resources.stdout.contains("cdf/local/events.cdf.sql"));

    let resource = run([
        "cdf",
        "--project",
        project.root_str(),
        "inspect",
        "resource",
        "local.events",
    ]);
    assert_eq!(resource.exit_code, 0, "stderr: {}", resource.stderr);
    assert!(resource.stdout.contains("OK resource local.events"));
    assert!(resource.stdout.contains("Resource"));
    assert!(resource.stdout.contains("stream capabilities"));
    assert!(resource.stdout.contains("bounded"));
    assert!(resource.stdout.contains("Next: cdf plan local.events"));

    let destinations = run([
        "cdf",
        "--project",
        project.root_str(),
        "inspect",
        "destinations",
    ]);
    assert_eq!(destinations.exit_code, 0, "stderr: {}", destinations.stderr);
    assert!(
        destinations
            .stdout
            .contains("OK inspected destination capabilities")
    );
    assert!(destinations.stdout.contains("Destination"));
    assert!(
        destinations
            .stdout
            .contains("environment  duckdb://.cdf/dev.duckdb")
    );
    assert!(destinations.stdout.contains("Next: cdf plan"));
}

#[test]
fn inspect_project_redacts_the_same_typed_report_for_json_and_human_output() {
    let project = TestProject::new();
    write_project_destination(
        &project,
        "postgres://user:inspect-project-secret@localhost/db",
    );

    let json_result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "project",
    ]);
    assert_eq!(json_result.exit_code, 0, "stderr: {}", json_result.stderr);
    assert_secret_absent(&json_result, "inspect-project-secret");
    assert!(
        json_result
            .stdout
            .contains("postgres://[redacted]@localhost/db")
    );

    let human_result = run(["cdf", "--project", project.root_str(), "inspect", "project"]);
    assert_eq!(human_result.exit_code, 0, "stderr: {}", human_result.stderr);
    assert_secret_absent(&human_result, "inspect-project-secret");
    assert!(
        human_result
            .stdout
            .contains("postgres://[redacted]@localhost/db")
    );
}

#[test]
fn parser_accepts_canonical_color_policy_anywhere_without_changing_json_envelope() {
    let result = run(["cdf", "version", "--color", "never", "--json"]);

    assert_eq!(result.exit_code, 0);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "version");
    assert_eq!(json["result"]["version"], env!("CARGO_PKG_VERSION"));
}
