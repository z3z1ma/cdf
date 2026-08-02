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
        "help", "version", "init", "add", "validate", "plan", "explain", "run", "preview", "sql",
        "inspect", "diff", "schema", "contract", "state", "resume", "replay", "backfill",
        "package", "doctor", "status",
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
        "Merge deduplication policy",
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
    assert!(validate.stdout.contains("--deep"));

    let add = run(["cdf", "add", "--help"]);

    assert_eq!(add.exit_code, 0);
    assert!(add.stdout.contains("Usage: cdf add"));
    assert!(add.stdout.contains("RESOURCE_ID"));
    assert!(add.stdout.contains("URL_OR_PATH"));
    assert!(add.stdout.contains("--dry-run"));

    let plan = run(["cdf", "plan", "--help"]);

    assert_eq!(plan.exit_code, 0);
    assert!(plan.stdout.contains("Usage: cdf plan"));
    assert!(plan.stdout.contains("[RESOURCE]"));
    assert!(plan.stdout.contains("--to <DEST>"));
    assert!(!plan.stdout.contains("--resource"));
    assert!(!plan.stdout.contains("--target"));

    let schema = run(["cdf", "schema", "discover", "--help"]);

    assert_eq!(schema.exit_code, 0);
    assert!(schema.stdout.contains("Usage: cdf schema discover"));
    assert!(schema.stdout.contains("[RESOURCE]"));
    assert!(!schema.stdout.contains("--resource"));

    for subcommand in ["pin", "show", "diff", "promote"] {
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
fn renderer_migration_gate_rejects_raw_human_output_bypasses() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rust_files(&src, &mut files);
    let mut violations = Vec::new();
    for file in files {
        let relative = file.strip_prefix(&src).unwrap();
        if relative == Path::new("output.rs") || relative.starts_with("tests") {
            continue;
        }
        let text = fs::read_to_string(&file).unwrap();
        for (pattern, reason) in [
            (
                "HumanOutput::Plain",
                "plain human output bypasses the renderer",
            ),
            (
                "CommandOutput {",
                "commands must construct output through renderer helpers",
            ),
            (
                "commands::output",
                "command modules must return RenderDocument output directly",
            ),
            (
                "commands::report_output",
                "command modules must return RenderDocument output directly",
            ),
            (
                "commands::{output",
                "command modules must not import the raw output shim",
            ),
            (
                "commands::{report_output",
                "command modules must not import the raw output shim",
            ),
            (
                "report_output(",
                "command modules must not call the raw output shim",
            ),
            (
                "human_message(",
                "legacy human message helpers bypass renderer documents",
            ),
        ] {
            if text.contains(pattern) {
                violations.push(format!(
                    "{} contains `{pattern}`: {reason}",
                    relative.display()
                ));
            }
        }
        let relative_text = relative.to_string_lossy();
        let is_renderer_authority = relative_text == "reports.rs"
            || relative_text == "resume_command/report.rs"
            || relative_text.ends_with("/render.rs");
        if !is_renderer_authority {
            for (pattern, reason) in [
                (
                    "RenderDocument",
                    "command execution modules must delegate layout to a report renderer",
                ),
                (
                    "primitives::{",
                    "command execution modules must not import renderer primitives",
                ),
                (
                    "KeyValuePanel",
                    "command execution modules must not assemble key-value panels",
                ),
                (
                    "StatusLine",
                    "command execution modules must not assemble status lines",
                ),
                (
                    "Table::new(",
                    "command execution modules must not assemble tables",
                ),
            ] {
                if text.contains(pattern) {
                    violations.push(format!(
                        "{} contains `{pattern}`: {reason}",
                        relative.display()
                    ));
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "renderer migration gate failed:\n{}",
        violations.join("\n")
    );
}

#[test]
fn progress_enabled_human_commands_route_through_progress_renderer() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let expectations: &[(&str, &[&str])] = &[
        (
            "run_command.rs",
            &[
                "let progress = human_progress_sink(cli.json, &cli.terminal, progress_delivery);",
                "let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);",
                "event_sink,",
                "error.with_progress(progress.finish())",
                "Some(progress) => CommandOutput::rendered_with_progress(",
            ],
        ),
        (
            "replay_command.rs",
            &[
                "let progress = human_progress_sink(cli.json, &cli.terminal, progress_delivery);",
                "let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);",
                "ReplayProgressRecorder::new(",
                "error.with_progress(progress.finish())",
                "CommandOutput::rendered_with_progress(",
            ],
        ),
        (
            "resume_command.rs",
            &[
                "let progress = human_progress_sink(cli.json, &cli.terminal, progress_delivery);",
                "let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);",
                "sink.try_emit(event)",
                "ResumeAttempt::new(",
                "destinations,",
                "finish_resume_report(report, progress.map(CliProgressSink::finish))",
            ],
        ),
        (
            "resume_command/report.rs",
            &["CommandOutput::rendered_with_progress_and_exit_code("],
        ),
        (
            "backfill_command.rs",
            &[
                "let mut progress = human_progress_sink(cli.json, &cli.terminal, progress_delivery);",
                "let event_sink = progress.as_ref().map(|sink| sink as &dyn RunEventSink);",
                "BackfillSliceExecutor {",
                ".execute(slice)",
                "destinations,",
                "progress.take().map(CliProgressSink::finish)",
                "CommandOutput::rendered_with_progress(",
            ],
        ),
    ];

    for (relative, patterns) in expectations {
        let text = fs::read_to_string(src.join(relative)).unwrap();
        for pattern in *patterns {
            assert!(
                text.contains(pattern),
                "{relative} no longer routes human progress through `{pattern}`"
            );
        }
    }
}

#[test]
fn destination_registry_composition_is_confined_to_the_cli_root() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let src = manifest_dir.join("src");
    let mut files = Vec::new();
    collect_rust_files(&src, &mut files);
    let mut violations = Vec::new();

    for path in files {
        let relative = path.strip_prefix(manifest_dir).unwrap();
        let relative_text = relative.to_string_lossy();
        if relative_text == "src/tests.rs"
            || relative_text.starts_with("src/tests/")
            || relative_text == "src/destination_registry_test_support.rs"
        {
            continue;
        }
        let text = fs::read_to_string(&path).unwrap();
        let concrete_driver_import = text.contains("cdf_dest_");
        let concrete_driver_allowed = matches!(
            relative_text.as_ref(),
            "src/destination_registry.rs" | "src/doctor_drift.rs"
        );
        if concrete_driver_import && !concrete_driver_allowed {
            violations.push(format!(
                "{relative_text} imports a concrete destination outside the composition root"
            ));
        }
        if text.contains("builtin_destination_registry()")
            && relative_text != "src/destination_registry.rs"
            && relative_text != "src/lib.rs"
        {
            violations.push(format!(
                "{relative_text} reconstructs the builtin destination registry below the invocation root"
            ));
        }
    }

    let lib = fs::read_to_string(src.join("lib.rs")).unwrap();
    assert_eq!(
        lib.matches("destination_registry::builtin_destination_registry()")
            .count(),
        1,
        "production invocation must construct the builtin destination registry exactly once"
    );
    assert!(
        violations.is_empty(),
        "destination composition boundary regressed:\n{}",
        violations.join("\n")
    );
}

fn collect_rust_files(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
    }
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
    assert!(resources.stdout.contains("resources/files.toml"));
    assert!(resources.stdout.contains("matched local.*"));

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
fn resource_mapping_pattern_mismatch_reports_validate_and_plan_commands() {
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

[resources."yellow"]
source = "resources/tlc.toml"
"#,
    )
    .unwrap();
    fs::write(
        project.root.join("resources/tlc.toml"),
        r#"
[source.tlc]
kind = "files"
root = "data"

[resource.yellow]
glob = "*.parquet"
format = "parquet"
write_disposition = "append"
trust = "governed"
"#,
    )
    .unwrap();

    let validate = run(["cdf", "--project", project.root_str(), "validate"]);
    assert_ne!(validate.exit_code, 0);
    assert!(validate.stderr.contains("cdf validate cannot load project"));
    assert!(
        validate
            .stderr
            .contains("resource mapping pattern `yellow`")
    );
    assert!(validate.stderr.contains("tlc.yellow"));
    assert!(validate.stderr.contains("[resources.\"tlc.yellow\"]"));

    let plan = run(["cdf", "--project", project.root_str(), "plan", "tlc.yellow"]);
    assert_ne!(plan.exit_code, 0);
    assert!(plan.stderr.contains("cdf plan cannot load project"));
    assert!(!plan.stderr.contains("cdf validate cannot load project"));
    assert!(plan.stderr.contains("resource mapping pattern `yellow`"));
    assert!(plan.stderr.contains("tlc.yellow"));
    assert!(plan.stderr.contains("[resources.\"tlc.yellow\"]"));
}

#[test]
fn parser_accepts_canonical_color_policy_anywhere_without_changing_json_envelope() {
    let result = run(["cdf", "version", "--color", "never", "--json"]);

    assert_eq!(result.exit_code, 0);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "version");
    assert_eq!(json["result"]["version"], env!("CARGO_PKG_VERSION"));
}
