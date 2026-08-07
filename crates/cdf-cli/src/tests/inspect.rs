use super::*;

#[test]
fn inspect_run_parser_rejects_missing_and_extra_args() {
    for args in [
        vec!["cdf", "--json", "inspect", "run"],
        vec!["cdf", "--json", "inspect", "run", "run-1", "extra"],
    ] {
        let result = run_dynamic(args.into_iter().map(str::to_owned).collect());

        assert_eq!(result.exit_code, 2, "stderr: {}", result.stderr);
        let json = assert_json_error_code(&result, "CDF-CLI-USAGE");
        assert_eq!(json["error"]["kind"], "contract");
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("inspect run")
        );
    }
}

#[test]
fn inspect_project_does_not_parse_authored_resource_sql() {
    let project = TestProject::new();
    fs::write(
        project.root.join("cdf/local/events.cdf.sql"),
        "this is not resource SQL",
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "project",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_eq!(
        stderr_or_stdout_json(&result.stdout)["command"],
        "inspect project"
    );
}

#[test]
fn inspect_run_reports_completed_run_json_and_human() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_report = &run_json["result"]["resources"][0]["result"];
    let run_id = run_report["run_id"].as_str().unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id,
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "inspect run");
    assert_eq!(report["command"], "inspect run");
    assert_eq!(report["run_id"], run_id);
    assert_eq!(report["terminal_state"], "succeeded");
    assert_eq!(report["terminal_kind"], "run_succeeded");
    assert_eq!(report["recovery"]["action"], "no_op");
    assert_eq!(report["recovery"]["mutation_required"], false);
    assert_eq!(report["recovery"]["source_contact"], false);
    assert_eq!(report["pointers"]["resource_ids"], json!(["local.events"]));
    assert_eq!(
        report["pointers"]["package_ids"],
        json!([run_report["package_id"].clone()])
    );
    assert_eq!(
        report["pointers"]["checkpoint_ids"],
        json!([run_report["checkpoint_id"].clone()])
    );
    let events = report["events"].as_array().unwrap();
    assert!(!events.is_empty());
    assert_eq!(events[0]["sequence"], 1);
    assert_eq!(events[0]["kind"], "run_started");
    assert_eq!(events.last().unwrap()["kind"], "run_succeeded");
    assert_eq!(report["artifacts"]["package_status"], "checkpointed");
    assert_eq!(
        report["artifacts"]["packages"][0]["status"], "available",
        "package report: {}",
        report["artifacts"]["packages"][0]
    );
    assert_eq!(
        report["artifacts"]["packages"][0]["lifecycle_status"],
        "checkpointed"
    );
    assert_eq!(report["artifacts"]["receipt"]["status"], "available");
    assert_eq!(
        report["artifacts"]["receipt"]["package_receipt_ids"][0],
        run_report["receipt_id"]
    );
    assert_eq!(report["artifacts"]["checkpoint"]["status"], "committed");
    assert_eq!(report["duplicate"]["status"], "unknown");
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id,
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(!human.stdout.contains("\u{1b}["));
    for expected in [
        &format!("OK run {run_id} terminal succeeded"),
        "Recovery",
        "Artifacts",
        "Pointers",
        "Duplicate",
        "Package artifacts",
        "action             no_op",
        "checkpoint status     committed",
        "Next: cdf inspect run ",
    ] {
        assert!(
            human.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human.stdout
        );
    }
}

#[test]
fn inspect_run_marks_missing_package_artifact() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_id = run_json["result"]["resources"][0]["result"]["run_id"]
        .as_str()
        .unwrap();
    fs::remove_dir_all(run_package_dir(&project, &run_result)).unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id,
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let package = &json["result"]["artifacts"]["packages"][0];
    assert_eq!(package["status"], "missing");
    assert_eq!(package["receipt_artifact_status"], "unavailable");
    assert!(
        package["reason"]
            .as_str()
            .unwrap()
            .contains("does not exist")
    );
    assert_eq!(
        json["result"]["artifacts"]["receipt"]["status"],
        "unavailable"
    );

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id,
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    for expected in [
        "Package artifacts",
        "missing",
        "package path recorded in the run ledger does not exist",
        "missing packages      1",
        "receipt status        unavailable",
    ] {
        assert!(
            human.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human.stdout
        );
    }
}

#[test]
fn inspect_run_human_rich_render_uses_recovery_and_artifact_panels() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_id = run_json["result"]["resources"][0]["result"]["run_id"]
        .as_str()
        .unwrap();
    let context =
        crate::context::ProjectOperationalContext::load(Some(&project.root), None).unwrap();

    let output = crate::inspect_run_command::inspect_run(&context, run_id.to_owned()).unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "terminal succeeded",
        "Recovery",
        "Artifacts",
        "Pointers",
        "no_op",
        "committed",
        "seq:",
        "kind:",
        "cdf inspect run ",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn inspect_run_missing_ledger_or_run_fails_without_creating_state() {
    let project = TestProject::new();
    let state_path = project.root.join(".cdf/state.db");

    let missing_state = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        "run-absent",
    ]);

    assert_eq!(missing_state.exit_code, 5);
    assert!(
        !state_path.exists(),
        "inspect run must not create missing state"
    );
    let json = stderr_or_stdout_json(&missing_state.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("state database")
    );

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let missing_run = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        "run-not-in-ledger",
    ]);

    assert_eq!(missing_run.exit_code, 5);
    let json = stderr_or_stdout_json(&missing_run.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("not present")
    );
}

#[test]
fn inspect_run_redacts_secret_ref_details_without_resolving_project_secret() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        "postgres://user:resolved-inspect-secret@localhost/db\n",
    )
    .unwrap();
    let ledger = SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();
    let run_id = RunId::new("run-redaction").unwrap();
    let run_record = ledger.create_run(Some(run_id.clone())).unwrap();
    let mut event = RunEventAppend::new(RunEventKind::RunStarted);
    event.destination_id = Some(DestinationId::new("postgres").unwrap());
    event.details = RunEventDetails::new([(
        "destination_secret",
        RunEventValue::SecretRef(SecretReference::new("secret://file/destination-dsn").unwrap()),
    )]);
    ledger.append_event(&run_record.run_id, event).unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id.as_str(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "resolved-inspect-secret");
    let json = stderr_or_stdout_json(&result.stdout);
    let detail = &json["result"]["events"][0]["details"]["attributes"]["destination_secret"];
    assert_eq!(detail["type"], "secret_ref");
    assert_eq!(detail["value"], "secret://file/destination-dsn");
}

#[test]
fn inspect_run_human_render_redacts_uri_userinfo_in_artifact_paths() {
    let project = TestProject::new();
    let ledger = SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();
    let run_id = RunId::new("run-inspect-render-redaction").unwrap();
    let run_record = ledger.create_run(Some(run_id.clone())).unwrap();
    let mut event = RunEventAppend::new(RunEventKind::PackageFinalized);
    event.package_id = Some("pkg-inspect-render-redaction".to_owned());
    event.package_path = Some("postgres://user:inspect-render-secret@localhost/db".to_owned());
    ledger.append_event(&run_record.run_id, event).unwrap();

    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id.as_str(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "inspect-render-secret");
    assert!(result.stdout.contains("postgres://[redacted]@localhost/db"));
    assert!(result.stdout.contains("missing"));
}

#[test]
fn inspect_run_reports_duplicate_replay_status() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let authority_state = fs::read(project.root.join(".cdf/state.db")).unwrap();
    let first = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/inspect-run-duplicate.duckdb",
    );
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    remove_state_store(&project);
    fs::write(project.root.join(".cdf/state.db"), authority_state).unwrap();

    let second = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/inspect-run-duplicate.duckdb",
    );
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    let second_json = stderr_or_stdout_json(&second.stdout);
    let run_id = second_json["result"]["run_id"].as_str().unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "inspect",
        "run",
        run_id,
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["terminal_state"], "replay_recorded");
    assert_eq!(json["result"]["duplicate"]["status"], "duplicate");
    assert_eq!(json["result"]["duplicate"]["duplicate"], true);
    assert_eq!(json["result"]["duplicate"]["no_op"], true);
    let replay_event = json["result"]["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["kind"] == "replay_recorded")
        .unwrap();
    assert_eq!(
        replay_event["details"]["attributes"]["receipt_source"]["value"],
        "duck_db_commit"
    );
}
