use super::*;

#[test]
fn resume_bare_noops_when_no_interrupted_runs_and_accepts_positional_terminal_noop() {
    let project = TestProject::new();
    SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();

    let bare = run(["cdf", "--json", "--project", project.root_str(), "resume"]);

    assert_eq!(bare.exit_code, 0, "stderr: {}", bare.stderr);
    let bare_json = stderr_or_stdout_json(&bare.stdout);
    assert_eq!(bare_json["command"], "resume");
    assert_eq!(bare_json["result"]["state"], "no_interrupted_runs");
    assert_eq!(bare_json["result"]["writes"]["package"], false);
    assert_eq!(bare_json["result"]["writes"]["destination"], false);
    assert_eq!(bare_json["result"]["writes"]["checkpoint"], false);

    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let run_json = stderr_or_stdout_json(&run_result.stdout);
    let run_id = run_json["result"]["run_id"].as_str().unwrap();

    let result = resume_command(&project, run_id);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "resume");
    assert_eq!(json["result"]["state"], "terminal_success");
    assert_eq!(json["result"]["action"], "no_op");
    assert_eq!(json["result"]["source_contact"], false);
    assert_eq!(json["result"]["mutation_required"], false);
    assert_eq!(json["result"]["mutated"], false);
}

#[test]
fn resume_missing_state_path_error_has_code_and_project_path_context() {
    let project = TestProject::new();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "resume",
        "run-missing-state",
    ]);

    assert_eq!(result.exit_code, 5);
    let json = assert_json_error_code(&result, "CDF-STATE-RESUME-LEDGER");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains(".cdf/state.db")
    );
}

#[test]
fn resume_bare_selects_single_interrupted_run_and_fails_closed() {
    let project = TestProject::new();
    create_resume_run_with_events(
        &project,
        "run-resume-bare-single",
        &[RunEventKind::RunStarted, RunEventKind::RunFailed],
    );

    let result = run(["cdf", "--json", "--project", project.root_str(), "resume"]);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["run_id"], "run-resume-bare-single");
    assert_eq!(json["result"]["state"], "no_finalized_package");
    assert_eq!(
        json["result"]["action"],
        "rerun_extraction_from_last_committed_checkpoint"
    );
}

#[test]
fn resume_bare_multiple_interrupted_runs_fails_closed_without_mutation() {
    let project = TestProject::new();
    for run_id in ["run-resume-bare-first", "run-resume-bare-second"] {
        create_resume_run_with_events(&project, run_id, &[RunEventKind::RunStarted]);
    }

    let result = run(["cdf", "--json", "--project", project.root_str(), "resume"]);

    assert_eq!(result.exit_code, 78);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], true);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("bare resume found 2 interrupted runs")
    );
}

#[test]
fn resume_no_finalized_package_fails_closed_with_guidance() {
    let project = TestProject::new();
    let run_id = create_resume_run_with_events(
        &project,
        "run-resume-no-package",
        &[RunEventKind::RunStarted, RunEventKind::RunFailed],
    );

    let result = resume_command(&project, run_id.as_str());

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["state"], "no_finalized_package");
    assert_eq!(
        json["result"]["action"],
        "rerun_extraction_from_last_committed_checkpoint"
    );
    assert_eq!(json["result"]["recovery"]["result"], "failed_closed");
    assert!(
        json["result"]["recovery"]["guidance"]
            .as_str()
            .unwrap()
            .contains("no finalized package")
    );
}

#[test]
fn resume_human_headless_render_uses_recovery_panels_and_redacts_destination_uri() {
    let project = TestProject::new();
    write_project_destination(
        &project,
        "postgres://user:resume-render-secret@localhost/db",
    );
    let run_id = create_resume_run_with_events(
        &project,
        "run-resume-human-no-package",
        &[RunEventKind::RunStarted, RunEventKind::RunFailed],
    );

    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "resume",
        run_id.as_str(),
    ]);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    assert_secret_absent(&result, "resume-render-secret");
    assert!(
        result.stderr.contains("[plan] failed run failed"),
        "stderr:\n{}",
        result.stderr
    );
    for expected in [
        "ERR resume run run-resume-human-no-package failed closed",
        "Recovery",
        "Durable artifacts",
        "State",
        "Run ledger",
        "no_finalized_package",
        "mutation performed",
        "postgres://[redacted]@localhost/db",
        "Next: cdf run <resource>",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn resume_human_rich_render_uses_recovery_and_artifact_panels() {
    let project = TestProject::new();
    let run_id = create_resume_run_with_events(
        &project,
        "run-resume-rich-no-package",
        &[RunEventKind::RunStarted, RunEventKind::RunFailed],
    );
    let output = crate::resume_command::resume(
        &test_cli(&project),
        cdf_cli_core::args::ResumeArgs {
            run_id: Some(run_id.to_string()),
        },
        &test_execution_services(),
        &test_destination_registry(),
        cdf_cli_core::progress::ProgressDelivery::Buffered,
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    for expected in [
        "resume run run-resume-rich-no-package failed closed",
        "Recovery",
        "Durable artifacts",
        "State",
        "Run ledger",
        "no_finalized_package",
        "mutation performed",
        "cdf run <resource>",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn injected_quasar_destination_reaches_lock_plan_run_duplicate_replay_doctor_and_inspect() {
    let project = TestProject::new();
    let destination_uri = crate::destination_registry_test_support::destination_uri();
    let secret = crate::destination_registry_test_support::secret_sentinel();
    write_project_destination(&project, &destination_uri);
    let (registry, state) =
        crate::destination_registry_test_support::registry_with_quasar_destination().unwrap();

    let mut read_only_commands = vec![
        vec!["compile".to_owned(), "local.events".to_owned()],
        vec!["validate".to_owned()],
        vec!["contract".to_owned(), "freeze".to_owned()],
        vec!["diff".to_owned(), "schema".to_owned()],
        vec!["inspect".to_owned(), "destinations".to_owned()],
        vec!["doctor".to_owned()],
        vec!["plan".to_owned(), "local.events".to_owned()],
        vec!["package".to_owned(), "ls".to_owned()],
    ];
    read_only_commands.push(vec![
        "add".to_owned(),
        "local.copy".to_owned(),
        project
            .root
            .join("data/events.ndjson")
            .display()
            .to_string(),
        "--dry-run".to_owned(),
    ]);
    for command in read_only_commands {
        let command_label = command.join(" ");
        let result = run_injected_dynamic(&project, &registry, command);
        assert_eq!(
            result.exit_code, 0,
            "{command_label} failed; stdout: {}; stderr: {}",
            result.stdout, result.stderr
        );
        assert!(
            !result.stdout.contains(secret),
            "read-only command leaked fixture secret:\n{}",
            result.stdout
        );
        assert_secret_absent(&result, secret);
        assert_eq!(
            state.durable_commits(),
            0,
            "inspection, health, lock, and planning must not mutate the destination"
        );
    }
    assert!(project.root.join("cdf.lock").is_file());
    assert!(state.inspections() >= 3);
    assert!(state.health_checks() >= 2);
    assert!(state.resolutions() >= 1);

    let loaded = run_injected_dynamic(
        &project,
        &registry,
        vec!["run".to_owned(), "local.events".to_owned()],
    );
    assert_eq!(loaded.exit_code, 0, "stderr: {}", loaded.stderr);
    assert!(
        !loaded.stdout.contains(secret),
        "run leaked fixture secret:\n{}",
        loaded.stdout
    );
    assert_secret_absent(&loaded, secret);
    assert_eq!(state.durable_commits(), 1);
    assert_eq!(state.commit_begins(), 1);
    assert!(state.plans() >= 1);
    let package_dir = run_package_dir(&project, &loaded);

    for command in [
        vec![
            "schema".to_owned(),
            "show".to_owned(),
            "local.events".to_owned(),
        ],
        vec![
            "state".to_owned(),
            "show".to_owned(),
            "local.events".to_owned(),
        ],
        vec![
            "state".to_owned(),
            "history".to_owned(),
            "local.events".to_owned(),
        ],
        vec!["status".to_owned()],
        vec!["package".to_owned(), "ls".to_owned()],
    ] {
        let command_label = command.join(" ");
        let result = run_injected_dynamic(&project, &registry, command);
        assert_eq!(
            result.exit_code, 0,
            "{command_label} failed; stdout: {}; stderr: {}",
            result.stdout, result.stderr
        );
        assert_secret_absent(&result, secret);
    }

    remove_state_store(&project);
    let userinfo_uri = crate::destination_registry_test_support::destination_uri_with_userinfo();
    let replayed = run_injected_dynamic(
        &project,
        &registry,
        vec![
            "replay".to_owned(),
            "package".to_owned(),
            package_dir.display().to_string(),
            "--to".to_owned(),
            userinfo_uri.clone(),
        ],
    );
    assert_eq!(replayed.exit_code, 0, "stderr: {}", replayed.stderr);
    assert!(
        !replayed.stdout.contains(secret),
        "replay leaked fixture secret:\n{}",
        replayed.stdout
    );
    assert_secret_absent(&replayed, secret);
    assert_eq!(
        state.durable_commits(),
        1,
        "duplicate replay must not create another durable destination commit"
    );
    assert_eq!(state.commit_begins(), 2);
    assert!(state.receipt_verifications() >= 2);

    remove_state_store(&project);
    let human_replay = run_injected_human_dynamic(
        &project,
        &registry,
        vec![
            "replay".to_owned(),
            "package".to_owned(),
            package_dir.display().to_string(),
            "--to".to_owned(),
            userinfo_uri.clone(),
        ],
    );
    assert_eq!(human_replay.exit_code, 0, "stderr: {}", human_replay.stderr);
    assert_secret_absent(&human_replay, secret);
    assert_eq!(state.durable_commits(), 1);

    let errored = run_injected_dynamic(
        &project,
        &registry,
        vec![
            "replay".to_owned(),
            "package".to_owned(),
            project.root.join("missing-package").display().to_string(),
            "--to".to_owned(),
            userinfo_uri,
        ],
    );
    assert_ne!(errored.exit_code, 0);
    assert_secret_absent(&errored, secret);
}

#[test]
fn injected_quasar_destination_resume_replays_finalized_package_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let destination_uri = crate::destination_registry_test_support::destination_uri();
    let secret = crate::destination_registry_test_support::secret_sentinel();
    write_project_destination(&project, &destination_uri);
    let (registry, state) =
        crate::destination_registry_test_support::registry_with_quasar_destination().unwrap();
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-quasar-replay",
        &package_dir,
        &[RunEventKind::PackageFinalized, RunEventKind::RunFailed],
    );

    let result = run_injected_dynamic(
        &project,
        &registry,
        vec!["resume".to_owned(), run_id.to_string()],
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(
        !result.stdout.contains(secret),
        "resume replay leaked fixture secret:\n{}",
        result.stdout
    );
    assert_secret_absent(&result, secret);
    let report = &stderr_or_stdout_json(&result.stdout)["result"];
    assert_eq!(report["state"], "package_finalized_without_receipt");
    assert_eq!(report["action"], "replay_package");
    assert_eq!(report["source_contact"], false);
    assert_eq!(report["mutated"], true);
    assert_eq!(report["receipt"]["destination_id"], "quasar");
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["package"]["status"], "checkpointed");
    assert_eq!(state.durable_commits(), 1);
    assert_eq!(state.commit_begins(), 1);
    assert_eq!(package_receipt_count(&package_dir), 1);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert!(!project.root.join("data/events.ndjson").exists());
}

#[test]
fn injected_quasar_destination_resume_verifies_durable_receipt_without_duplicate_commit() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let destination_uri = crate::destination_registry_test_support::destination_uri();
    let secret = crate::destination_registry_test_support::secret_sentinel();
    write_project_destination(&project, &destination_uri);
    let (registry, state) =
        crate::destination_registry_test_support::registry_with_quasar_destination().unwrap();
    let run_id = seed_quasar_resume_receipt_before_checkpoint(
        &project,
        &package_dir,
        &destination_uri,
        &registry,
        "run-resume-quasar-receipt",
    );
    let commits_before_resume = state.durable_commits();
    let begins_before_resume = state.commit_begins();
    let verifications_before_resume = state.receipt_verifications();

    let result = run_injected_dynamic(
        &project,
        &registry,
        vec!["resume".to_owned(), run_id.to_string()],
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(
        !result.stdout.contains(secret),
        "resume receipt recovery leaked fixture secret:\n{}",
        result.stdout
    );
    assert_secret_absent(&result, secret);
    let report = &stderr_or_stdout_json(&result.stdout)["result"];
    assert_eq!(
        report["state"],
        "receipt_recorded_without_checkpoint_commit"
    );
    assert_eq!(report["action"], "verify_receipt_then_commit_checkpoint");
    assert_eq!(report["source_contact"], false);
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(state.durable_commits(), commits_before_resume);
    assert_eq!(state.commit_begins(), begins_before_resume);
    assert!(state.receipt_verifications() > verifications_before_resume);
    assert_eq!(package_receipt_count(&package_dir), 1);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert!(!project.root.join("data/events.ndjson").exists());
}

#[test]
fn resume_finalized_package_without_receipt_replays_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-replay",
        &package_dir,
        &[RunEventKind::PackageFinalized, RunEventKind::RunFailed],
    );

    let result = resume_command(&project, run_id.as_str());

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["state"], "package_finalized_without_receipt");
    assert_eq!(json["result"]["action"], "replay_package");
    assert_eq!(json["result"]["source_contact"], false);
    assert_eq!(json["result"]["mutation_required"], true);
    assert_eq!(json["result"]["mutated"], true);
    assert_eq!(json["result"]["package"]["status"], "checkpointed");
    assert_eq!(json["result"]["checkpoint"]["status"], "committed");
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(package_receipt_count(&package_dir), 1);
}

#[test]
fn resume_finalized_package_human_progress_replays_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-progress",
        &package_dir,
        &[RunEventKind::PackageFinalized, RunEventKind::RunFailed],
    );

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "resume".to_owned(),
        run_id.to_string(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join("data/events.ndjson").exists());
    assert_no_headless_progress_controls(&result.stdout);
    for expected in [
        "[package] failed run failed",
        "[verify] running destination receipt recorded",
        "[gate] succeeded run resumed",
    ] {
        assert!(
            result.stderr.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stderr
        );
    }
    for expected in [
        "OK resume run run-resume-progress completed",
        "source contact",
        "mutation performed  yes",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(package_receipt_count(&package_dir), 1);
}

#[test]
fn resume_finalized_postgres_package_without_receipt_replays_without_source_contact() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        format!(
            "{}?options=-csearch_path%3D{}\n",
            postgres.url, postgres.schema
        ),
    )
    .unwrap();
    let target = "events";
    let package_dir = create_replay_package_fixture(&project);
    write_project_destination(&project, "postgres://secret://file/destination-dsn");
    let mut reader = PackageReader::open(&package_dir).unwrap();
    let checkpoint_id = reader.replay_inputs().unwrap().state_delta.checkpoint_id;
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-postgres-replay",
        &package_dir,
        &[RunEventKind::PackageFinalized, RunEventKind::RunFailed],
    );

    let result = resume_command(&project, run_id.as_str());

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &postgres.url);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["state"], "package_finalized_without_receipt");
    assert_eq!(report["action"], "replay_package");
    assert_eq!(report["source_contact"], false);
    assert_eq!(report["mutated"], true);
    assert_eq!(report["package"]["status"], "checkpointed");
    assert_eq!(report["package"]["receipt_count"], 1);
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["receipt"]["destination_id"], "postgres");
    assert_eq!(report["receipt"]["target"], target);
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
    assert_eq!(package_receipt_count(&package_dir), 1);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("resume Postgres checkpoint head");
    assert_eq!(head.delta.checkpoint_id, checkpoint_id);
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt"]["receipt_id"].as_str().unwrap()
    );

    let mut client = postgres.client();
    let rows: i64 = client
        .query_one(
            &format!("SELECT COUNT(*)::bigint FROM {}", postgres.table("events")),
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(rows, 2);
}

#[test]
fn resume_durable_receipt_commits_uncommitted_checkpoint_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Packaged).unwrap();
    remove_package_receipts(&package_dir);
    let run_id =
        seed_resume_receipt_before_checkpoint(&project, &package_dir, "run-resume-receipt");

    let result = resume_command(&project, run_id.as_str());

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        json["result"]["state"],
        "receipt_recorded_without_checkpoint_commit"
    );
    assert_eq!(
        json["result"]["action"],
        "verify_receipt_then_commit_checkpoint"
    );
    assert_eq!(json["result"]["source_contact"], false);
    assert_eq!(json["result"]["mutated"], true);
    assert_eq!(json["result"]["checkpoint"]["status"], "committed");
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
}

#[test]
fn resume_committed_checkpoint_updates_stale_package_status_only() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    let package_dir = run_package_dir(&project, &run_result);
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Loading).unwrap();
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-stale-status",
        &package_dir,
        &[
            RunEventKind::PackageFinalized,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::CheckpointCommitted,
            RunEventKind::RunFailed,
        ],
    );

    let result = resume_command(&project, run_id.as_str());

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        json["result"]["state"],
        "checkpoint_committed_with_stale_package_status"
    );
    assert_eq!(json["result"]["action"], "update_package_status");
    assert_eq!(json["result"]["mutated"], true);
    assert_eq!(json["result"]["package"]["status"], "checkpointed");
    assert_eq!(package_status(&package_dir), PackageStatus::Checkpointed);
}

#[test]
fn resume_stale_package_status_fails_closed_when_current_head_is_different() {
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
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    let package_dir = run_package_dir(&project, &first);
    let mut reader = PackageReader::open(&package_dir).unwrap();
    reader.update_status(PackageStatus::Loading).unwrap();
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-wrong-head",
        &package_dir,
        &[
            RunEventKind::PackageFinalized,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::CheckpointCommitted,
            RunEventKind::RunFailed,
        ],
    );

    let result = resume_command(&project, run_id.as_str());

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        json["result"]["state"],
        "checkpoint_committed_head_not_exact"
    );
    assert_eq!(json["result"]["action"], "inspect_missing_artifacts");
    assert_eq!(json["result"]["mutated"], false);
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
}

#[test]
fn resume_stale_package_status_fails_closed_when_selected_receipt_differs_from_head() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    fs::remove_file(project.root.join("data/events.ndjson")).unwrap();
    let package_dir = run_package_dir(&project, &run_result);
    let mut reader = PackageReader::open(&package_dir).unwrap();
    let mut wrong_receipt = collect_package_receipts(&reader)[0].clone();
    wrong_receipt.receipt_id = ReceiptId::new("receipt-resume-wrong").unwrap();
    reader.append_receipt(wrong_receipt).unwrap();
    reader.update_status(PackageStatus::Loading).unwrap();
    let run_id = create_resume_run_with_package(
        &project,
        "run-resume-wrong-receipt",
        &package_dir,
        &[
            RunEventKind::PackageFinalized,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::CheckpointCommitted,
            RunEventKind::RunFailed,
        ],
    );

    let result = resume_command(&project, run_id.as_str());

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        json["result"]["state"],
        "checkpoint_committed_head_not_exact"
    );
    assert_eq!(json["result"]["mutated"], false);
    assert_eq!(package_status(&package_dir), PackageStatus::Loading);
}

#[test]
fn resume_missing_package_artifact_fails_closed_with_guidance() {
    let project = TestProject::new();
    let missing_package = project.root.join(".cdf/packages/pkg-resume-missing");
    let run_id = create_resume_run_with_missing_package(
        &project,
        "run-resume-missing-package",
        &missing_package,
    );

    let result = resume_command(&project, run_id.as_str());

    assert_eq!(result.exit_code, 1, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["result"]["state"], "missing_package_artifact");
    assert_eq!(json["result"]["action"], "inspect_missing_artifacts");
    assert_eq!(json["result"]["recovery"]["result"], "failed_closed");
    assert!(
        json["result"]["recovery"]["guidance"]
            .as_str()
            .unwrap()
            .contains("does not exist")
    );
}
