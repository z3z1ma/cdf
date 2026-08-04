use super::*;

#[test]
fn state_show_uses_sqlite_store_and_reports_missing_head() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "show",
        "local.events",
        "--pipeline",
        "pipeline-1",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "state show");
    assert!(json["result"]["head"].is_null());

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "state",
        "show",
        "local.events",
        "--pipeline",
        "pipeline-1",
    ]);

    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(!human.stdout.contains("\u{1b}["));
    for expected in [
        "WARN no committed state head",
        "Scope",
        "Head",
        "pipeline",
        "pipeline-1",
        "checkpoint",
        "none",
        "mutation performed",
        "Next: cdf state history local.events --pipeline pipeline-1",
    ] {
        assert!(
            human.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human.stdout
        );
    }
}

#[test]
fn state_followup_commands_render_scope_pairs_for_scope_json_objects() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "state",
        "show",
        "local.events",
        "--scope-json",
        r#"{"kind":"window","start":"0","end":"10"}"#,
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "Next: cdf state history local.events",
        "--scope kind=window",
        "--scope start=0",
        "--scope end=10",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
    assert!(
        !result.stdout.contains("--scope-json"),
        "follow-up command should teach --scope pairs:\n{}",
        result.stdout
    );
}

#[test]
fn state_product_grammar_uses_default_pipeline_scope_pairs_and_rewind_marker() {
    let project = TestProject::new();
    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
    ]);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    let first_checkpoint = stderr_or_stdout_json(&first.stdout)["result"]["checkpoint_id"]
        .as_str()
        .unwrap()
        .to_owned();
    fs::write(
        project.root.join("data/events.ndjson"),
        concat!(
            "{\"id\":1,\"updated_at\":1783296000000000}\n",
            "{\"id\":2,\"updated_at\":1783296060000000}\n",
            "{\"id\":3,\"updated_at\":1783296120000000}\n"
        ),
    )
    .unwrap();
    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
    ]);
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    let second_checkpoint = stderr_or_stdout_json(&second.stdout)["result"]["checkpoint_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let show = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "show",
        "local.events",
        "--scope",
        "kind=resource",
    ]);

    assert_eq!(show.exit_code, 0, "stderr: {}", show.stderr);
    let show_json = stderr_or_stdout_json(&show.stdout);
    assert_eq!(show_json["result"]["scope"]["kind"], "resource");
    assert_eq!(
        show_json["result"]["head"]["delta"]["pipeline_id"],
        "cdf-run"
    );
    assert_eq!(
        show_json["result"]["head"]["delta"]["checkpoint_id"],
        second_checkpoint
    );

    let history = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "history",
        "local.events",
        "--scope",
        "kind=resource",
    ]);

    assert_eq!(history.exit_code, 0, "stderr: {}", history.stderr);
    let history_json = stderr_or_stdout_json(&history.stdout);
    assert_eq!(
        history_json["result"]["history"].as_array().unwrap().len(),
        2
    );

    let human_show = run([
        "cdf",
        "--project",
        project.root_str(),
        "state",
        "show",
        "local.events",
        "--scope",
        "kind=resource",
    ]);
    assert_eq!(human_show.exit_code, 0, "stderr: {}", human_show.stderr);
    for expected in [
        "OK state head found",
        "Scope",
        "Head",
        &second_checkpoint,
        "Next: cdf state history local.events --scope kind=resource",
    ] {
        assert!(
            human_show.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human_show.stdout
        );
    }

    let human_history = run([
        "cdf",
        "--project",
        project.root_str(),
        "state",
        "history",
        "local.events",
        "--scope",
        "kind=resource",
    ]);
    assert_eq!(
        human_history.exit_code, 0,
        "stderr: {}",
        human_history.stderr
    );
    for expected in [
        "OK 2 checkpoint(s)",
        "checkpoint",
        &first_checkpoint,
        &second_checkpoint,
        "Next: cdf state show local.events --scope kind=resource",
    ] {
        assert!(
            human_history.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human_history.stdout
        );
    }

    let rewind = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "rewind",
        "local.events",
        "--scope",
        "kind=resource",
        "--to",
        &first_checkpoint,
    ]);

    assert_eq!(rewind.exit_code, 0, "stderr: {}", rewind.stderr);
    let rewind_json = stderr_or_stdout_json(&rewind.stdout);
    assert!(
        rewind_json["result"]["marker"]["delta"]["checkpoint_id"]
            .as_str()
            .unwrap()
            .starts_with("rewind-marker-")
    );
    assert_eq!(
        rewind_json["result"]["head"]["delta"]["checkpoint_id"],
        first_checkpoint
    );
    assert_eq!(
        rewind_json["result"]["packages_ahead"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn state_rewind_human_headless_render_reports_marker_and_packages_ahead() {
    let project = TestProject::new();
    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
    ]);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    let first_checkpoint = stderr_or_stdout_json(&first.stdout)["result"]["checkpoint_id"]
        .as_str()
        .unwrap()
        .to_owned();
    fs::write(
        project.root.join("data/events.ndjson"),
        concat!(
            "{\"id\":1,\"updated_at\":1783296000000000}\n",
            "{\"id\":2,\"updated_at\":1783296060000000}\n",
            "{\"id\":3,\"updated_at\":1783296120000000}\n"
        ),
    )
    .unwrap();
    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
    ]);
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);

    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "state",
        "rewind",
        "local.events",
        "--scope",
        "kind=resource",
        "--to",
        &first_checkpoint,
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        &format!("OK rewound to {first_checkpoint}"),
        "Rewind",
        "marker              rewind-marker-",
        "packages ahead      1",
        "rewind marker checkpoint appended",
        "package ahead of state",
        "Next: cdf state show local.events --scope kind=resource",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn state_show_human_rich_render_uses_scope_and_head_panels() {
    let project = TestProject::new();
    let run_result = run_valid_run_args(&project);
    assert_eq!(run_result.exit_code, 0, "stderr: {}", run_result.stderr);
    let checkpoint_id = stderr_or_stdout_json(&run_result.stdout)["result"]["checkpoint_id"]
        .as_str()
        .unwrap()
        .to_owned();

    let output = crate::state_command::state(
        &test_cli(&project),
        cdf_cli_core::args::StateCommand::Show(cdf_cli_core::args::StateScopeArgs {
            pipeline_id: Some("cdf-run".to_owned()),
            resource_id: "local.events".to_owned(),
            scope_json: None,
            scope: vec!["kind=resource".to_owned()],
        }),
        &test_execution_services(),
        &test_destination_registry(),
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "state head found",
        "Scope",
        "Head",
        checkpoint_id.as_str(),
        "cdf state history local.events --pipeline cdf-run --scope kind=resource",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn state_show_renders_typed_table_snapshot_authority() {
    let project = TestProject::new();
    let position = SourcePosition::TableSnapshot(Box::new(TableSnapshotPosition {
        version: CHECKPOINT_STATE_VERSION,
        protocol: "iceberg".to_owned(),
        catalog: "glue:us-east-1:123456789012".to_owned(),
        namespace: vec!["analytics".to_owned(), "curated".to_owned()],
        table: "orders".to_owned(),
        selector: TableSnapshotSelector::Branch {
            name: "main".to_owned(),
        },
        snapshot_id: 42,
        sequence_number: 7,
        parent_snapshot_id: Some(41),
        metadata_location: "s3://warehouse/analytics/orders/metadata/v42.json".to_owned(),
        metadata_generation: "version-id:v42".to_owned(),
    }));
    let package_hash = "package-table-snapshot";
    let mut delta = status_delta("cdf-run", "checkpoint-table-snapshot", package_hash);
    delta.output_position = position.clone();
    delta.segments[0].output_position = position;
    let checkpoint_id = delta.checkpoint_id.clone();
    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    store.propose(delta).unwrap();
    store
        .commit(
            &checkpoint_id,
            status_receipt(package_hash, "receipt-table-snapshot", 1_700_000_000_000),
        )
        .unwrap();

    let output = crate::state_command::state(
        &test_cli(&project),
        cdf_cli_core::args::StateCommand::Show(cdf_cli_core::args::StateScopeArgs {
            pipeline_id: Some("cdf-run".to_owned()),
            resource_id: "local.events".to_owned(),
            scope_json: None,
            scope: vec!["kind=resource".to_owned()],
        }),
        &test_execution_services(),
        &test_destination_registry(),
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "source position",
        "table_snapshot",
        "table protocol",
        "iceberg",
        "glue:us-east-1:123456789012",
        "analytics.curated.orders",
        "branch:main",
        "snapshot",
        "42",
        "sequence",
        "7",
        "parent snapshot",
        "41",
        "version-id:v42",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }

    let json_result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "state",
        "show",
        "local.events",
        "--pipeline",
        "cdf-run",
    ]);
    assert_eq!(json_result.exit_code, 0, "stderr: {}", json_result.stderr);
    let json = stderr_or_stdout_json(&json_result.stdout);
    assert_eq!(
        json["result"]["head"]["delta"]["output_position"]["kind"],
        "table_snapshot"
    );
    assert_eq!(
        json["result"]["head"]["delta"]["output_position"]["snapshot_id"],
        42
    );
}

#[test]
fn compiler_planning_frontier_comes_from_the_default_pipeline_head() {
    let project = TestProject::new();
    commit_status_head(
        &project,
        "cdf-run",
        "checkpoint-planning-frontier",
        "package-planning-frontier",
        "receipt-planning-frontier",
        1_700_000_000_000,
    );
    let context = crate::context::ProjectContext::load(Some(&project.root), None).unwrap();
    let descriptor = context.resource("local.events").unwrap().descriptor();

    let frontier = crate::scan_command::planning_frontier(
        &context,
        descriptor,
        &PipelineId::new("cdf-run").unwrap(),
    )
    .unwrap();

    assert_eq!(
        frontier,
        Some(SourcePosition::Cursor(CursorPosition {
            version: CHECKPOINT_STATE_VERSION,
            field: "updated_at".to_owned(),
            value: CursorValue::I64(42),
        }))
    );
}

#[test]
fn state_recover_commits_verified_package_receipt_without_destination_rows() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let reader = PackageReader::open(&package_dir).unwrap();
    let package_hash = reader.manifest().package_hash.clone();
    let package_id = reader.manifest().identity.package_id.clone();
    let checkpoint_id = reader.replay_inputs().unwrap().state_delta.checkpoint_id;
    let receipt_id = collect_package_receipts(&reader)[0].receipt_id.to_string();
    let destination_path = project.root.join(".cdf/dev.duckdb");
    let rows_before = duckdb_event_count(&destination_path);

    let result = state_recover_command(
        &project,
        &package_dir,
        "duckdb://.cdf/dev.duckdb",
        None,
        None,
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "state recover");
    assert_eq!(report["command"], "state recover");
    assert_eq!(report["package_id"], package_id);
    assert_eq!(report["package_hash"], package_hash);
    assert_eq!(report["selected_receipt_id"], receipt_id);
    assert_eq!(report["receipt_selection"], "single_durable_receipt");
    assert_eq!(report["destination"]["kind"], "duckdb");
    assert_eq!(report["destination"]["destination_id"], "duckdb");
    assert_eq!(report["checkpoint_id"], checkpoint_id.as_str());
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["checkpoint"]["is_head"], true);
    assert_eq!(report["receipt_source"], "supplied_durable_receipt");
    assert_eq!(report["writes"]["destination_rows"], false);
    assert_eq!(report["writes"]["checkpoint"], true);
    assert!(
        report["evidence_limits"]
            .as_array()
            .unwrap()
            .iter()
            .any(|limit| limit.as_str().unwrap().contains("quarantine lineage"))
    );
    assert_eq!(duckdb_event_count(&destination_path), rows_before);
    assert_eq!(package_receipt_count(&package_dir), 1);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("state recover checkpoint head");
    assert_eq!(head.delta.checkpoint_id, checkpoint_id);
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.to_string(),
        receipt_id
    );
}

#[test]
fn state_recover_human_headless_render_reports_receipt_checkpoint_and_limits() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let checkpoint_id = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta
        .checkpoint_id;

    let result = run_dynamic(vec![
        "cdf".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "state".to_owned(),
        "recover".to_owned(),
        "--package".to_owned(),
        package_dir.to_str().unwrap().to_owned(),
        "--to".to_owned(),
        "duckdb://.cdf/dev.duckdb".to_owned(),
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        &format!("OK recovered checkpoint {checkpoint_id}"),
        "Recovery",
        "Checkpoint",
        "Effects",
        "destination rows  no",
        "verified receipt only; destination rows were not written",
        "evidence limit:",
        "does not reconstruct quarantine lineage",
        "Next: cdf inspect package ",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn state_recover_explicit_receipt_disambiguates_multiple_package_receipts() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let reader = PackageReader::open(&package_dir).unwrap();
    let mut receipts = collect_package_receipts(&reader);
    let selected_receipt_id = receipts[0].receipt_id.to_string();
    receipts[0].receipt_id = ReceiptId::new("receipt-state-recover-extra").unwrap();
    reader.append_receipt(receipts[0].clone()).unwrap();
    let rows_before = duckdb_event_count(project.root.join(".cdf/dev.duckdb"));

    let result = state_recover_command(
        &project,
        &package_dir,
        "duckdb://.cdf/dev.duckdb",
        Some(&selected_receipt_id),
        None,
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["receipt_selection"], "explicit");
    assert_eq!(report["selected_receipt_id"], selected_receipt_id);
    assert_eq!(report["receipt_id"], selected_receipt_id);
    assert_eq!(
        duckdb_event_count(project.root.join(".cdf/dev.duckdb")),
        rows_before
    );
}

#[test]
fn state_recover_fails_closed_on_zero_or_ambiguous_package_receipts() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    remove_package_receipts(&package_dir);

    let missing = state_recover_command(
        &project,
        &package_dir,
        "duckdb://.cdf/dev.duckdb",
        None,
        None,
    );

    assert_eq!(missing.exit_code, 3);
    assert!(
        !project.root.join(".cdf/state.db").exists(),
        "missing receipt recovery must not create checkpoint state"
    );
    let missing_json = stderr_or_stdout_json(&missing.stderr);
    assert!(
        missing_json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("found zero")
    );

    let ambiguous_project = TestProject::new();
    let package_dir = create_replay_package_fixture(&ambiguous_project);
    let reader = PackageReader::open(&package_dir).unwrap();
    let mut duplicate = collect_package_receipts(&reader)[0].clone();
    duplicate.receipt_id = ReceiptId::new("receipt-state-recover-ambiguous-extra").unwrap();
    reader.append_receipt(duplicate).unwrap();

    let ambiguous = state_recover_command(
        &ambiguous_project,
        &package_dir,
        "duckdb://.cdf/dev.duckdb",
        None,
        None,
    );

    assert_eq!(ambiguous.exit_code, 3);
    assert!(
        !ambiguous_project.root.join(".cdf/state.db").exists(),
        "ambiguous receipt recovery must not create checkpoint state"
    );
    let ambiguous_json = stderr_or_stdout_json(&ambiguous.stderr);
    assert!(
        ambiguous_json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("pass --receipt")
    );
}
