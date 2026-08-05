use super::*;

#[test]
fn plan_json_exposes_pushdown_ddl_guarantee_and_state_advancement() {
    let project = TestProject::new();
    let package_root = project.root.join(".cdf/packages");
    let state_path = project.root.join(".cdf/state.db");
    let duckdb_path = project.root.join(".cdf/dev.duckdb");
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--select",
        "id,updated_at",
        "--filter",
        "id > 10",
        "--limit",
        "5",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!package_root.exists(), "plan must not create package root");
    assert!(!state_path.exists(), "plan must not create state store");
    assert!(
        !duckdb_path.exists(),
        "plan must not create destination data"
    );
    let json = stderr_or_stdout_json(&result.stdout);
    let result = &json["result"];
    assert_eq!(result["resource_id"], "local.events");
    assert!(
        result["resource_schema"]["schema_hash"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(result["resource_schema"]["fields"][0]["name"], "id");
    assert_eq!(result["will_fetch"]["limit"], 5);
    assert!(
        result["scheduler"]["effective_jobs"]["jobs"]
            .as_u64()
            .is_some_and(|jobs| jobs >= 1)
    );
    assert!(
        result["scheduler"]["managed_memory_available_bytes"]
            .as_u64()
            .is_some_and(|bytes| bytes > 0)
    );
    assert_eq!(result["scheduler"]["destination_writer_concurrency"], 1);
    assert_eq!(
        result["pushdown"]["unsupported"][0]["fidelity"],
        "unsupported"
    );
    assert_eq!(result["destination"]["destination_id"], "duckdb");
    assert_eq!(result["destination"]["target"], "events");
    assert_eq!(result["destination"]["disposition"], "append");
    assert_eq!(result["destination"]["idempotency"], "package_token");
    assert_eq!(result["ddl_preview"]["supported"], true);
    assert_eq!(result["ddl_preview"]["migration_support"], "supported");
    assert!(
        result["ddl_preview"]["migrations"][0]["description"]
            .as_str()
            .unwrap()
            .contains("CREATE TABLE")
    );
    assert_eq!(result["delivery_guarantee"], "effectively_once_per_package");
    assert_eq!(
        result["delivery_guarantee_detail"]["qualifier"],
        "per_package"
    );
    assert_eq!(
        result["state_advancement"]["advances_after"],
        "destination receipt is recorded and CheckpointStore::commit verifies coverage"
    );
}

#[test]
fn plan_human_headless_render_prioritizes_decision_summary() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--select",
        "id,updated_at",
        "--filter",
        "id > 10",
        "--limit",
        "5",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        "OK plan local.events -> events",
        "Plan",
        "execution",
        "bounded",
        "jobs",
        "Attention",
        "unsupported pushdowns",
        "effectively_once_per_package",
        "migrations",
        "Next: cdf run local.events",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
    assert!(!result.stdout.contains("managed memory available"));
    assert!(!result.stdout.contains("advances after"));
}

#[test]
fn plan_human_rich_render_uses_glyphs_color_and_operator_panels() {
    let project = TestProject::new();
    let mut cli = test_cli(&project);
    cli.terminal.verbosity = cdf_cli_core::terminal::Verbosity::Verbose(1);
    let services = test_execution_services();
    let output = crate::scan_command::plan_or_explain(
        &cli,
        cdf_cli_core::args::ScanArgs {
            resource_id: "local.events".to_owned(),
            destination_uri: None,
            projection: Some(vec!["id".to_owned(), "updated_at".to_owned()]),
            filters: vec!["id > 10".to_owned()],
            limit: Some(5),
            order_by: Vec::new(),
            no_pin: false,
            segmentation: cdf_cli_core::args::SegmentationArgs::default(),
        },
        "plan",
        &services,
        &test_destination_registry(),
    )
    .unwrap();
    let result = render_verbose_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "plan local.events -> events",
        "Pushdown",
        "Destination",
        "Guarantee",
        "Contract",
        "Migration",
        "cdf run local.events",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn plan_human_next_command_preserves_explicit_destination_with_canonical_target() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
        "--to",
        "duckdb://.cdf/plan-explicit.duckdb",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(
        result
            .stdout
            .contains("Next: cdf run local.events --to duckdb://.cdf/plan-explicit.duckdb"),
        "stdout:\n{}",
        result.stdout
    );
    assert!(!result.stdout.contains("--package-id"));
    assert!(!result.stdout.contains("--checkpoint-id"));
}

#[test]
fn explain_json_exposes_destination_plan_without_writes() {
    let project = TestProject::new();
    let override_path = project.root.join(".cdf/explain.duckdb");
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "explain",
        "local.events",
        "--to",
        "duckdb://.cdf/explain.duckdb",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    assert!(!override_path.exists());
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "explain");
    let report = &json["result"];
    assert_eq!(report["destination"]["target"], "events");
    assert!(
        report["destination"]["label"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/explain.duckdb")
    );
    assert_eq!(report["ddl_preview"]["supported"], true);
    assert_eq!(report["delivery_guarantee"], "effectively_once_per_package");
    assert_eq!(report["explain"]["execution_extent"]["kind"], "bounded");
    assert!(report["explain"].get("compiled_stream_policy").is_none());
}

#[test]
fn explain_human_headless_render_uses_operator_panels() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "explain",
        "local.events",
        "--to",
        "duckdb://.cdf/explain-render.duckdb",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        "OK explain local.events -> events",
        "Plan",
        "effectively_once_per_package",
        ".cdf/explain-render.duckdb",
        "Next: cdf run local.events --to duckdb://.cdf/explain-render.duckdb",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn backfill_dry_plan_splits_postgres_cursor_windows_without_writes() {
    let project = TestProject::new();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/postgres-dsn"),
    );
    fs::write(
        project.root.join("cdf/warehouse/orders.cdf.sql"),
        postgres_resource_sql("orders", true),
    )
    .unwrap();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "backfill",
        "warehouse.orders",
        "--from",
        "0",
        "--to",
        "25",
        "--target",
        "orders",
        "--slice-size",
        "10",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(json["command"], "backfill");
    let report = &json["result"];
    assert_eq!(report["mode"], "dry_plan");
    assert_eq!(report["resource_id"], "warehouse.orders");
    assert_eq!(report["target"], "orders");
    assert_eq!(report["requested"]["from"], "0");
    assert_eq!(report["requested"]["to"], "25");
    assert_eq!(report["requested"]["slice_size"], 10);
    assert_eq!(report["writes"]["package"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["writes"]["checkpoint"], false);
    assert_eq!(report["slices"].as_array().unwrap().len(), 3);
    assert_eq!(report["slices"][0]["start"], "0");
    assert_eq!(report["slices"][0]["end"], "10");
    assert_eq!(
        report["slices"][0]["filters"],
        json!(["updated_at >= 0", "updated_at < 10"])
    );
    assert_eq!(report["slices"][0]["scope"]["kind"], "window");
    assert_eq!(report["slices"][0]["status"], "planned");
    assert_eq!(report["slices"][0]["reason"], "dry_plan_only");
    assert!(
        report["slices"][0]["package_id"]
            .as_str()
            .unwrap()
            .starts_with("cdf-backfill-pkg-")
    );

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "backfill",
        "warehouse.orders",
        "--from",
        "0",
        "--to",
        "25",
        "--target",
        "orders",
        "--slice-size",
        "10",
    ]);

    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert!(!human.stdout.contains("\u{1b}["));
    for expected in [
        "OK planned backfill warehouse.orders -> orders",
        "Backfill",
        "Effects",
        "dry plan only; no package, destination, checkpoint, or run-ledger writes",
        "slice  window  status",
        "Next: cdf backfill warehouse.orders --from 0 --to 25 --target orders --execute",
    ] {
        assert!(
            human.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            human.stdout
        );
    }
}

#[test]
fn backfill_human_rich_render_uses_plan_panels_and_slice_table() {
    let project = TestProject::new();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/postgres-dsn"),
    );
    fs::write(
        project.root.join("cdf/warehouse/orders.cdf.sql"),
        postgres_resource_sql("orders", true),
    )
    .unwrap();

    let (host, services) =
        cdf_engine::StandaloneExecutionHost::default_services(64 * 1024 * 1024).unwrap();
    let output = crate::backfill_command::backfill(
        &test_cli(&project),
        cdf_cli_core::args::BackfillArgs {
            resource_id: "warehouse.orders".to_owned(),
            from: "0".to_owned(),
            to: "20".to_owned(),
            target: Some("orders".to_owned()),
            execute: false,
            slice_size: Some(10),
            segmentation: cdf_cli_core::args::SegmentationArgs::default(),
        },
        (host.as_ref(), &services),
        &test_destination_registry(),
        cdf_cli_core::progress::ProgressDelivery::Buffered,
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        "planned backfill warehouse.orders -> orders",
        "Backfill",
        "Effects",
        "dry plan only; no package, destination, checkpoint, or run-ledger writes",
        "slice  window  status",
        "cdf backfill warehouse.orders --from 0 --to 20 --target orders --execute",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn backfill_rejects_removed_resource_alias_before_project_load() {
    let result = run([
        "cdf",
        "--json",
        "backfill",
        "local.events",
        "--resource",
        "other.events",
        "--from",
        "0",
        "--to",
        "10",
        "--target",
        "events",
    ]);

    assert_eq!(result.exit_code, 2);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--resource")
    );
}

#[test]
fn backfill_rejects_file_resource_without_runtime_writes() {
    let project = TestProject::new();

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "backfill",
        "local.events",
        "--from",
        "0",
        "--to",
        "10",
    ]);

    assert_eq!(result.exit_code, 3);
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("no cursor")
    );
}

#[test]
fn backfill_execute_postgres_cursor_window_commits_window_scope() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = seed_ordered_cursor_table(
        &postgres,
        "backfill_source_orders",
        "(1, 5), (2, 15), (3, 25)",
    );
    let project = TestProject::new();
    let source_dsn = write_postgres_project_with_secret(&project, &postgres, &table);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "backfill",
        "warehouse.orders",
        "--from",
        "0",
        "--to",
        "20",
        "--target",
        "orders",
        "--execute",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &source_dsn);
    assert!(!result.stdout.contains("Run progress"));
    assert!(!result.stderr.contains("[plan]"));
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["mode"], "execute");
    assert_eq!(report["writes"]["package"], true);
    assert_eq!(report["writes"]["destination"], true);
    assert_eq!(report["writes"]["checkpoint"], true);
    assert_eq!(report["slices"].as_array().unwrap().len(), 1);
    let slice = &report["slices"][0];
    assert_eq!(
        slice["scope"],
        json!({ "kind": "window", "start": "0", "end": "20" })
    );
    assert_eq!(slice["status"], "succeeded");
    assert_eq!(slice["executed"]["row_count"], 2);
    assert_eq!(slice["executed"]["destination"]["destination_id"], "duckdb");

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM orders", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let window_scope = ScopeKey::Window {
        start: "0".to_owned(),
        end: "20".to_owned(),
    };
    let window_head = store
        .head(
            &PipelineId::new("cdf-backfill").unwrap(),
            &ResourceId::new("warehouse.orders").unwrap(),
            &window_scope,
        )
        .unwrap()
        .expect("backfill window checkpoint head");
    assert_eq!(
        window_head.delta.checkpoint_id.as_str(),
        slice["checkpoint_id"].as_str().unwrap()
    );
    assert!(
        store
            .head(
                &PipelineId::new("cdf-backfill").unwrap(),
                &ResourceId::new("warehouse.orders").unwrap(),
                &ScopeKey::Resource,
            )
            .unwrap()
            .is_none(),
        "backfill must not advance the resource-scope head"
    );
}

#[test]
fn backfill_execute_human_progress_reports_each_slice_and_summary() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = seed_ordered_cursor_table(
        &postgres,
        "backfill_progress_orders",
        "(1, 5), (2, 15), (3, 25)",
    );
    let project = TestProject::new();
    let source_dsn = write_postgres_project_with_secret(&project, &postgres, &table);

    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "backfill",
        "warehouse.orders",
        "--from",
        "0",
        "--to",
        "20",
        "--target",
        "orders",
        "--slice-size",
        "10",
        "--execute",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &source_dsn);
    assert!(!result.stdout.contains("\u{1b}["));
    for expected in [
        "[plan] running plan recorded",
        "scope=window:0..10",
        "scope=window:10..20",
        "[gate] succeeded run succeeded",
    ] {
        assert!(
            result.stderr.contains(expected),
            "missing {expected:?} in stderr:\n{}",
            result.stderr
        );
    }
    for expected in [
        "OK executed backfill warehouse.orders -> orders",
        "Summary",
        "slices succeeded  2/2",
        "rows              2",
        "segments          2",
        "Next: cdf state history <resource>",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in stdout:\n{}",
            result.stdout
        );
    }
}

#[test]
fn backfill_execute_human_failure_reports_failed_slice_and_recovery_guidance() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = seed_ordered_cursor_table(&postgres, "backfill_progress_failure_orders", "(1, 5)");
    let project = TestProject::new();
    let source_dsn = write_postgres_project_with_secret(&project, &postgres, &table);

    let args = || {
        vec![
            "cdf".to_owned(),
            "--project".to_owned(),
            project.root_str().to_owned(),
            "backfill".to_owned(),
            "warehouse.orders".to_owned(),
            "--from".to_owned(),
            "0".to_owned(),
            "--to".to_owned(),
            "10".to_owned(),
            "--target".to_owned(),
            "orders".to_owned(),
            "--execute".to_owned(),
        ]
    };
    let first = run_dynamic(args());
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    assert_secret_absent(&first, &source_dsn);

    let second = run_dynamic(args());
    assert_secret_absent(&second, &source_dsn);

    assert_ne!(second.exit_code, 0);
    assert!(second.stdout.is_empty());
    assert!(!second.stderr.contains("\u{1b}["));
    for expected in [
        "backfill slice 1 (0..10) failed",
        "package cdf-backfill-pkg-",
        "checkpoint cdf-backfill-cp-",
        "mutation status:",
        "next recovery command:",
        "not available before a run id is recorded",
    ] {
        assert!(
            second.stderr.contains(expected),
            "missing {expected:?} in:\n{}",
            second.stderr
        );
    }
    assert!(!second.stderr.contains("suggestions:"));
    assert!(!second.stderr.contains("cdf resume "));
}

#[test]
fn plan_json_derives_merge_guarantee_per_key() {
    let project = TestProject::new();
    write_resource_disposition(&project, "merge");
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["destination"]["disposition"], "merge");
    assert_eq!(report["delivery_guarantee"], "effectively_once_per_key");
    assert_eq!(report["delivery_guarantee_detail"]["qualifier"], "per_key");
    assert!(
        !project.root.join(".cdf/packages").exists(),
        "merge plan must not create package root"
    );
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
}

#[test]
fn plan_unsupported_destination_disposition_fails_closed_without_writes() {
    let project = TestProject::new();
    write_project_destination(&project, "parquet://.cdf/parquet");
    write_resource_disposition(&project, "merge");

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "plan",
        "local.events",
    ]);

    assert_ne!(result.exit_code, 0);
    let json = stderr_or_stdout_json(&result.stderr);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("Parquet")
    );
    assert!(
        !result.stdout.contains("effectively_once"),
        "unsupported plan must not pretend a delivery guarantee"
    );
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    assert!(
        !project.root.join(".cdf/parquet").exists(),
        "Parquet no-write planning must not create the destination root"
    );
}
