use super::*;

fn publish_replay_fixture_schema_v2(project: &TestProject) {
    let context = crate::context::ProjectContext::load_with_destination_registry(
        Some(&project.root),
        None,
        &test_destination_registry(),
    )
    .unwrap();
    let active =
        crate::schema_authority::load_active(&context, &ResourceId::new("local.events").unwrap())
            .unwrap()
            .unwrap();
    let state_path = context.state_store_path().unwrap();
    let store = SqliteSchemaPromotionStore::open(&state_path).unwrap();
    let promotion_id = cdf_kernel::PromotionId::new("package-replay-schema-v2").unwrap();
    let proposed = cdf_kernel::SchemaVersion::new(
        cdf_kernel::CanonicalArrowSchema::from_arrow(&Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("updated_at", DataType::Int64, false),
            Field::new("promoted", DataType::Utf8, true),
        ]))
        .unwrap(),
        Some(active.head.schema_hash.clone()),
        None,
        now_ms_for_test(),
        cdf_kernel::SchemaVersionProvenance::Promotion {
            promotion_id: promotion_id.clone(),
        },
    )
    .unwrap();
    let lease = cdf_kernel::ScopeLeaseStore::acquire(
        &store,
        active.head.key.promotion_scope().unwrap(),
        LeaseOwnerId::new("package-replay-schema-promoter").unwrap(),
        30_000,
    )
    .unwrap();
    let fence = cdf_kernel::SchemaPromotionFence::new(
        active.head.key.authority_domain_id.clone(),
        promotion_id,
        lease,
    )
    .unwrap();
    let target = cdf_kernel::SchemaPromotionTarget {
        destination_id: DestinationId::new("local-test").unwrap(),
        target: TargetName::new("events").unwrap(),
    };
    let plan = cdf_kernel::SchemaPromotionPlanState::new(
        fence.promotion_id.clone(),
        "{}".to_owned(),
        vec![target.clone()],
        Vec::new(),
        now_ms_for_test(),
    )
    .unwrap();
    SchemaAuthorityStore::begin_promotion(&store, &active.head, proposed.clone(), plan, &fence)
        .unwrap();
    let promoting = SchemaAuthorityStore::head(&store, &active.head.key)
        .unwrap()
        .unwrap();
    SchemaAuthorityStore::establish_promotion_cutoff(&store, &promoting, &fence).unwrap();

    let output_position = SourcePosition::Cursor(CursorPosition {
        version: cdf_kernel::SOURCE_POSITION_VERSION,
        field: "updated_at".to_owned(),
        value: CursorValue::I64(1),
    });
    let package_hash = PackageHash::new("package-schema-v2-correction").unwrap();
    let checkpoint_id = CheckpointId::new("checkpoint-schema-v2-correction").unwrap();
    let segment_id = SegmentId::new("segment-schema-v2-correction").unwrap();
    let delta = StateDelta {
        checkpoint_id: checkpoint_id.clone(),
        pipeline_id: PipelineId::new("schema-v2-promotion").unwrap(),
        resource_id: ResourceId::new("local.events").unwrap(),
        scope: fence.lease.scope.clone(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position: output_position.clone(),
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        package_hash: package_hash.clone(),
        schema_hash: proposed.schema_hash.clone(),
        segments: vec![StateSegment {
            segment_id: segment_id.clone(),
            scope: fence.lease.scope.clone(),
            output_position,
            row_count: 1,
            byte_count: 8,
        }],
    };
    CheckpointStore::propose(&store, delta).unwrap();
    let receipt = Receipt {
        receipt_id: ReceiptId::new("receipt-schema-v2-correction").unwrap(),
        destination: target.destination_id.clone(),
        target: target.target.clone(),
        package_hash: package_hash.clone(),
        segment_acks: vec![SegmentAck {
            segment_id,
            row_count: 1,
            byte_count: 8,
        }],
        disposition: WriteDisposition::Append,
        idempotency_token: IdempotencyToken::new(package_hash.to_string()).unwrap(),
        transaction: None,
        counts: CommitCounts {
            rows_written: 1,
            rows_inserted: Some(1),
            rows_updated: Some(0),
            rows_deleted: Some(0),
        },
        schema_hash: proposed.schema_hash,
        migrations: Vec::new(),
        committed_at_ms: now_ms_for_test(),
        verify: VerifyClause {
            kind: "schema-promotion-test".to_owned(),
            statement: "select 1".to_owned(),
            parameters: BTreeMap::new(),
        },
    };
    SchemaAuthorityStore::commit_promotion_target(
        &store,
        &promoting,
        &fence,
        &target,
        &checkpoint_id,
        receipt,
    )
    .unwrap();
    let published = SchemaAuthorityStore::publish_promotion(&store, &promoting, &fence).unwrap();
    assert_eq!(published.generation, 2);
}

#[test]
fn replay_package_fences_v1_before_destination_mutation_after_v2_publication() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let receipt_count = package_receipt_count(&package_dir);
    let initial_status = package_status(&package_dir);
    publish_replay_fixture_schema_v2(&project);
    let destination = project.root.join(".cdf/stale-package.duckdb");

    let result =
        replay_package_command(&project, &package_dir, "duckdb://.cdf/stale-package.duckdb");

    assert_ne!(result.exit_code, 0, "{}", result.stdout);
    assert!(
        result.stdout.contains("schema authority") || result.stderr.contains("schema authority"),
        "stdout: {}\nstderr: {}",
        result.stdout,
        result.stderr
    );
    assert_eq!(package_receipt_count(&package_dir), receipt_count);
    assert_eq!(package_status(&package_dir), initial_status);
    if destination.exists() {
        let connection = DuckConnection::open(destination).unwrap();
        let event_tables: i64 = connection
            .query_row(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'events'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            event_tables, 0,
            "stale package must not create its target table"
        );
    }
}

#[test]
fn run_package_requires_and_uses_explicit_destination_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let package_id = PackageReader::open(&package_dir)
        .unwrap()
        .manifest()
        .identity
        .package_id
        .clone();
    fs::write(
        project.root.join("cdf/local/events.cdf.sql"),
        "this authored SQL is deliberately invalid after package finalization",
    )
    .unwrap();
    let config = fs::read_to_string(project.root.join("cdf.toml")).unwrap();
    fs::write(
        project.root.join("cdf.toml"),
        config.replace(
            "root = \"data\"",
            "root = \"https://source.example.invalid/data\"\ncredentials = \"secret://env/CDF_TEST_MISSING_SOURCE_SECRET\"",
        ),
    )
    .unwrap();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "--package",
        package_dir.to_str().unwrap(),
        "--to",
        "duckdb://.cdf/dev.duckdb",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(report["package_id"], package_id);
    assert_eq!(report["target"], "events");
    assert!(
        report["destination"]["database_path"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/dev.duckdb")
    );
    assert_eq!(duckdb_event_count(project.root.join(".cdf/dev.duckdb")), 2);
}

#[test]
fn replay_package_missing_package_rejects_before_duckdb_parent_creation() {
    let project = TestProject::new();
    let package_dir = project.root.join(".cdf/packages/missing-package");
    let destination_parent = project.root.join(".cdf/new-replay-parent");
    let result = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/new-replay-parent/replay.duckdb",
    );

    assert_ne!(result.exit_code, 0);
    assert!(
        !destination_parent.exists(),
        "missing package replay must not create destination parent"
    );
    assert!(
        !project.root.join(".cdf/state.db").exists(),
        "missing package replay must not create checkpoint state"
    );
}

#[test]
fn replay_package_duckdb_replays_from_artifacts_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let manifest = PackageReader::open(&package_dir)
        .unwrap()
        .manifest()
        .clone();

    let result = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/replay-success.duckdb",
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "run");
    assert_eq!(report["command"], "run");
    assert_eq!(report["input_authority"], "package");
    assert_eq!(report["effect_ceiling"], "execute");
    assert!(!report["run_id"].as_str().unwrap().is_empty());
    assert_eq!(report["package_id"], manifest.identity.package_id.as_str());
    assert_eq!(report["package_hash"], manifest.package_hash);
    assert_eq!(report["destination"]["kind"], "duckdb");
    assert_eq!(report["destination"]["destination_id"], "duckdb");
    assert!(
        report["destination"]["database_path"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/replay-success.duckdb")
    );
    assert_eq!(report["target"], "events");
    assert_eq!(report["receipt"]["destination_id"], "duckdb");
    assert_eq!(report["receipt"]["target"], "events");
    assert_eq!(report["receipt"]["package_hash"], manifest.package_hash);
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert!(!report["receipt_id"].as_str().unwrap().is_empty());
    let checkpoint_id = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta
        .checkpoint_id;
    assert_eq!(report["checkpoint_id"], checkpoint_id.as_str());
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["checkpoint"]["committed"], true);
    assert_eq!(report["checkpoint"]["is_head"], true);
    assert_eq!(report["receipt_source"]["kind"], "duck_db_commit");
    assert_eq!(report["receipt_source"]["duplicate"], false);
    assert_eq!(report["receipt_source"]["no_op"], false);
    assert_eq!(report["package_status"], "checkpointed");
    assert_eq!(
        report["phases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|metric| metric["phase"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![
            "destination_write_receipt",
            "checkpoint_gate",
            "package_execution",
        ]
    );
    assert!(
        report["phases"]
            .as_array()
            .unwrap()
            .iter()
            .all(|metric| metric["status"] == "completed")
    );
    assert_eq!(report["ledger_events"]["event_count"], 8);
    assert_eq!(report["ledger_events"]["terminal_kind"], "replay_recorded");
    assert_eq!(report["ledger_events"]["kinds"]["package_finalized"], 1);
    assert_eq!(
        report["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
    assert_eq!(report["ledger_events"]["kinds"]["replay_recorded"], 1);
    assert_eq!(report["writes"]["package"], true);
    assert_eq!(report["writes"]["destination"], true);
    assert_eq!(report["writes"]["checkpoint"], true);

    let conn = DuckConnection::open(project.root.join(".cdf/replay-success.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("replay checkpoint head");
    assert_eq!(head.delta.checkpoint_id, checkpoint_id);
    assert_eq!(head.delta.package_hash.as_str(), manifest.package_hash);
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
    );
}

#[test]
fn replay_package_duckdb_same_state_duplicate_reports_no_op() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let first = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/replay-duplicate.duckdb",
    );
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);

    let second = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/replay-duplicate.duckdb",
    );

    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    let json = stderr_or_stdout_json(&second.stdout);
    let report = &json["result"];
    assert_eq!(report["receipt_source"]["kind"], "duck_db_commit");
    assert_eq!(report["receipt_source"]["duplicate"], true);
    assert_eq!(report["receipt_source"]["no_op"], true);
    assert_eq!(report["package_status"], "checkpointed");

    let conn = DuckConnection::open(project.root.join(".cdf/replay-duplicate.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);
    let destination =
        DuckDbDestination::new(project.root.join(".cdf/replay-duplicate.duckdb")).unwrap();
    let mirrors = destination.read_mirror_snapshot_read_only().unwrap();
    assert_eq!(mirrors.loads.len(), 1);
    assert_eq!(mirrors.state.len(), 1);
}

#[test]
fn replay_package_failure_records_progress_events_without_json_progress_output() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let first = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/replay-progress-failure.duckdb",
    );
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);

    let second = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/replay-progress-failure-again.duckdb",
    );

    assert_ne!(second.exit_code, 0);
    assert!(second.stdout.is_empty());
    assert!(!second.stderr.contains("Run progress"));
    assert!(!second.stderr.contains("package finalized"));

    let conn = Connection::open(project.root.join(".cdf/state.db")).unwrap();
    let latest_run_id: String = conn
        .query_row(
            "SELECT run_id FROM cdf_runs ORDER BY sequence DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let ledger = SqliteRunLedger::open(project.root.join(".cdf/state.db")).unwrap();
    let events = ledger
        .events(&RunId::new(latest_run_id).unwrap())
        .unwrap()
        .into_iter()
        .map(|event| event.kind)
        .collect::<Vec<_>>();
    assert_eq!(
        events,
        vec![
            RunEventKind::PackageFinalized,
            RunEventKind::DestinationCommitStarted,
            RunEventKind::DestinationSegmentAcknowledged,
            RunEventKind::DestinationReceiptRecorded,
            RunEventKind::RunFailed,
        ]
    );
}

#[test]
fn replay_package_failure_human_stderr_includes_progress_context() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let checkpoint_id = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta
        .checkpoint_id;
    let first = replay_package_command(
        &project,
        &package_dir,
        "duckdb://.cdf/replay-progress-human-failure.duckdb",
    );
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);

    let second = run_dynamic(vec![
        "cdf".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "run".to_owned(),
        "--package".to_owned(),
        package_dir.to_str().unwrap().to_owned(),
        "--to".to_owned(),
        "duckdb://.cdf/replay-progress-human-failure-again.duckdb".to_owned(),
    ]);

    assert_ne!(second.exit_code, 0);
    assert!(second.stdout.is_empty());
    assert_no_headless_progress_controls(&second.stderr);
    for expected in [
        "[verify] failed run failed",
        "error[CDF-PROJECT-CONTRACT]:",
        checkpoint_id.as_str(),
    ] {
        assert!(
            second.stderr.contains(expected),
            "missing {expected:?} in:\n{}",
            second.stderr
        );
    }
}

#[test]
fn replay_package_human_headless_render_reports_receipt_checkpoint_and_duplicate_facts() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let authority_state = fs::read(project.root.join(".cdf/state.db")).unwrap();
    let reader = PackageReader::open(&package_dir).unwrap();
    let package_id = reader.manifest().identity.package_id.clone();
    let checkpoint_id = reader.replay_inputs().unwrap().state_delta.checkpoint_id;
    let first = run_dynamic(vec![
        "cdf".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "run".to_owned(),
        "--package".to_owned(),
        package_dir.to_str().unwrap().to_owned(),
        "--to".to_owned(),
        "duckdb://.cdf/replay-human.duckdb".to_owned(),
    ]);
    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);

    remove_state_store(&project);
    fs::write(project.root.join(".cdf/state.db"), authority_state).unwrap();
    let second = run_dynamic(vec![
        "cdf".to_owned(),
        "--project".to_owned(),
        project.root_str().to_owned(),
        "run".to_owned(),
        "--package".to_owned(),
        package_dir.to_str().unwrap().to_owned(),
        "--to".to_owned(),
        "duckdb://.cdf/replay-human.duckdb".to_owned(),
    ]);

    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    assert_no_headless_progress_controls(&second.stdout);
    for expected in [
        "[commit] succeeded replay recorded",
        "duplicate=true",
        "no_op=true",
    ] {
        assert!(
            second.stderr.contains(expected),
            "missing {expected:?} in:\n{}",
            second.stderr
        );
    }
    for expected in [
        &format!("OK Package {package_id} was already loaded"),
        "Summary",
        "no-op (package already loaded)",
        "Proof",
        "receipt",
        "checkpoint",
        checkpoint_id.as_str(),
        "Next: cdf inspect run ",
    ] {
        assert!(
            second.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            second.stdout
        );
    }
}

#[test]
fn replay_package_human_rich_render_uses_duplicate_receipt_checkpoint_panels() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let reader = PackageReader::open(&package_dir).unwrap();
    let package_id = reader.manifest().identity.package_id.clone();
    let checkpoint_id = reader.replay_inputs().unwrap().state_delta.checkpoint_id;
    let cli = test_cli(&project);
    let output = crate::package_run::run_package(
        &cli,
        package_dir,
        "duckdb://.cdf/replay-rich.duckdb".to_owned(),
        None,
        &test_execution_services(),
        &test_destination_registry(),
        cdf_cli_core::progress::ProgressDelivery::Buffered,
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    for expected in [
        &format!("Loaded 2 rows from {package_id}"),
        "Summary",
        "destination",
        "Proof",
        "receipt",
        "checkpoint",
        "destination",
        "receipt",
        checkpoint_id.as_str(),
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
fn replay_package_postgres_destination_fails_closed_before_mutation() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        "postgres://user:destination-secret@localhost/db\n",
    )
    .unwrap();
    let package_dir = create_replay_package_fixture(&project);
    let receipts = package_receipt_count(&package_dir);
    let status = package_status(&package_dir);

    let result = replay_package_command(
        &project,
        &package_dir,
        "postgres://secret://file/destination-dsn",
    );

    assert_eq!(result.exit_code, 2);
    assert_secret_absent(&result, "destination-secret");
    assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("requires --target schema.table")
    );
}

#[test]
fn replay_package_postgres_target_mismatch_fails_closed_before_state_creation() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let receipts = package_receipt_count(&package_dir);
    let status = package_status(&package_dir);

    let result = replay_package_command_with_target(
        &project,
        &package_dir,
        "postgres://localhost/cdf",
        Some("public.events"),
    );

    assert_eq!(result.exit_code, 3, "stderr: {}", result.stderr);
    assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("does not match package destination commit target")
    );
}

#[test]
fn replay_package_postgres_secret_backed_uri_redacts_resolved_dsn_on_target_mismatch() {
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        "postgres://user:destination-secret@localhost/db\n",
    )
    .unwrap();
    let package_dir = create_replay_package_fixture(&project);
    let receipts = package_receipt_count(&package_dir);
    let status = package_status(&package_dir);

    let result = replay_package_command_with_target(
        &project,
        &package_dir,
        "postgres://secret://file/destination-dsn",
        Some("public.events"),
    );

    assert_eq!(result.exit_code, 3, "stderr: {}", result.stderr);
    assert_secret_absent(&result, "destination-secret");
    assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["kind"], "contract");
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("does not match package destination commit target")
    );
}

#[test]
fn replay_package_postgres_replays_from_artifacts_without_source_contact() {
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
    let manifest = PackageReader::open(&package_dir)
        .unwrap()
        .manifest()
        .clone();
    let receipts_before = package_receipt_count(&package_dir);

    let result = replay_package_command_with_target(
        &project,
        &package_dir,
        "postgres://secret://file/destination-dsn",
        Some(target),
    );

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &postgres.url);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "run");
    assert_eq!(report["command"], "run");
    assert_eq!(report["input_authority"], "package");
    assert_eq!(report["package_id"], manifest.identity.package_id.as_str());
    assert_eq!(report["package_hash"], manifest.package_hash);
    assert_eq!(report["destination"]["kind"], "postgres");
    assert_eq!(report["destination"]["destination_id"], "postgres");
    assert_eq!(report["destination"]["target"], target);
    assert_eq!(report["target"], target);
    assert_eq!(report["receipt"]["destination_id"], "postgres");
    assert_eq!(report["receipt"]["target"], target);
    assert_eq!(report["receipt"]["package_hash"], manifest.package_hash);
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(
        report["receipt_source"]["kind"],
        "destination_commit_receipt_only"
    );
    assert_eq!(report["receipt_source"]["package_receipt_recorded"], true);
    let checkpoint_id = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta
        .checkpoint_id;
    assert_eq!(report["checkpoint_id"], checkpoint_id.as_str());
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["package_status"], "checkpointed");
    assert_eq!(report["ledger_events"]["event_count"], 8);
    assert_eq!(report["ledger_events"]["terminal_kind"], "replay_recorded");
    assert_eq!(report["ledger_events"]["kinds"]["package_finalized"], 1);
    assert_eq!(
        report["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
    assert_eq!(report["ledger_events"]["kinds"]["replay_recorded"], 1);
    assert_eq!(package_receipt_count(&package_dir), receipts_before + 1);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("replay checkpoint head");
    assert_eq!(head.delta.checkpoint_id, checkpoint_id);
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
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
fn replay_package_parquet_replays_from_artifacts_without_source_contact() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let manifest = PackageReader::open(&package_dir)
        .unwrap()
        .manifest()
        .clone();
    let receipts_before = package_receipt_count(&package_dir);
    let parquet_root = project.root.join(".cdf/replay-parquet");

    let result = replay_package_command(&project, &package_dir, "parquet://.cdf/replay-parquet");

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = &json["result"];
    assert_eq!(json["command"], "run");
    assert_eq!(report["command"], "run");
    assert_eq!(report["input_authority"], "package");
    assert_eq!(report["package_id"], manifest.identity.package_id.as_str());
    assert_eq!(report["package_hash"], manifest.package_hash);
    assert_eq!(report["destination"]["kind"], "parquet");
    assert_eq!(
        report["destination"]["destination_id"],
        "parquet_object_store"
    );
    assert!(
        report["destination"]["root"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/replay-parquet")
    );
    assert_eq!(report["target"], "events");
    assert_eq!(report["receipt"]["destination_id"], "parquet_object_store");
    assert_eq!(report["receipt"]["target"], "events");
    assert_eq!(report["receipt"]["package_hash"], manifest.package_hash);
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(report["receipt_source"]["kind"], "destination_commit");
    assert_eq!(report["receipt_source"]["duplicate"], false);
    assert_eq!(report["receipt_source"]["no_op"], false);
    let checkpoint_id = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap()
        .state_delta
        .checkpoint_id;
    assert_eq!(report["checkpoint_id"], checkpoint_id.as_str());
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["package_status"], "checkpointed");
    assert_eq!(report["ledger_events"]["event_count"], 8);
    assert_eq!(report["ledger_events"]["terminal_kind"], "replay_recorded");
    assert_eq!(report["ledger_events"]["kinds"]["package_finalized"], 1);
    assert_eq!(
        report["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
    assert_eq!(report["ledger_events"]["kinds"]["replay_recorded"], 1);
    assert!(parquet_root.exists());
    assert_eq!(package_receipt_count(&package_dir), receipts_before + 1);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("replay checkpoint head");
    assert_eq!(head.delta.checkpoint_id, checkpoint_id);
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
    );
}

#[test]
fn replay_package_parquet_malformed_uri_fails_before_mutation() {
    for uri in ["parquet://", "parquet://s3://bucket"] {
        let project = TestProject::new();
        let package_dir = create_replay_package_fixture(&project);
        let receipts = package_receipt_count(&package_dir);
        let status = package_status(&package_dir);

        let result = replay_package_command(&project, &package_dir, uri);

        assert_eq!(result.exit_code, 78, "uri {uri}: {}", result.stderr);
        assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
        let json = stderr_or_stdout_json(&result.stderr);
        assert_eq!(json["error"]["not_supported"], true);
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("malformed or non-local")
        );
    }
}

#[test]
fn replay_package_unknown_destination_scheme_fails_closed_before_mutation() {
    let project = TestProject::new();
    let package_dir = create_replay_package_fixture(&project);
    let receipts = package_receipt_count(&package_dir);
    let status = package_status(&package_dir);

    let result = replay_package_command(&project, &package_dir, "s3://bucket/replay");

    assert_eq!(result.exit_code, 78);
    assert_no_replay_mutation(&project, &package_dir, receipts, status, None);
    let json = stderr_or_stdout_json(&result.stderr);
    assert_eq!(json["error"]["not_supported"], true);
    assert!(
        json["error"]["message"]
            .as_str()
            .unwrap()
            .contains("supported destinations are duckdb://path, parquet://root, and postgres://")
    );
}
