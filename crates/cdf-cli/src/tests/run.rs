use super::*;

#[test]
fn run_command_commits_package_rows_mirrors_and_checkpoint() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = single_resource_run_report(&json);
    assert_eq!(json["command"], "run");
    assert_eq!(report["command"], "run");
    assert!(!report["run_id"].as_str().unwrap().is_empty());
    assert_eq!(report["resource_id"], "local.events");
    assert_eq!(report["pipeline_id"], "cdf-run");
    assert_eq!(report["target"], "events");
    assert_eq!(report["destination"]["kind"], "duckdb");
    assert_eq!(report["destination"]["destination_id"], "duckdb");
    assert!(
        report["destination"]["database_path"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/dev.duckdb")
    );
    assert!(
        report["package_id"]
            .as_str()
            .unwrap()
            .starts_with("pkg-local-events-")
    );
    assert_eq!(report["package_status"], "checkpointed");
    assert!(
        report["checkpoint_id"]
            .as_str()
            .unwrap()
            .starts_with("checkpoint-local-events-")
    );
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["checkpoint"]["committed"], true);
    assert_eq!(report["checkpoint"]["is_head"], true);
    assert_eq!(report["receipt"]["destination_id"], "duckdb");
    assert_eq!(report["receipt"]["target"], "events");
    assert_eq!(report["receipt"]["counts"]["rows_written"], 2);
    assert_eq!(report["receipt_source"]["kind"], "duck_db_commit");
    assert_eq!(report["receipt_source"]["duplicate"], false);
    assert_eq!(report["receipt_source"]["no_op"], false);
    assert_eq!(report["row_count"], 2);
    assert_eq!(report["segment_count"], 1);
    assert_eq!(
        report["memory"]["budget"]["resolution"]["process_budget_bytes"],
        4 * 1024 * 1024 * 1024_u64
    );
    assert_eq!(
        report["memory"]["managed"]["budget_bytes"],
        report["memory"]["budget"]["resolution"]["managed_pool_bytes"]
    );
    assert!(report["memory"]["managed"]["peak_bytes"].as_u64().is_some());
    assert!(
        report["memory"]["budget"]["memory_authority"]["enforcement"]
            .as_str()
            .is_some()
    );
    assert_eq!(
        report["ledger_events"]["event_count"],
        report["ledger_events"]["events"].as_array().unwrap().len()
    );
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");
    assert_eq!(
        report["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
    let events = report["ledger_events"]["events"].as_array().unwrap();
    assert_eq!(events.first().unwrap()["kind"], "run_started");
    assert_eq!(events.last().unwrap()["kind"], "run_succeeded");
    assert_eq!(report["writes"]["package"], true);
    assert_eq!(report["writes"]["destination"], true);
    assert_eq!(report["writes"]["checkpoint"], true);

    let package_dir = run_package_dir(&project, &result);
    let manifest = PackageReader::open(&package_dir)
        .unwrap()
        .manifest()
        .clone();
    assert_eq!(manifest.lifecycle.status, PackageStatus::Checkpointed);
    assert_eq!(report["package_hash"], manifest.package_hash);

    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(rows, 2);

    let destination = DuckDbDestination::new(project.root.join(".cdf/dev.duckdb")).unwrap();
    let mirrors = destination.read_mirror_snapshot_read_only().unwrap();
    assert!(mirrors.loads_table_present);
    assert!(mirrors.state_table_present);
    assert_eq!(mirrors.loads.len(), 1);
    assert_eq!(mirrors.state.len(), 1);
    assert_eq!(mirrors.loads[0].package_hash, manifest.package_hash);

    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new("local.events").unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .expect("committed run head");
    assert_eq!(
        head.delta.checkpoint_id.as_str(),
        report["checkpoint_id"].as_str().unwrap()
    );
    assert_eq!(head.delta.package_hash.as_str(), manifest.package_hash);
    assert!(head.delta.schema_hash.as_str().starts_with("sha256:"));
    assert_eq!(
        head.receipt.as_ref().unwrap().receipt_id.as_str(),
        report["receipt_id"].as_str().unwrap()
    );
    assert_eq!(head.delta.segments.len(), 1);
    assert!(matches!(
        head.delta.output_position,
        SourcePosition::FileManifest(_)
    ));
}

#[test]
fn run_short_form_uses_product_defaults_and_destination_alias() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "local.events",
        "--to",
        "duckdb://.cdf/short-form.duckdb",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    let json = stderr_or_stdout_json(&result.stdout);
    let report = single_resource_run_report(&json);
    assert_eq!(report["resource_id"], "local.events");
    assert_eq!(report["pipeline_id"], "cdf-run");
    assert_eq!(report["target"], "events");
    assert!(
        report["package_id"]
            .as_str()
            .unwrap()
            .starts_with("pkg-local-events-")
    );
    assert!(
        report["checkpoint_id"]
            .as_str()
            .unwrap()
            .starts_with("checkpoint-local-events-")
    );
    assert!(
        report["destination"]["database_path"]
            .as_str()
            .unwrap()
            .ends_with(".cdf/short-form.duckdb")
    );
    assert!(project.root.join(".cdf/short-form.duckdb").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());

    let package_dir = project
        .root
        .join(".cdf/packages")
        .join(report["package_id"].as_str().unwrap());
    assert!(package_dir.exists());
}

#[test]
fn run_adhoc_local_parquet_reuses_identity_and_ordinary_evidence_spine() {
    const PATH_SECRET: &str = "local-path-secret-sentinel";
    let project = TestProject::new();
    let source_dir = project.root.join("data").join(PATH_SECRET);
    fs::create_dir_all(&source_dir).unwrap();
    let source = source_dir.join("yellow.parquet");
    write_vendor_parquet(&source);
    let source = source.to_str().unwrap();

    let first = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        source,
        "--to",
        "duckdb://.cdf/adhoc-local.duckdb",
    ]);

    assert_eq!(first.exit_code, 0, "stderr: {}", first.stderr);
    assert_secret_absent(&first, PATH_SECRET);
    let first_json = stderr_or_stdout_json(&first.stdout);
    let report = single_resource_run_report(&first_json);
    let resource_id = report["resource_id"].as_str().unwrap();
    assert!(resource_id.starts_with("adhoc.parquet_"));
    assert_eq!(report["adhoc"]["resource_id"], resource_id);
    assert_eq!(report["adhoc"]["reused"], false);
    let definition_path = report["adhoc"]["definition_path"].as_str().unwrap();
    let staged_path = report["adhoc"]["source_artifact_path"].as_str().unwrap();
    assert!(definition_path.starts_with(".cdf/adhoc/parquet_"));
    assert!(definition_path.ends_with(".cdf.sql"));
    assert!(staged_path.starts_with(".cdf/adhoc/data/parquet_"));
    assert!(project.root.join(definition_path).is_file());
    assert!(project.root.join(staged_path).is_file());
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");
    assert_eq!(
        report["ledger_events"]["kinds"]["destination_receipt_recorded"],
        1
    );
    assert_eq!(report["writes"]["package"], true);
    assert_eq!(report["writes"]["destination"], true);
    assert_eq!(report["writes"]["checkpoint"], true);
    assert!(
        report["adhoc"]["make_permanent_command"]
            .as_str()
            .unwrap()
            .starts_with(&format!("cdf add {resource_id} .cdf/adhoc/data/"))
    );

    let resource_sql = fs::read_to_string(project.root.join(definition_path)).unwrap();
    assert!(resource_sql.contains("FROM upstream("));
    assert!(resource_sql.contains("source => 'adhoc'"));
    assert!(!resource_sql.contains(PATH_SECRET));
    assert_eq!(
        active_schema_hash(&project, resource_id),
        report["schema_hash"].as_str().unwrap()
    );
    let package = PackageReader::open(run_package_dir(&project, &first)).unwrap();
    package.verify().unwrap();
    let receipt = collect_package_receipts(&package).remove(0);
    assert_eq!(receipt.schema_hash.as_str(), report["schema_hash"]);
    let destination = DuckDbDestination::new(project.root.join(".cdf/adhoc-local.duckdb")).unwrap();
    assert!(destination.verify_receipt(&receipt).unwrap().verified);
    let store = SqliteCheckpointStore::open(project.root.join(".cdf/state.db")).unwrap();
    let head = store
        .head(
            &PipelineId::new("cdf-run").unwrap(),
            &ResourceId::new(resource_id).unwrap(),
            &ScopeKey::Resource,
        )
        .unwrap()
        .unwrap();
    assert_eq!(head.delta.schema_hash.as_str(), report["schema_hash"]);
    assert!(receipt.covers_state_delta(&head.delta));
    drop(store);

    let second = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        source,
        "--to",
        "duckdb://.cdf/adhoc-local.duckdb",
    ]);
    assert_eq!(second.exit_code, 0, "stderr: {}", second.stderr);
    assert_secret_absent(&second, PATH_SECRET);
    let second_json = stderr_or_stdout_json(&second.stdout);
    let second_report = single_resource_run_report(&second_json);
    assert_eq!(second_report["resource_id"], resource_id);
    assert_eq!(second_report["adhoc"]["reused"], true);
    assert_eq!(second_report["schema_hash"], report["schema_hash"]);
    assert_eq!(
        fs::read_dir(project.root.join(".cdf/adhoc"))
            .unwrap()
            .filter_map(Result::ok)
            .filter(
                |entry| entry.path().extension().and_then(|value| value.to_str()) == Some("sql")
            )
            .count(),
        1
    );
    assert_generated_artifacts_exclude(&project.root, PATH_SECRET);

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "run",
        source,
        "--to",
        "duckdb://.cdf/adhoc-local.duckdb",
    ]);
    assert_eq!(human.exit_code, 0, "stderr: {}", human.stderr);
    assert_secret_absent(&human, PATH_SECRET);
    assert!(human.stdout.contains("Ad-hoc Resource"));
    assert!(human.stdout.contains(definition_path));
    assert!(human.stdout.contains(&format!("cdf add {resource_id}")));
}

#[test]
fn run_adhoc_destination_failure_preserves_recoverable_evidence_and_retry() {
    let project = TestProject::new();
    let source = project.root.join("data/yellow.parquet");
    write_vendor_parquet(&source);
    let canonical = fs::canonicalize(&source)
        .unwrap()
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/");
    let digest = Sha256::digest(canonical.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let target = format!("parquet_{}", &digest[..24]);
    let destination_path = project.root.join(".cdf/adhoc-retry.duckdb");
    let connection = DuckConnection::open(&destination_path).unwrap();
    connection
        .execute_batch(&format!(
            "CREATE TABLE {target} (vendor_id INTEGER NOT NULL UNIQUE); INSERT INTO {target} VALUES (1), (2)"
        ))
        .unwrap();
    drop(connection);

    let failed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        source.to_str().unwrap(),
        "--to",
        "duckdb://.cdf/adhoc-retry.duckdb",
    ]);
    assert_ne!(failed.exit_code, 0);

    let package_dir = single_package_dir(&project);
    let package = PackageReader::open(&package_dir).unwrap();
    let resource_id = package
        .replay_inputs()
        .unwrap()
        .state_delta
        .resource_id
        .to_string();
    assert!(resource_id.starts_with("adhoc.parquet_"));
    package.verify().unwrap();
    assert!(collect_package_receipts(&package).is_empty());
    assert_eq!(
        package.manifest().lifecycle.status,
        PackageStatus::Loading,
        "failed run stderr: {}",
        failed.stderr
    );

    let state_path = project.root.join(".cdf/state.db");
    let state = Connection::open(&state_path).unwrap();
    let run_id: String = state
        .query_row(
            "SELECT run_id FROM cdf_runs ORDER BY created_at_ms DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    drop(state);
    let ledger = SqliteRunLedger::open(&state_path).unwrap();
    let events = ledger.events(&RunId::new(run_id).unwrap()).unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.kind == RunEventKind::PackageFinalized)
    );
    assert_eq!(events.last().unwrap().kind, RunEventKind::RunFailed);
    assert!(
        events
            .iter()
            .all(|event| event.kind != RunEventKind::DestinationReceiptRecorded)
    );
    assert!(
        events
            .iter()
            .all(|event| event.kind != RunEventKind::CheckpointCommitted)
    );
    drop(ledger);
    let store = SqliteCheckpointStore::open(&state_path).unwrap();
    assert!(
        store
            .head(
                &PipelineId::new("cdf-run").unwrap(),
                &ResourceId::new(&resource_id).unwrap(),
                &ScopeKey::Resource,
            )
            .unwrap()
            .is_none()
    );
    drop(store);

    let connection = DuckConnection::open(&destination_path).unwrap();
    connection
        .execute_batch(&format!("DROP TABLE {target}"))
        .unwrap();
    drop(connection);
    let retry = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        source.to_str().unwrap(),
        "--to",
        "duckdb://.cdf/adhoc-retry.duckdb",
    ]);
    assert_eq!(retry.exit_code, 0, "stderr: {}", retry.stderr);
    let retry = stderr_or_stdout_json(&retry.stdout);
    let retry = single_resource_run_report(&retry);
    assert_eq!(retry["resource_id"], resource_id);
    assert_eq!(retry["adhoc"]["reused"], true);
    assert_eq!(retry["checkpoint"]["status"], "committed");
    assert_eq!(retry["ledger_events"]["terminal_kind"], "run_succeeded");
}

#[test]
fn run_adhoc_http_parquet_uses_bounded_discovery_and_ordinary_run() {
    let project = TestProject::new();
    write_vendor_parquet(&project.root.join("data/yellow.parquet"));
    let parquet = fs::read(project.root.join("data/yellow.parquet")).unwrap();
    let max_requests = parquet.len() + 64;
    let (base_url, requests) = serve_parquet_file(parquet, max_requests);
    let url = format!("{base_url}/yellow.parquet");
    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        &url,
        "--to",
        "duckdb://.cdf/adhoc-http.duckdb",
    ]);

    let observed_requests = requests.lock().unwrap().clone();
    assert_eq!(
        result.exit_code, 0,
        "stderr: {}\nrequests: {observed_requests:#?}",
        result.stderr
    );
    let json = stderr_or_stdout_json(&result.stdout);
    let report = single_resource_run_report(&json);
    assert!(
        report["resource_id"]
            .as_str()
            .unwrap()
            .starts_with("adhoc.parquet_")
    );
    assert!(report["adhoc"]["source_artifact_path"].is_null());
    assert_eq!(
        report["schema_hash"],
        report["schema_authority"]["schema_hash"]
    );
    assert_eq!(
        report["schema_snapshot"]["discovery"]["file_coverage"],
        "all_files"
    );
    assert_eq!(report["checkpoint"]["status"], "committed");
    assert_eq!(report["ledger_events"]["terminal_kind"], "run_succeeded");
    let definition = fs::read_to_string(
        project
            .root
            .join(report["adhoc"]["definition_path"].as_str().unwrap()),
    )
    .unwrap();
    assert!(definition.contains("source => 'adhoc'"));
    assert!(definition.contains("glob => 'yellow.parquet'"));
    assert!(
        observed_requests
            .iter()
            .any(|request| request.starts_with("HEAD /yellow.parquet"))
    );
    assert!(observed_requests.iter().any(|request| {
        request.starts_with("GET /yellow.parquet")
            && request.to_ascii_lowercase().contains("range: bytes=")
    }));
}

#[test]
fn run_adhoc_rejects_missing_destination_and_sensitive_or_unsupported_urls_without_writes() {
    let project = TestProject::new();
    let source = project.root.join("data/yellow.parquet");
    write_vendor_parquet(&source);
    let before = project_tree_snapshot(&project.root);
    let missing_destination = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        source.to_str().unwrap(),
    ]);
    assert_ne!(missing_destination.exit_code, 0);
    assert!(
        missing_destination
            .stderr
            .contains("explicit `--to <destination>`")
    );
    assert_eq!(project_tree_snapshot(&project.root), before);

    const URL_SECRET: &str = "signed-url-secret-sentinel";
    let signed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "https://data.example.test/yellow.parquet?sig=signed-url-secret-sentinel",
        "--to",
        "duckdb://.cdf/adhoc-rejected.duckdb",
    ]);
    assert_ne!(signed.exit_code, 0);
    assert_secret_absent(&signed, URL_SECRET);
    assert_eq!(project_tree_snapshot(&project.root), before);

    const USERINFO_SECRET: &str = "userinfo-secret-sentinel";
    let userinfo = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "https://public-user:userinfo-secret-sentinel@data.example.test/yellow.parquet",
        "--to",
        "duckdb://.cdf/adhoc-rejected.duckdb",
    ]);
    assert_ne!(userinfo.exit_code, 0);
    assert!(
        userinfo
            .stderr
            .contains("does not accept URL userinfo credentials")
    );
    assert_secret_absent(&userinfo, USERINFO_SECRET);
    assert_eq!(project_tree_snapshot(&project.root), before);

    const MALFORMED_URL_SECRET: &str = "malformed-url-secret-sentinel";
    let malformed_url = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "https://public-user:malformed-url-secret-sentinel@[bad/yellow.parquet",
        "--to",
        "duckdb://.cdf/adhoc-rejected.duckdb",
    ]);
    assert_ne!(malformed_url.exit_code, 0);
    assert!(malformed_url.stderr.contains("[redacted-url]"));
    assert_secret_absent(&malformed_url, MALFORMED_URL_SECRET);
    assert_eq!(project_tree_snapshot(&project.root), before);

    const UNSUPPORTED_SECRET: &str = "unsupported-location-secret";
    let unsupported = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        "s3://unsupported-location-secret@bucket/yellow.parquet",
        "--to",
        "duckdb://.cdf/adhoc-rejected.duckdb",
    ]);
    assert_ne!(unsupported.exit_code, 0);
    assert_secret_absent(&unsupported, UNSUPPORTED_SECRET);
    assert_eq!(project_tree_snapshot(&project.root), before);
    assert!(!project.root.join(".cdf/adhoc").exists());
}

#[test]
fn run_adhoc_rejected_local_paths_redact_details_without_writes() {
    let project = TestProject::new();
    const MISSING_SECRET: &str = "missing-local-secret-sentinel";
    const EXTENSION_SECRET: &str = "wrong-extension-secret-sentinel";
    const DIRECTORY_SECRET: &str = "directory-local-secret-sentinel";
    let missing = project
        .root
        .join("data")
        .join(MISSING_SECRET)
        .join("yellow.parquet");
    let wrong_extension = project
        .root
        .join("data")
        .join(format!("{EXTENSION_SECRET}.unknown"));
    fs::write(&wrong_extension, "not parquet").unwrap();
    let directory = project
        .root
        .join("data")
        .join(format!("{DIRECTORY_SECRET}.parquet"));
    fs::create_dir_all(&directory).unwrap();
    let before = project_tree_snapshot(&project.root);

    for (path, secret) in [
        (missing, MISSING_SECRET),
        (wrong_extension, EXTENSION_SECRET),
        (directory, DIRECTORY_SECRET),
    ] {
        let result = run_dynamic(vec![
            "cdf".to_owned(),
            "--json".to_owned(),
            "--project".to_owned(),
            project.root_str().to_owned(),
            "run".to_owned(),
            path.to_string_lossy().into_owned(),
            "--to".to_owned(),
            "duckdb://.cdf/adhoc-rejected-local.duckdb".to_owned(),
        ]);
        assert_ne!(result.exit_code, 0);
        assert!(result.stderr.contains("[redacted-local-source-path]"));
        assert_secret_absent(&result, secret);
        assert_eq!(project_tree_snapshot(&project.root), before);
    }
}

#[test]
fn run_adhoc_synthetic_resource_id_collision_fails_before_mutation() {
    let project = TestProject::new();
    let source = project.root.join("data/yellow.parquet");
    write_vendor_parquet(&source);
    let canonical = fs::canonicalize(&source)
        .unwrap()
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/");
    let digest = Sha256::digest(canonical.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let resource_name = format!("parquet_{}", &digest[..24]);
    let resource_id = format!("adhoc.{resource_name}");
    fs::create_dir_all(project.root.join("cdf/adhoc")).unwrap();
    fs::write(
        project
            .root
            .join("cdf/adhoc")
            .join(format!("{resource_name}.cdf.sql")),
        RESOURCE.replace("source => 'local'", "source => 'adhoc'"),
    )
    .unwrap();
    let mut project_toml = fs::read_to_string(project.root.join("cdf.toml")).unwrap();
    project_toml.push_str("\n[sources.adhoc]\ntype = \"files\"\nroot = \"data\"\n");
    fs::write(project.root.join("cdf.toml"), project_toml).unwrap();
    let before = project_tree_snapshot(&project.root);

    let result = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "run",
        source.to_str().unwrap(),
        "--to",
        "duckdb://.cdf/adhoc-collision.duckdb",
    ]);

    assert_ne!(result.exit_code, 0);
    assert!(result.stderr.contains(&resource_id));
    assert!(
        result
            .stderr
            .contains("conflicts with an already compiled project resource")
    );
    assert_eq!(project_tree_snapshot(&project.root), before);
    assert!(!project.root.join(".cdf/adhoc").exists());
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/adhoc-collision.duckdb").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
}

#[test]
fn run_human_output_mentions_receipt_verified_commit_gate() {
    let project = TestProject::new();
    let result = run([
        "cdf",
        "--project",
        project.root_str(),
        "--progress",
        "always",
        "run",
        "local.events",
    ]);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_no_headless_progress_controls(&result.stdout);
    assert_no_headless_progress_controls(&result.stderr);
    for expected in [
        "[plan] running plan recorded",
        "[gate] succeeded run succeeded",
    ] {
        assert!(
            result.stderr.contains(expected),
            "missing {expected:?} in stderr:\n{}",
            result.stderr
        );
    }
    for expected in [
        "OK Loaded 2 rows from local.events",
        "Summary",
        "Proof",
        "local.events",
        "events",
        "checkpoint",
        "committed",
        "Next: cdf inspect run ",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}

#[test]
fn run_human_rich_render_uses_checkpoint_gate_panel() {
    let project = TestProject::new();
    let mut cli = test_cli(&project);
    cli.terminal.progress = cdf_cli_core::terminal::PolicyMode::Always;
    let (host, services) =
        cdf_engine::StandaloneExecutionHost::default_services(512 * 1024 * 1024).unwrap();
    let output = crate::run_command::run(
        &cli,
        cdf_cli_core::args::RunArgs {
            plan: None,
            package: None,
            resume: false,
            resume_run_id: None,
            selectors: vec!["local.events".to_owned()],
            exclude: Vec::new(),
            locked: false,
            destination_uri: None,
            target: None,
            jobs: None,
            stats_profile: false,
            explain_memory: true,
            loop_mode: false,
            segmentation: cdf_cli_core::args::SegmentationArgs::default(),
        },
        host.as_ref(),
        &services,
        &test_destination_registry(),
        cdf_cli_core::progress::ProgressDelivery::Buffered,
    )
    .unwrap();
    let result = render_rich(output);

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert!(result.stderr.contains("Planned"), "{}", result.stderr);
    for expected in [
        "Loaded 2 rows from local.events",
        "Summary",
        "Proof",
        "rows",
        "Memory",
        "receipt",
        "gate",
        "checkpoint",
        "cdf inspect run ",
    ] {
        assert!(
            result.stdout.contains(expected),
            "missing {expected:?} in:\n{}",
            result.stdout
        );
    }
}
