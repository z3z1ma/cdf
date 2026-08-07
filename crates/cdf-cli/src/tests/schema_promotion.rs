use super::*;

fn promotion_planning_authority(
    context: &crate::context::ProjectContext,
    resource: &cdf_declarative::CompiledResource,
    destinations: &[ResolvedProjectDestination],
) -> SchemaPromotionPlanningAuthority {
    let active = crate::schema_authority::load_active(context, &resource.descriptor().resource_id)
        .unwrap()
        .unwrap();
    let mut destination_sheets = BTreeMap::new();
    for destination in destinations {
        destination_sheets.insert(
            destination.describe().destination_id.to_string(),
            destination.destination_sheet_artifact().unwrap(),
        );
    }
    SchemaPromotionPlanningAuthority {
        head: active.head,
        schema_cache: SchemaSnapshotArtifact::new(
            &resource.descriptor().resource_id,
            &active.version.canonical_schema.to_arrow().unwrap(),
            BTreeMap::new(),
        )
        .unwrap(),
        version: active.version,
        destinations: destination_sheets,
    }
}

#[test]
fn schema_promote_plans_fresh_residual_correction_without_writes() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let compile = compile_resource(&project, "local.events");
    assert_eq!(compile.exit_code, 0, "{}", compile.stderr);
    let locked_hash = active_schema_hash(&project, "local.events");

    write_vendor_score_parquet(&source_path);
    write_schema_promote_package_fixture(&project, &locked_hash);
    let before = project_tree_snapshot(&project.root);

    let planned = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(planned.exit_code, 0, "{}", planned.stderr);
    assert_project_tree_unchanged(&project.root, &before);
    let json = stderr_or_stdout_json(&planned.stdout);
    let report = &json["result"];
    assert_eq!(report["writes"]["schema_snapshot"], false);
    assert_eq!(report["writes"]["destination"], false);
    assert_eq!(report["evidence_inventory_complete"], true);
    assert_eq!(report["paths"][0]["path"], "/score", "{report}");
    assert_eq!(report["paths"][0]["source_name"], "score", "{report}");
    assert_eq!(report["paths"][0]["selected_type"], "Int64");
    assert_eq!(report["paths"][0]["observed_count"], 2);
    assert!(
        report["paths"][0]["affected_address_value_digest"]
            .as_str()
            .unwrap()
            .starts_with("sha256:")
    );
    assert_eq!(report["evidence"][0]["availability"], "retained_package");
    assert_eq!(report["targets"][0]["destination"], "duckdb");
    assert_eq!(
        report["paths"][0]["associations"][0]["target"],
        report["targets"][0]["target"]
    );
    assert_eq!(
        report["paths"][0]["associations"][0]["package_hash"],
        report["paths"][0]["affected_packages"][0]
    );
    assert_eq!(report["targets"][0]["strategy"], "in_place_update");
    assert_eq!(report["targets"][0]["migrations"][0]["path"], "/score");
    assert_eq!(report["recovery_argv"][0], "cdf");
    assert!(
        report["recovery_command"]
            .as_str()
            .unwrap()
            .contains("--type /score=Int64")
    );
    assert!(
        report["proposed_snapshot"]["path"]
            .as_str()
            .unwrap()
            .contains(report["proposed_snapshot"]["schema_hash"].as_str().unwrap())
    );
    let proposed: cdf_project::SchemaSnapshotArtifact =
        serde_json::from_value(report["proposed_snapshot"]["artifact"].clone()).unwrap();
    assert_eq!(
        cdf_kernel::canonical_arrow_schema_hash(&proposed.schema.to_arrow().unwrap())
            .unwrap()
            .as_str(),
        report["new_schema_hash"].as_str().unwrap()
    );
    assert_eq!(
        report["proposed_snapshot"]["artifact"]["version"],
        cdf_project::SCHEMA_SNAPSHOT_ARTIFACT_VERSION
    );
    let repeated = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(repeated.exit_code, 0, "{}", repeated.stderr);
    assert_eq!(project_tree_snapshot(&project.root), before);
    let repeated_json = stderr_or_stdout_json(&repeated.stdout);
    assert_eq!(
        repeated_json["result"]["promotion_id"],
        report["promotion_id"]
    );
    assert_eq!(
        repeated_json["result"]["new_schema_hash"],
        report["new_schema_hash"]
    );

    let human = run([
        "cdf",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(human.exit_code, 0, "{}", human.stderr);
    assert_eq!(project_tree_snapshot(&project.root), before);
    assert!(human.stdout.contains("retained_package"));
    assert!(human.stdout.contains("in_place_update"));
    assert!(human.stdout.contains("score"));
    assert!(human.stdout.contains("Effects"));
    assert!(human.stdout.contains("Fresh discovery identity"));
    assert!(human.stdout.contains("Target evidence"));
    assert!(human.stdout.contains("receipt verification"));
    assert!(human.stdout.contains("preserved"));
    assert!(human.stdout.contains("Execution preconditions"));

    let invalid = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--type",
        "/score=not-an-arrow-type",
    ]);
    assert_ne!(invalid.exit_code, 0);
    assert_eq!(project_tree_snapshot(&project.root), before);
    assert!(invalid.stderr.contains("invalid Arrow type declaration"));

    let unknown = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--type",
        "/missing=int64",
    ]);
    assert_eq!(unknown.exit_code, 0, "{}", unknown.stderr);
    assert_eq!(project_tree_snapshot(&project.root), before);
    let unknown_json = stderr_or_stdout_json(&unknown.stdout);
    assert!(
        unknown_json["result"]["conflicts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|conflict| conflict["code"] == "unknown_path")
    );
}

#[test]
fn schema_promote_execute_commits_correction_checkpoint_and_state_publication() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    let resource_path = project.root.join("cdf/local/events.cdf.sql");
    let resource_text = fs::read_to_string(&resource_path).unwrap();
    fs::write(
        &resource_path,
        resource_text.replace(
            "trust = \"governed\"",
            "trust = \"governed\"\ncontract = \"events-contract\"",
        ),
    )
    .unwrap();
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let compile = compile_resource(&project, "local.events");
    assert_eq!(compile.exit_code, 0, "{}", compile.stderr);
    let old_hash = active_schema_hash(&project, "local.events");
    write_vendor_score_parquet(&source_path);
    write_schema_promote_package_fixture(&project, &old_hash);

    let executed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--execute",
    ]);
    assert_eq!(executed.exit_code, 0, "{}", executed.stderr);
    let json = stderr_or_stdout_json(&executed.stdout);
    let report = &json["result"];
    assert_eq!(report["phase"], "published");
    assert_eq!(report["state_published"], true);
    assert_eq!(report["current_generation"], 1);
    assert_eq!(report["published_generation"], 2);
    assert_eq!(report["targets"][0]["committed"], true);
    assert!(
        report["recovery_command"]
            .as_str()
            .unwrap()
            .ends_with("--execute")
    );
    let new_hash = report["new_schema_hash"].as_str().unwrap();
    assert_ne!(new_hash, old_hash);

    let correction_package = fs::read_dir(project.root.join(".cdf/packages"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.file_name().unwrap() != "pkg-promote-source")
        .unwrap();
    let replay_inputs = PackageReader::open(correction_package)
        .unwrap()
        .replay_inputs()
        .unwrap();
    let published_context = crate::context::ProjectContext::load_with_destination_registry(
        Some(&project.root),
        None,
        &test_destination_registry(),
    )
    .unwrap();
    let published = crate::schema_authority::load_active(
        &published_context,
        &ResourceId::new("local.events").unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        replay_inputs.state_delta.scope,
        published.head.key.promotion_scope().unwrap()
    );
    assert_eq!(published.head.schema_hash.as_str(), new_hash);
    let conn = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
    let rows = conn
        .prepare("SELECT vendor_id, score, _cdf_variant FROM events ORDER BY _cdf_row_key")
        .unwrap()
        .query_map([], |row| {
            Ok((
                row.get::<_, i32>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, Option<String>>(2)?,
            ))
        })
        .unwrap()
        .map(|row| row.unwrap())
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![(1, 10, None), (2, 20, None)]);
    drop(conn);

    let replay = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--type",
        "/score=Int64",
        "--execute",
    ]);
    assert_eq!(replay.exit_code, 0, "{}", replay.stderr);
    let replay_json = stderr_or_stdout_json(&replay.stdout);
    assert_eq!(
        replay_json["result"]["promotion_id"],
        report["promotion_id"]
    );
    assert_eq!(replay_json["result"]["resumed"], true);
}

#[test]
fn schema_promote_multi_target_uses_canonical_checkpoint_chain_and_exact_publication() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let compile = compile_resource(&project, "local.events");
    assert_eq!(compile.exit_code, 0, "{}", compile.stderr);
    let old_hash = active_schema_hash(&project, "local.events");
    write_vendor_score_parquet(&source_path);
    write_schema_promote_package_fixture_for_target(
        &project,
        "pkg-promote-z",
        "z_events",
        &old_hash,
    );
    write_schema_promote_package_fixture_for_target(
        &project,
        "pkg-promote-a",
        "a_events",
        &old_hash,
    );

    let dry = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
    let plan: SchemaPromotionPlanReport =
        serde_json::from_value(stderr_or_stdout_json(&dry.stdout)["result"].clone()).unwrap();
    let context = crate::context::ProjectContext::load_with_destination_registry(
        Some(&project.root),
        None,
        &test_destination_registry(),
    )
    .unwrap();
    let destinations = plan
        .targets
        .iter()
        .map(|target| {
            crate::destination_uri::resolve_selected_destination(
                &test_destination_registry(),
                &context,
                &TargetName::new(target.target.clone()).unwrap(),
                None,
            )
            .unwrap()
            .destination
        })
        .collect::<Vec<_>>();
    let authority = promotion_planning_authority(
        &context,
        context.resource("local.events").unwrap(),
        &destinations,
    );
    let store = SqlitePromotionSettlementStore::open(context.state_store_path().unwrap()).unwrap();
    let failure = execute_schema_promotion(SchemaPromotionExecutionRequest {
        project_root: &context.root,
        package_root: &context.package_root(),
        resource: context.resource("local.events").unwrap(),
        authority: &authority,
        dry_plan: &plan,
        destinations,
        execution_services: test_execution_services(),
        pipeline_id: PipelineId::new("cdf-schema-promotion").unwrap(),
        lease_owner: LeaseOwnerId::new("multi-target-crash").unwrap(),
        lease_duration_ms: DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS,
        settlement_store: &store,
        failpoint: Some(SchemaPromotionExecutionFailpoint::AfterTargetSettlementIndex(1)),
    })
    .unwrap_err();
    assert!(
        failure.message.contains("schema promotion failpoint"),
        "{failure}"
    );
    drop(store);
    drop(context);
    fs::remove_dir_all(project.root.join(".cdf/packages/pkg-promote-a")).unwrap();
    fs::remove_dir_all(project.root.join(".cdf/packages/pkg-promote-z")).unwrap();

    let executed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--type",
        "/score=Int64",
        "--execute",
    ]);
    assert_eq!(executed.exit_code, 0, "{}", executed.stderr);
    let report = stderr_or_stdout_json(&executed.stdout)["result"].clone();
    assert_eq!(report["resumed"], true);
    let targets = report["targets"].as_array().unwrap();
    assert_eq!(targets.len(), 2);
    assert_eq!(targets[0]["target"], "a_events");
    assert_eq!(targets[1]["target"], "z_events");

    let store = SqlitePromotionSettlementStore::open(project.root.join(".cdf/state.db")).unwrap();
    let scope = ScopeKey::SchemaContract {
        contract: cdf_kernel::ContractRef::new("local.events").unwrap(),
    };
    let history = CheckpointStore::history(
        &store,
        &PipelineId::new("cdf-schema-promotion").unwrap(),
        &ResourceId::new("local.events").unwrap(),
        &scope,
    )
    .unwrap();
    let committed = history
        .iter()
        .filter(|checkpoint| checkpoint.status == CheckpointStatus::Committed)
        .collect::<Vec<_>>();
    assert_eq!(committed.len(), 2);
    assert_eq!(
        committed[1].delta.parent_checkpoint_id.as_ref(),
        Some(&committed[0].delta.checkpoint_id)
    );
    assert_eq!(
        committed[1].delta.input_position.as_ref(),
        Some(&committed[0].delta.output_position)
    );
    let publication = store
        .promotion_publication(
            &cdf_kernel::PromotionId::new(report["promotion_id"].as_str().unwrap()).unwrap(),
        )
        .unwrap()
        .unwrap();
    assert_eq!(publication.targets.len(), 2);
    assert_eq!(publication.targets[0].target.as_str(), "a_events");
    assert_eq!(publication.targets[1].target.as_str(), "z_events");
    assert_eq!(
        publication.targets[1].checkpoint_id,
        committed[1].delta.checkpoint_id
    );
}

#[test]
fn schema_promote_execute_recovers_every_persisted_crash_boundary() {
    for failpoint in [
        SchemaPromotionExecutionFailpoint::AfterPromotionFenced,
        SchemaPromotionExecutionFailpoint::AfterCutoffEstablished,
        SchemaPromotionExecutionFailpoint::AfterCorrectionPackages,
        SchemaPromotionExecutionFailpoint::AfterDestinationReceipt,
        SchemaPromotionExecutionFailpoint::AfterTargetSettlement,
        SchemaPromotionExecutionFailpoint::AfterHeadPublished,
    ] {
        let project = TestProject::new();
        write_parquet_discover_resource(&project, "*.parquet");
        let source_path = project.root.join("data/events.parquet");
        write_vendor_parquet(&source_path);
        let compile = compile_resource(&project, "local.events");
        assert_eq!(compile.exit_code, 0, "{failpoint:?}: {}", compile.stderr);
        let old_hash = active_schema_hash(&project, "local.events");
        write_vendor_score_parquet(&source_path);
        write_schema_promote_package_fixture(&project, &old_hash);
        let dry = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "promote",
            "local.events",
        ]);
        assert_eq!(dry.exit_code, 0, "{failpoint:?}: {}", dry.stderr);
        let dry_json = stderr_or_stdout_json(&dry.stdout);
        let plan: SchemaPromotionPlanReport =
            serde_json::from_value(dry_json["result"].clone()).unwrap();

        let context = crate::context::ProjectContext::load_with_destination_registry(
            Some(&project.root),
            None,
            &test_destination_registry(),
        )
        .unwrap();
        let resource = context.resource("local.events").unwrap();
        let target = TargetName::new(plan.targets[0].target.clone()).unwrap();
        let destination = crate::destination_uri::resolve_selected_destination(
            &test_destination_registry(),
            &context,
            &target,
            None,
        )
        .unwrap()
        .destination;
        let destinations = vec![destination];
        let authority = promotion_planning_authority(&context, resource, &destinations);
        let state_path = context.state_store_path().unwrap();
        let settlement_store = SqlitePromotionSettlementStore::open(&state_path).unwrap();
        let run_ledger = SqliteRunLedger::open(&state_path).unwrap();
        let error = execute_schema_promotion(SchemaPromotionExecutionRequest {
            project_root: &context.root,
            package_root: &context.package_root(),
            resource,
            authority: &authority,
            dry_plan: &plan,
            destinations,
            execution_services: test_execution_services(),
            pipeline_id: PipelineId::new("cdf-schema-promotion").unwrap(),
            lease_owner: LeaseOwnerId::new(format!("crash-{failpoint:?}")).unwrap(),
            lease_duration_ms: DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS,
            settlement_store: &settlement_store,
            failpoint: Some(failpoint),
        })
        .unwrap_err();
        assert!(
            error.message.contains("schema promotion failpoint"),
            "{failpoint:?}: {error}"
        );
        let expected_phase = match failpoint {
            SchemaPromotionExecutionFailpoint::AfterPromotionFenced => {
                SchemaPromotionLifecyclePhase::Fenced
            }
            SchemaPromotionExecutionFailpoint::AfterCutoffEstablished => {
                SchemaPromotionLifecyclePhase::CutoffEstablished
            }
            SchemaPromotionExecutionFailpoint::AfterCorrectionPackages => {
                SchemaPromotionLifecyclePhase::CutoffEstablished
            }
            SchemaPromotionExecutionFailpoint::AfterDestinationReceipt => {
                SchemaPromotionLifecyclePhase::CutoffEstablished
            }
            SchemaPromotionExecutionFailpoint::AfterTargetSettlement => {
                SchemaPromotionLifecyclePhase::CutoffEstablished
            }
            SchemaPromotionExecutionFailpoint::AfterHeadPublished => {
                SchemaPromotionLifecyclePhase::Published
            }
            SchemaPromotionExecutionFailpoint::AfterTargetSettlementIndex(_) => unreachable!(),
        };
        let status = settlement_store
            .promotion_state(
                &authority.head.key,
                &cdf_kernel::PromotionId::new(plan.promotion_id.clone()).unwrap(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(status.phase, expected_phase, "{failpoint:?}");
        drop(run_ledger);
        drop(settlement_store);
        drop(context);

        if failpoint != SchemaPromotionExecutionFailpoint::AfterPromotionFenced
            && failpoint != SchemaPromotionExecutionFailpoint::AfterCutoffEstablished
        {
            fs::remove_dir_all(project.root.join(".cdf/packages/pkg-promote-source")).unwrap();
            let correction_packages = fs::read_dir(project.root.join(".cdf/packages"))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.join(MANIFEST_FILE).is_file())
                .collect::<Vec<_>>();
            assert_eq!(correction_packages.len(), 1, "{failpoint:?}");
            PackageReader::open(&correction_packages[0])
                .unwrap()
                .replay_inputs()
                .unwrap();
        }

        let recovered = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "promote",
            "local.events",
            "--type",
            "/score=Int64",
            "--execute",
        ]);
        assert_eq!(
            recovered.exit_code, 0,
            "{failpoint:?}: {}",
            recovered.stderr
        );
        let recovered_json = stderr_or_stdout_json(&recovered.stdout);
        assert_eq!(recovered_json["result"]["phase"], "published");
        assert_eq!(recovered_json["result"]["resumed"], true);
        let state = SqlitePromotionSettlementStore::open(&state_path).unwrap();
        let head = SchemaAuthorityStore::head(&state, &authority.head.key)
            .unwrap()
            .unwrap();
        assert_eq!(head.generation, 2, "{failpoint:?}");
        assert!(matches!(head.status, cdf_kernel::SchemaHeadStatus::Active));
        let lifecycle = state
            .promotion_state(
                &authority.head.key,
                &cdf_kernel::PromotionId::new(plan.promotion_id).unwrap(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(
            lifecycle.phase,
            SchemaPromotionLifecyclePhase::Published,
            "{failpoint:?}"
        );
    }
}

#[test]
fn schema_promote_failure_reports_persisted_recovery_status_without_secret_leak() {
    let project = TestProject::new();
    let secret = format!(
        "postgresql://cdf:promotion-secret@127.0.0.1:{}/cdf",
        free_port()
    );
    fs::write(project.root.join("destination-dsn"), format!("{secret}\n")).unwrap();
    write_project_destination(&project, "postgres://secret://file/destination-dsn");
    write_parquet_discover_resource(&project, "*.parquet");
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let compile = compile_resource(&project, "local.events");
    assert_eq!(compile.exit_code, 0, "{}", compile.stderr);
    let old_hash = active_schema_hash(&project, "local.events");
    write_vendor_score_parquet(&source_path);
    write_schema_promote_package_fixture(&project, &old_hash);
    let source_package = project.root.join(".cdf/packages/pkg-promote-source");
    let mut receipts = collect_package_receipts(&PackageReader::open(&source_package).unwrap());
    receipts[0].destination = DestinationId::new("postgres").unwrap();
    fs::write(
        source_package.join(RECEIPTS_FILE),
        cdf_package::canonical_json_bytes(&receipts).unwrap(),
    )
    .unwrap();

    let json_failure = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--execute",
    ]);
    assert_ne!(json_failure.exit_code, 0);
    assert_secret_absent(&json_failure, "promotion-secret");
    assert_secret_absent(&json_failure, &secret);
    let error = stderr_or_stdout_json(&json_failure.stderr);
    assert_eq!(
        error["error"]["details"]["phase"], "staged",
        "{}",
        json_failure.stderr
    );
    assert_eq!(
        error["error"]["details"]["remaining_action"],
        "build authenticated correction packages"
    );
    assert!(
        error["error"]["details"]["recovery_command"]
            .as_str()
            .unwrap()
            .ends_with("--execute")
    );

    let human_failure = run([
        "cdf",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--execute",
    ]);
    assert_ne!(human_failure.exit_code, 0);
    assert_secret_absent(&human_failure, "promotion-secret");
    assert!(human_failure.stderr.contains("phase: staged"));
    assert!(human_failure.stderr.contains("recovery_command:"));
    assert!(
        project_tree_snapshot(&project.root)
            .into_iter()
            .filter(|(path, _)| path != "destination-dsn")
            .all(|(_, bytes)| !String::from_utf8_lossy(&bytes).contains("promotion-secret"))
    );
}

#[test]
fn schema_promote_rejects_tampered_correction_authority_before_mutation() {
    for _attempt in 0..1 {
        let project = TestProject::new();
        write_parquet_discover_resource(&project, "*.parquet");
        let source_path = project.root.join("data/events.parquet");
        write_vendor_parquet(&source_path);
        let compile = compile_resource(&project, "local.events");
        assert_eq!(compile.exit_code, 0, "{}", compile.stderr);
        let old_hash = active_schema_hash(&project, "local.events");
        write_vendor_score_parquet(&source_path);
        write_schema_promote_package_fixture(&project, &old_hash);
        let dry = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "promote",
            "local.events",
        ]);
        assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
        let plan: SchemaPromotionPlanReport =
            serde_json::from_value(stderr_or_stdout_json(&dry.stdout)["result"].clone()).unwrap();
        let context = crate::context::ProjectContext::load_with_destination_registry(
            Some(&project.root),
            None,
            &test_destination_registry(),
        )
        .unwrap();
        let resource = context.resource("local.events").unwrap();
        let target = TargetName::new(plan.targets[0].target.clone()).unwrap();
        let destination = crate::destination_uri::resolve_selected_destination(
            &test_destination_registry(),
            &context,
            &target,
            None,
        )
        .unwrap()
        .destination;
        let destinations = vec![destination];
        let authority = promotion_planning_authority(&context, resource, &destinations);
        let state_path = context.state_store_path().unwrap();
        let settlement_store = SqlitePromotionSettlementStore::open(&state_path).unwrap();
        execute_schema_promotion(SchemaPromotionExecutionRequest {
            project_root: &context.root,
            package_root: &context.package_root(),
            resource,
            authority: &authority,
            dry_plan: &plan,
            destinations,
            execution_services: test_execution_services(),
            pipeline_id: PipelineId::new("cdf-schema-promotion").unwrap(),
            lease_owner: LeaseOwnerId::new("tamper-fixture").unwrap(),
            lease_duration_ms: DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS,
            settlement_store: &settlement_store,
            failpoint: Some(SchemaPromotionExecutionFailpoint::AfterCorrectionPackages),
        })
        .unwrap_err();
        drop(settlement_store);
        drop(context);

        let correction = fs::read_dir(project.root.join(".cdf/packages"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.file_name().unwrap() != "pkg-promote-source")
            .unwrap();
        let artifact = correction.join("plan/promotion-correction.json");
        let mut bytes = fs::read(&artifact).unwrap();
        bytes.push(b' ');
        fs::write(&artifact, bytes).unwrap();
        fs::remove_dir_all(project.root.join(".cdf/packages/pkg-promote-source")).unwrap();

        let recovered = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "promote",
            "local.events",
            "--type",
            "/score=Int64",
            "--execute",
        ]);
        assert_ne!(recovered.exit_code, 0, "{}", recovered.stdout);
        let store = SqlitePromotionSettlementStore::open(&state_path).unwrap();
        let head = SchemaAuthorityStore::head(&store, &authority.head.key)
            .unwrap()
            .unwrap();
        assert_eq!(head.schema_hash.as_str(), old_hash);
        assert!(matches!(
            head.status,
            cdf_kernel::SchemaHeadStatus::Promoting { .. }
        ));
        let connection = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
        let score_columns = connection
            .prepare("SELECT count(*) FROM pragma_table_info('events') WHERE name = 'score'")
            .unwrap()
            .query_row([], |row| row.get::<_, i64>(0))
            .unwrap();
        assert_eq!(score_columns, 0);
    }
}

#[test]
fn schema_promote_api_rejects_divergent_destination_authority_before_mutation() {
    let project = TestProject::new();
    write_parquet_discover_resource(&project, "*.parquet");
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let compile = compile_resource(&project, "local.events");
    assert_eq!(compile.exit_code, 0, "{}", compile.stderr);
    let old_hash = active_schema_hash(&project, "local.events");
    write_vendor_score_parquet(&source_path);
    write_schema_promote_package_fixture(&project, &old_hash);
    let dry = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
    let plan: SchemaPromotionPlanReport =
        serde_json::from_value(stderr_or_stdout_json(&dry.stdout)["result"].clone()).unwrap();
    let context = crate::context::ProjectContext::load_with_destination_registry(
        Some(&project.root),
        None,
        &test_destination_registry(),
    )
    .unwrap();
    let resource = context.resource("local.events").unwrap();
    let target = TargetName::new(plan.targets[0].target.clone()).unwrap();
    let destination = crate::destination_uri::resolve_selected_destination(
        &test_destination_registry(),
        &context,
        &target,
        None,
    )
    .unwrap()
    .destination;
    let destinations = vec![destination];
    let mut authority = promotion_planning_authority(&context, resource, &destinations);
    authority.destinations.clear();
    let state_path = context.state_store_path().unwrap();
    let settlement_store = SqlitePromotionSettlementStore::open(&state_path).unwrap();

    let error = execute_schema_promotion(SchemaPromotionExecutionRequest {
        project_root: &context.root,
        package_root: &context.package_root(),
        resource,
        authority: &authority,
        dry_plan: &plan,
        destinations,
        execution_services: test_execution_services(),
        pipeline_id: PipelineId::new("cdf-schema-promotion").unwrap(),
        lease_owner: LeaseOwnerId::new("divergent-lock-fixture").unwrap(),
        lease_duration_ms: DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS,
        settlement_store: &settlement_store,
        failpoint: None,
    })
    .unwrap_err();

    assert!(error.message.contains("destination sheet"), "{error}");
    assert!(!project.root.join(".cdf/promotions").exists());
    assert_eq!(
        fs::read_dir(project.root.join(".cdf/packages"))
            .unwrap()
            .count(),
        1
    );
    assert!(
        settlement_store
            .promotion_state(
                &authority.head.key,
                &cdf_kernel::PromotionId::new(plan.promotion_id).unwrap(),
            )
            .unwrap()
            .is_none()
    );
}

#[test]
fn schema_promote_rejects_semantically_rebuilt_correction_packages_without_sources() {
    for tamper in [
        CorrectionSemanticRepackage::Subset,
        CorrectionSemanticRepackage::ValueSubstitution,
    ] {
        let project = TestProject::new();
        write_parquet_discover_resource(&project, "*.parquet");
        let source_path = project.root.join("data/events.parquet");
        write_vendor_parquet(&source_path);
        let compile = compile_resource(&project, "local.events");
        assert_eq!(compile.exit_code, 0, "{}", compile.stderr);
        let old_hash = active_schema_hash(&project, "local.events");
        write_vendor_score_parquet(&source_path);
        write_schema_promote_package_fixture(&project, &old_hash);
        let dry = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "promote",
            "local.events",
        ]);
        assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
        let plan: SchemaPromotionPlanReport =
            serde_json::from_value(stderr_or_stdout_json(&dry.stdout)["result"].clone()).unwrap();
        let context = crate::context::ProjectContext::load_with_destination_registry(
            Some(&project.root),
            None,
            &test_destination_registry(),
        )
        .unwrap();
        let resource = context.resource("local.events").unwrap();
        let target = TargetName::new(plan.targets[0].target.clone()).unwrap();
        let destination = crate::destination_uri::resolve_selected_destination(
            &test_destination_registry(),
            &context,
            &target,
            None,
        )
        .unwrap()
        .destination;
        let destinations = vec![destination];
        let authority = promotion_planning_authority(&context, resource, &destinations);
        let state_path = context.state_store_path().unwrap();
        let settlement_store = SqlitePromotionSettlementStore::open(&state_path).unwrap();
        execute_schema_promotion(SchemaPromotionExecutionRequest {
            project_root: &context.root,
            package_root: &context.package_root(),
            resource,
            authority: &authority,
            dry_plan: &plan,
            destinations,
            execution_services: test_execution_services(),
            pipeline_id: PipelineId::new("cdf-schema-promotion").unwrap(),
            lease_owner: LeaseOwnerId::new("semantic-repackage-fixture").unwrap(),
            lease_duration_ms: DEFAULT_SCHEMA_PROMOTION_LEASE_DURATION_MS,
            settlement_store: &settlement_store,
            failpoint: Some(SchemaPromotionExecutionFailpoint::AfterCorrectionPackages),
        })
        .unwrap_err();
        drop(settlement_store);
        drop(context);

        let correction = fs::read_dir(project.root.join(".cdf/packages"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| path.file_name().unwrap() != "pkg-promote-source")
            .unwrap();
        rebuild_correction_package_semantically(&correction, tamper);
        fs::remove_dir_all(project.root.join(".cdf/packages/pkg-promote-source")).unwrap();

        let recovered = run([
            "cdf",
            "--json",
            "--project",
            project.root_str(),
            "schema",
            "promote",
            "local.events",
            "--type",
            "/score=Int64",
            "--execute",
        ]);
        assert_ne!(recovered.exit_code, 0, "{}", recovered.stdout);
        assert!(collect_package_receipts(&PackageReader::open(&correction).unwrap()).is_empty());
        let connection = DuckConnection::open(project.root.join(".cdf/dev.duckdb")).unwrap();
        let score_columns = connection
            .prepare("SELECT count(*) FROM pragma_table_info('events') WHERE name = 'score'")
            .unwrap()
            .query_row([], |row| row.get::<_, i64>(0))
            .unwrap();
        assert_eq!(score_columns, 0);
        let state = SqlitePromotionSettlementStore::open(&state_path).unwrap();
        let head = SchemaAuthorityStore::head(&state, &authority.head.key)
            .unwrap()
            .unwrap();
        assert!(matches!(
            head.status,
            cdf_kernel::SchemaHeadStatus::Promoting { .. }
        ));
    }
}

#[test]
fn schema_promote_execute_routes_parquet_through_correction_sidecar() {
    let project = TestProject::new();
    let project_toml = fs::read_to_string(project.root.join("cdf.toml"))
        .unwrap()
        .replace("duckdb://.cdf/dev.duckdb", "parquet://.cdf/parquet");
    fs::write(project.root.join("cdf.toml"), project_toml).unwrap();
    write_parquet_discover_resource(&project, "*.parquet");
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let compile = compile_resource(&project, "local.events");
    assert_eq!(compile.exit_code, 0, "{}", compile.stderr);
    let old_hash = active_schema_hash(&project, "local.events");
    let target = TargetName::new("events").unwrap();
    let policy = cdf_project::DestinationPolicy::default();
    let services = test_execution_services();
    let resolution = cdf_project::ProjectResolutionContext::for_project_run(&project.root, &target)
        .with_environment_name("dev")
        .with_destination_policy(&policy)
        .with_execution_services(&services);
    let registry = crate::destination_registry::builtin_destination_registry().unwrap();
    let mut runtime = registry
        .resolve("parquet://.cdf/parquet", &resolution)
        .unwrap();
    runtime.ensure_protocol_ready().unwrap();
    let mut lock = parse_lock(&fs::read_to_string(project.root.join("cdf.lock")).unwrap()).unwrap();
    let artifact = runtime.protocol().sheet_artifact().unwrap();
    lock.resources
        .get_mut("local.events")
        .unwrap()
        .destinations
        .insert(
            artifact.sheet.destination.to_string(),
            cdf_project::LockedDestination::new(artifact).unwrap(),
        );
    fs::write(
        project.root.join("cdf.lock"),
        cdf_project::lock_to_toml(&lock).unwrap(),
    )
    .unwrap();
    write_vendor_score_parquet(&source_path);
    write_schema_promote_package_fixture_for_target_with_commit(
        &project,
        "pkg-promote-source",
        "events",
        &old_hash,
        false,
    );
    let source_package = project.root.join(".cdf/packages/pkg-promote-source");
    let store = SqliteCheckpointStore::open(
        project
            .root
            .join(".cdf/schema-promote-parquet-fixture-state.db"),
    )
    .unwrap();
    replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: source_package,
        destination: ResolvedProjectDestination::new(
            Box::new(
                ParquetDestination::new_filesystem(
                    project.root.join(".cdf/parquet"),
                    services.clone(),
                )
                .unwrap(),
            ),
            target.clone(),
        )
        .with_bound_execution_services(services)
        .unwrap(),
        checkpoint_store: &store,
        after_receipt_verified: None,
    })
    .unwrap();

    let dry = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
    ]);
    assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
    let dry_json = stderr_or_stdout_json(&dry.stdout);
    assert_eq!(dry_json["result"]["executable"], true, "{}", dry.stdout);

    let executed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--execute",
    ]);
    assert_eq!(executed.exit_code, 0, "{}", executed.stderr);
    let json = stderr_or_stdout_json(&executed.stdout);
    assert_eq!(json["result"]["phase"], "complete");
    assert_eq!(
        json["result"]["targets"][0]["destination"],
        "parquet_object_store"
    );
    assert_eq!(json["result"]["targets"][0]["committed"], true);
    assert_eq!(json["result"]["lock_published"], true);
    assert_eq!(json["result"]["publication_event_recorded"], true);
    assert!(
        project_tree_snapshot(&project.root)
            .keys()
            .any(|path| path.starts_with(".cdf/parquet/targets/events/corrections/manifests/"))
    );
}

#[test]
fn schema_promote_execute_updates_postgres_through_generic_command_dispatch() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let project = TestProject::new();
    fs::write(
        project.root.join("destination-dsn"),
        format!("{}\n", postgres.url),
    )
    .unwrap();
    write_project_destination(&project, "postgres://secret://file/destination-dsn");
    write_parquet_discover_resource(&project, "*.parquet");
    let source_path = project.root.join("data/events.parquet");
    write_vendor_parquet(&source_path);
    let compile = compile_resource(&project, "local.events");
    assert_eq!(compile.exit_code, 0, "{}", compile.stderr);
    let old_hash = active_schema_hash(&project, "local.events");
    write_vendor_score_parquet(&source_path);
    let target = postgres.table("events_promotion");
    write_schema_promote_package_fixture_for_target_with_commit(
        &project,
        "pkg-promote-source",
        &target,
        &old_hash,
        false,
    );
    let package_dir = project.root.join(".cdf/packages/pkg-promote-source");
    let reader = PackageReader::open(&package_dir).unwrap();
    let package_hash = PackageHash::new(reader.manifest().package_hash.clone()).unwrap();
    let delta = reader
        .state_delta_preimage()
        .unwrap()
        .into_state_delta(package_hash.clone());
    let segment = &delta.segments[0];
    let batches = reader
        .verified_canonical_segment_stream(test_execution_services().memory(), 128 * 1024 * 1024)
        .unwrap()
        .find_map(|candidate| {
            let candidate = candidate.unwrap();
            (candidate.entry.segment_id == segment.segment_id).then_some(candidate.batches)
        })
        .unwrap();
    let residuals = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let row_key_start = 1_i64;
    let row_key_end = row_key_start + segment.row_count as i64;
    let mut client = postgres.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (vendor_id INTEGER NOT NULL, _cdf_variant TEXT, _cdf_row_key BIGINT NOT NULL, _cdf_loaded_at_ms BIGINT NOT NULL); \
             CREATE TABLE {}._cdf_segments (row_key_start BIGINT PRIMARY KEY, row_key_end BIGINT NOT NULL, target TEXT NOT NULL, package_hash TEXT NOT NULL, segment_id TEXT NOT NULL, CHECK (row_key_start < row_key_end), UNIQUE (target, package_hash, segment_id))",
            target, postgres.schema
        ))
        .unwrap();
    for (row, vendor_id) in [1_i32, 2_i32].into_iter().enumerate() {
        client
            .execute(
                &format!(
                    "INSERT INTO {} (vendor_id, _cdf_variant, _cdf_row_key, _cdf_loaded_at_ms) VALUES ($1, $2, $3, $4)",
                    target
                ),
                &[
                    &vendor_id,
                    &residuals.value(row),
                    &(row_key_start + row as i64),
                    &1_i64,
                ],
            )
            .unwrap();
    }
    client
        .execute(
            &format!(
                "INSERT INTO {}._cdf_segments (row_key_start, row_key_end, target, package_hash, segment_id) VALUES ($1, $2, $3, $4, $5)",
                postgres.schema
            ),
            &[
                &row_key_start,
                &row_key_end,
                &target,
                &package_hash.as_str(),
                &segment.segment_id.as_str(),
            ],
        )
        .unwrap();
    let receipt = Receipt {
            receipt_id: ReceiptId::new("receipt-postgres-promotion-source").unwrap(),
            destination: DestinationId::new("postgres").unwrap(),
            target: TargetName::new(target.clone()).unwrap(),
            package_hash: package_hash.clone(),
            segment_acks: vec![SegmentAck {
                segment_id: segment.segment_id.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            }],
            disposition: WriteDisposition::Append,
            idempotency_token: IdempotencyToken::new(package_hash.as_str()).unwrap(),
            transaction: None,
            counts: CommitCounts {
                rows_written: 2,
                rows_inserted: Some(2),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: SchemaHash::new(&old_hash).unwrap(),
            migrations: Vec::new(),
            committed_at_ms: now_ms_for_test(),
            verify: VerifyClause {
                kind: "postgres_sql".to_owned(),
                statement: "SELECT \"receipt_id\", \"xid\", \"rows_written\", \"schema_hash\", \"receipt_json\"::text AS \"receipt_json\" FROM \"_cdf_loads\" WHERE \"destination\" = 'postgres' AND \"target\" = $1 AND \"package_hash\" = $2 AND \"idempotency_token\" = $3 AND \"schema_hash\" = $4".to_owned(),
                parameters: BTreeMap::from([
                    ("target".to_owned(), target.clone()),
                    ("package_hash".to_owned(), package_hash.to_string()),
                    ("idempotency_token".to_owned(), package_hash.to_string()),
                    ("schema_hash".to_owned(), old_hash.clone()),
                    ("destination".to_owned(), "postgres".to_owned()),
                    ("target_schema".to_owned(), postgres.schema.clone()),
                ]),
            },
        };
    client
        .batch_execute(&format!(
            "CREATE TABLE {}._cdf_loads (receipt_id TEXT PRIMARY KEY, destination TEXT NOT NULL, target TEXT NOT NULL, resource_id TEXT, package_hash TEXT NOT NULL, idempotency_token TEXT NOT NULL, disposition TEXT NOT NULL, schema_hash TEXT NOT NULL, rows_written BIGINT NOT NULL, rows_inserted BIGINT, rows_updated BIGINT, rows_deleted BIGINT, segment_count BIGINT NOT NULL, migrations_json JSONB NOT NULL, receipt_json JSONB NOT NULL, xid TEXT NOT NULL, duplicate BOOLEAN NOT NULL DEFAULT FALSE, committed_at_ms BIGINT NOT NULL, UNIQUE (target, package_hash))",
            postgres.schema
        ))
        .unwrap();
    let receipt_json = serde_json::to_string(&receipt).unwrap();
    client
        .execute(
            &format!("INSERT INTO {}._cdf_loads (receipt_id, destination, target, resource_id, package_hash, idempotency_token, disposition, schema_hash, rows_written, rows_inserted, rows_updated, rows_deleted, segment_count, migrations_json, receipt_json, xid, duplicate, committed_at_ms) VALUES ($1, 'postgres', $2, 'local.events', $3, $4, 'append', $5, 2, 2, 0, 0, 1, '[]'::jsonb, $6::text::jsonb, 'fixture', false, $7)", postgres.schema),
            &[&receipt.receipt_id.as_str(), &target, &package_hash.as_str(), &package_hash.as_str(), &old_hash, &receipt_json, &receipt.committed_at_ms],
        )
        .unwrap();
    reader.append_receipt(receipt).unwrap();
    drop(client);

    let executed = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "promote",
        "local.events",
        "--execute",
    ]);
    assert_eq!(executed.exit_code, 0, "{}", executed.stderr);
    assert_secret_absent(&executed, &postgres.url);
    let report = stderr_or_stdout_json(&executed.stdout);
    assert_eq!(report["result"]["targets"][0]["destination"], "postgres");
    assert_eq!(report["result"]["targets"][0]["committed"], true);
    let rows = postgres
        .client()
        .query(
            &format!("SELECT vendor_id, score, _cdf_variant FROM {target} ORDER BY _cdf_row_key"),
            &[],
        )
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, i64>(1), 10);
    assert_eq!(rows[0].get::<_, Option<String>>(2), None);
    assert_eq!(rows[1].get::<_, i64>(1), 20);
}

#[test]
fn schema_diff_rest_compares_pinned_snapshot_to_fresh_probe_without_writes_or_secret_leak() {
    let project = TestProject::new();
    write_minimal_lockfile(&project);
    fs::write(project.root.join("rest-token"), "rest-diff-secret\n").unwrap();
    let (base_url, requests) = serve_json_sequence([
        r#"{ "items": [{ "VendorID": 1, "updated_at": 10 }] }"#,
        r#"{ "items": [{ "VendorID": 1, "updated_at": 10, "score": 4.5 }] }"#,
    ]);
    write_rest_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        &base_url,
        "secret://file/rest-token",
    );
    fs::write(
        project.root.join("cdf/api/items.cdf.sql"),
        rest_resource_sql("exact"),
    )
    .unwrap();

    let compile = compile_resource(&project, "api.items");
    assert_eq!(compile.exit_code, 0, "stderr: {}", compile.stderr);
    assert_secret_absent(&compile, "rest-diff-secret");
    let pinned_snapshot_count = fs::read_dir(project.root.join(".cdf/schemas"))
        .unwrap()
        .count();
    assert!(pinned_snapshot_count >= 2);

    let diff = run([
        "cdf",
        "--json",
        "--project",
        project.root_str(),
        "schema",
        "diff",
        "api.items",
    ]);

    assert_eq!(diff.exit_code, 0, "stderr: {}", diff.stderr);
    assert_secret_absent(&diff, "rest-diff-secret");
    assert!(!project.root.join(".cdf/packages").exists());
    assert!(!project.root.join(".cdf/state.db").exists());
    assert!(!project.root.join(".cdf/dev.duckdb").exists());
    assert_eq!(
        fs::read_dir(project.root.join(".cdf/schemas"))
            .unwrap()
            .count(),
        pinned_snapshot_count
    );
    let diff_json = stderr_or_stdout_json(&diff.stdout);
    let report = &diff_json["result"];
    assert_eq!(report["summary"]["changed"], true);
    assert_eq!(report["summary"]["added_fields"], 1);
    assert_eq!(report["added_fields"][0]["name"], "score");
    assert_eq!(report["writes"]["schema_snapshot"], false);
    assert_eq!(report["writes"]["lockfile"], false);

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.contains("authorization: Bearer rest-diff-secret"))
    );
}

#[test]
fn compile_postgres_catalog_establishes_state_without_secret_leak() {
    let Some(postgres) = LivePostgres::start() else {
        return;
    };
    let table = postgres.table("schema_pin_orders");
    let mut client = postgres.client();
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (
                \"VendorID\" INTEGER NOT NULL,
                \"updated_at\" BIGINT NOT NULL
            )",
            table
        ))
        .unwrap();

    let project = TestProject::new();
    let source_dsn =
        postgres
            .url
            .replacen("postgresql://cdf@", "postgresql://cdf:compile-secret@", 1);
    fs::write(project.root.join("postgres-dsn"), format!("{source_dsn}\n")).unwrap();
    write_secret_project(
        &project,
        "duckdb://.cdf/dev.duckdb",
        None,
        Some("secret://file/postgres-dsn"),
    );
    fs::write(
        project.root.join("cdf/warehouse/orders.cdf.sql"),
        postgres_resource_sql(&table, false),
    )
    .unwrap();

    let result = compile_resource(&project, "warehouse.orders");

    assert_eq!(result.exit_code, 0, "stderr: {}", result.stderr);
    assert_secret_absent(&result, &source_dsn);
    assert_secret_absent(&result, "compile-secret");
    assert_generated_artifacts_exclude(&project.root, &source_dsn);
    assert_generated_artifacts_exclude(&project.root, "compile-secret");
    assert!(project.root.join(".cdf/state.db").is_file());
    assert!(project.root.join(".cdf/manifest.json").is_file());
    assert!(!project.root.join("cdf.lock").exists());
    let json = stderr_or_stdout_json(&result.stdout);
    assert_eq!(
        json["result"]["resources"][0]["schema_authority"]["status"],
        "established"
    );
}
