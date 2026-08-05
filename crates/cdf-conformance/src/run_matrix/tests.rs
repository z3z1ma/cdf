use super::{
    ExcludedMatrixCell, MatrixDestination, RunMatrixCell, RunMatrixOutput, SourceArchetype, core,
    destination_matrix_cells,
    destinations::{ConformanceEnvironment, target_table_for_cell},
    run_spine_matrix_cells, source_catalog, source_matrix_cells,
};

const RUN_MATRIX_DESTINATION_ENV: &str = "CDF_RUN_MATRIX_DESTINATION";
const RUN_MATRIX_SOURCE_ENV: &str = "CDF_RUN_MATRIX_SOURCE";
const RUN_MATRIX_SHARDS_JSON: &str = include_str!("../../run-matrix-shards.json");

#[test]
fn generated_target_names_are_stable_and_valid_for_every_catalog_destination() {
    let temp = tempfile::tempdir().unwrap();
    for cell in run_spine_matrix_cells() {
        let target = target_table_for_cell(&cell);
        assert_eq!(
            target,
            format!(
                "cdf_{}_events_{}",
                cell.source_archetype.as_str(),
                cell.disposition.as_str()
            )
        );
        assert!(target.starts_with("cdf_"));
        assert!(!target.starts_with("_cdf_"));
        assert!(!target.starts_with("sqlite_"));
        cdf_kernel::TargetName::new(&target).unwrap();

        let rules = crate::destination_catalog::destination_identifier_rules(
            &cell.destination,
            temp.path(),
        )
        .unwrap();
        let policy = cdf_contract::identifier_policy_from_destination_rules(&rules).unwrap();
        assert_eq!(
            cdf_contract::normalize_identifier(&target, &policy).unwrap(),
            target,
            "generated target must satisfy the published rules for {}",
            cell.destination.as_str()
        );
    }
}

#[test]
fn registered_run_matrix_shards_cover_source_catalog() {
    let shards = declared_shards();
    let catalog = source_catalog::archetypes();
    assert_eq!(
        shards, catalog,
        "run-matrix shards must cover the catalog exactly"
    );
    assert_eq!(
        shards
            .iter()
            .map(|source| source_matrix_cells(source.clone()).len())
            .sum::<usize>(),
        run_spine_matrix_cells().len(),
        "sharded and aggregate run-matrix coverage must contain the same cells"
    );
}

#[test]
#[ignore = "scheduled source shard; set CDF_RUN_MATRIX_SOURCE and run this test explicitly"]
fn registered_source_shard_cells_persist_output() {
    let source = SourceArchetype::new(
        std::env::var(RUN_MATRIX_SOURCE_ENV)
            .unwrap_or_else(|_| panic!("{RUN_MATRIX_SOURCE_ENV} must name a declared shard")),
    )
    .expect("run-matrix source shard must be a valid archetype");
    assert!(
        declared_shards().contains(&source),
        "run-matrix source shard `{source}` is not declared in run-matrix-shards.json"
    );

    let environment = ConformanceEnvironment::start().expect(
        "C2 run matrix requires Postgres coverage; set TEST_DATABASE_URL or install initdb/pg_ctl",
    );
    let cells = source_matrix_cells(source.clone());
    let output = execute_cells(cells.clone(), &environment);

    assert_source_counts(&output, &source);
    assert_required_cells(&output, &cells);

    let serialized = serde_json::to_string(&output).unwrap();
    assert!(!serialized.contains("run-matrix-token"));
    environment.assert_redacted(&serialized);
    println!("CDF_RUN_MATRIX_OUTPUT={serialized}");
}

#[test]
#[ignore = "connector certification destination slice; set CDF_RUN_MATRIX_DESTINATION"]
fn registered_destination_shard_cells_persist_output() {
    let destination = MatrixDestination::new(
        std::env::var(RUN_MATRIX_DESTINATION_ENV)
            .unwrap_or_else(|_| panic!("{RUN_MATRIX_DESTINATION_ENV} must name a destination")),
    )
    .expect("run-matrix destination must be a valid identifier");
    assert!(
        crate::destination_catalog::conformance_destinations().contains(&destination),
        "run-matrix destination `{}` is absent from the conformance catalog",
        destination.as_str()
    );

    let environment = ConformanceEnvironment::start().expect(
        "connector certification requires Postgres coverage; set TEST_DATABASE_URL or install initdb/pg_ctl",
    );
    let cells = destination_matrix_cells(&destination);
    let output = execute_cells(cells.clone(), &environment);
    assert_eq!(
        output.executed_cells.len() + output.excluded_cells.len(),
        cells.len()
    );
    assert_required_cells(&output, &cells);

    let serialized = serde_json::to_string(&output).unwrap();
    assert!(!serialized.contains("run-matrix-token"));
    environment.assert_redacted(&serialized);
    println!("CDF_RUN_MATRIX_OUTPUT={serialized}");
}

#[test]
#[ignore = "live MongoDB runtime-drift boundary; set CDF_MONGODB_ENDPOINT"]
fn mongodb_current_query_project_runtime_drift_uses_compiled_residual_policy() {
    let environment = ConformanceEnvironment::start().expect(
        "MongoDB drift conformance requires Postgres coverage; set TEST_DATABASE_URL or install initdb/pg_ctl",
    );
    let temp = tempfile::tempdir().unwrap();
    let cell = RunMatrixCell::new(
        SourceArchetype::new("mongodb").unwrap(),
        MatrixDestination::new("duckdb").unwrap(),
        super::MatrixDisposition::Append,
    );
    let source = source_catalog::prepare(&cell, temp.path(), &environment).unwrap();
    super::mongodb_fixture::add_runtime_unknown_field(&cell).unwrap();
    let plan = source
        .engine_plan("mongodb-runtime-drift", cell.disposition, None)
        .unwrap();

    let preview = futures_executor::block_on(cdf_engine::preview_resource(
        &plan,
        source.queryable(),
        cdf_engine::EnginePreviewLimits::default(),
    ))
    .unwrap();

    assert_eq!(preview.row_count, 2);
    assert_eq!(preview.residual_row_count, 1);
    assert!(!preview.fields.iter().any(|field| field == "runtime_extra"));
}

#[test]
#[ignore = "live MongoDB full-queue cancellation boundary; set CDF_MONGODB_ENDPOINT"]
fn mongodb_live_full_queue_cancellation_joins_source_invocation() {
    let environment = ConformanceEnvironment::start().expect(
        "MongoDB cancellation conformance requires Postgres coverage and CDF_MONGODB_ENDPOINT",
    );
    let temp = tempfile::tempdir().unwrap();
    let cell = RunMatrixCell::new(
        SourceArchetype::new("mongodb").unwrap(),
        MatrixDestination::new("duckdb").unwrap(),
        super::MatrixDisposition::Append,
    );
    let source = source_catalog::prepare(&cell, temp.path(), &environment).unwrap();
    let scan = source
        .queryable()
        .negotiate(&cdf_kernel::ScanRequest {
            resource_id: source.queryable().descriptor().resource_id.clone(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: source.queryable().descriptor().state_scope.clone(),
        })
        .unwrap();
    let partition = scan.inline_partitions().unwrap()[0].clone();
    let mut stream = futures_executor::block_on(source.queryable().open(partition)).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(100));
    futures_executor::block_on(stream.terminate_and_join()).unwrap();
}

#[test]
#[ignore = "live Postgres compatible-drift boundary; set TEST_DATABASE_URL"]
fn postgres_current_query_project_admits_compatible_live_catalog_drift() {
    let environment = ConformanceEnvironment::start()
        .expect("Postgres drift conformance requires TEST_DATABASE_URL or local initdb/pg_ctl");
    let temp = tempfile::tempdir().unwrap();
    let cell = RunMatrixCell::new(
        SourceArchetype::new("postgres").unwrap(),
        MatrixDestination::new("duckdb").unwrap(),
        super::MatrixDisposition::Append,
    );
    let source = source_catalog::prepare(&cell, temp.path(), &environment).unwrap();
    super::postgres_fixture::make_runtime_id_physically_narrower(
        &cell,
        environment.postgres().unwrap(),
    )
    .unwrap();
    let plan = source
        .engine_plan("postgres-compatible-runtime-drift", cell.disposition, None)
        .unwrap();

    let preview = futures_executor::block_on(cdf_engine::preview_resource(
        &plan,
        source.queryable(),
        cdf_engine::EnginePreviewLimits::default(),
    ))
    .unwrap();

    assert_eq!(preview.row_count, 2);
    assert_eq!(preview.residual_row_count, 0);
    assert_eq!(preview.quarantined_row_count, 0);
}

#[test]
#[ignore = "live MongoDB public CLI lifecycle; set CDF_MONGODB_ENDPOINT"]
fn mongodb_public_cli_lifecycle_is_current_redacted_and_jobs_invariant() {
    let endpoint = std::env::var("CDF_MONGODB_ENDPOINT")
        .expect("CDF_MONGODB_ENDPOINT must name the live MongoDB endpoint");
    let collection = "cdf_public_cli_lifecycle";
    super::mongodb_fixture::seed_collection(&endpoint, collection).unwrap();
    let base = tempfile::tempdir().unwrap();
    let project_root = base.path().join("project");
    let init = cdf_cli::invoke([
        std::ffi::OsString::from("cdf"),
        "--json".into(),
        "init".into(),
        project_root.as_os_str().to_owned(),
    ]);
    assert_eq!(init.exit_code, 0, "{}", init.stderr);
    let mut location = url::Url::parse(&endpoint).unwrap();
    assert!(
        !location.username().is_empty() && location.password().is_some(),
        "CDF_MONGODB_ENDPOINT must include fixture credentials so runtime secret resolution is exercised"
    );
    let encoded_secret = location.password().unwrap().to_owned();
    location.set_path(&format!("/cdf_conformance/{collection}"));
    let location = location.to_string();
    let auth_source =
        std::env::var("CDF_MONGODB_AUTH_SOURCE").unwrap_or_else(|_| "admin".to_owned());

    let dry = invoke_public_cli(
        &project_root,
        false,
        &[
            "add",
            "warehouse.events",
            &location,
            "--option",
            "cursor=updated_at",
            "--option",
            "batch_rows=1",
            "--option",
            &format!("auth_source={auth_source}"),
            "--dry-run",
        ],
    );
    assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
    let add = invoke_public_cli(
        &project_root,
        true,
        &[
            "add",
            "warehouse.events",
            &location,
            "--option",
            "cursor=updated_at",
            "--option",
            "batch_rows=1",
            "--option",
            &format!("auth_source={auth_source}"),
        ],
    );
    assert_eq!(add.exit_code, 0, "{}", add.stderr);
    let secret = walk_files(&project_root.join(".cdf/secrets"))
        .into_iter()
        .filter_map(|path| std::fs::read_to_string(path).ok())
        .find(|value| {
            url::form_urlencoded::byte_serialize(value.as_bytes()).collect::<String>()
                == encoded_secret
        })
        .expect("MongoDB add must publish the decoded credential to a private secret file");
    assert_ne!(
        secret, encoded_secret,
        "authenticated lifecycle fixture must distinguish decoded and URL-encoded secret forms"
    );
    assert!(location.contains(&encoded_secret));
    assert_invocation_redacted(&dry, &secret);
    assert_invocation_redacted(&add, &secret);

    // Remove only the scaffolded local source; MongoDB credential references stay active so every
    // contact-bearing lifecycle command exercises secret resolution and output redaction.
    let config_path = project_root.join("cdf.toml");
    let config = std::fs::read_to_string(&config_path).unwrap();
    let mut removing_local = false;
    let config = config
        .lines()
        .filter(|line| {
            if line.trim() == "[sources.local]" {
                removing_local = true;
                return false;
            }
            if removing_local && line.starts_with('[') {
                removing_local = false;
            }
            !removing_local
        })
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(&config_path, format!("{config}\n")).unwrap();
    std::fs::remove_dir_all(project_root.join("cdf/local")).unwrap();

    for command in [
        vec!["schema", "discover", "warehouse.events"],
        vec!["schema", "pin", "warehouse.events"],
        vec!["compile", "--refresh"],
        vec!["validate"],
        vec!["plan", "warehouse.events"],
    ] {
        let result = invoke_public_cli(&project_root, true, &command);
        assert_eq!(
            result.exit_code, 0,
            "command {command:?} failed: {}",
            result.stderr
        );
        assert_invocation_redacted(&result, &secret);
    }
    super::mongodb_fixture::make_lifecycle_runtime_cursor_physically_narrower(
        &endpoint, collection,
    )
    .unwrap();
    for command in [vec!["preview", "warehouse.events"], vec!["doctor"]] {
        let result = invoke_public_cli(&project_root, true, &command);
        assert_eq!(
            result.exit_code, 0,
            "command {command:?} failed: {}",
            result.stderr
        );
        assert_invocation_redacted(&result, &secret);
    }

    let jobs_one = base.path().join("jobs-one");
    let jobs_four = base.path().join("jobs-four");
    copy_tree(&project_root, &jobs_one, &[]);
    copy_tree(&project_root, &jobs_four, &[]);
    let first = invoke_public_cli(&jobs_one, true, &["run", "warehouse.events", "--jobs", "1"]);
    let second = invoke_public_cli(
        &jobs_four,
        true,
        &["run", "warehouse.events", "--jobs", "4"],
    );
    assert_eq!(first.exit_code, 0, "{}", first.stderr);
    assert_eq!(second.exit_code, 0, "{}", second.stderr);
    assert_invocation_redacted(&first, &secret);
    assert_invocation_redacted(&second, &secret);
    let first_json: serde_json::Value = serde_json::from_str(&first.stdout).unwrap();
    let second_json: serde_json::Value = serde_json::from_str(&second.stdout).unwrap();
    assert_eq!(first_json["result"]["row_count"], 2);
    assert_eq!(second_json["result"]["row_count"], 2);
    assert_eq!(
        first_json["result"]["schema_hash"],
        second_json["result"]["schema_hash"]
    );

    let package_id = first_json["result"]["package_id"].as_str().unwrap();
    let second_package_id = second_json["result"]["package_id"].as_str().unwrap();
    let package = jobs_one.join(".cdf/packages").join(package_id);
    let second_package = jobs_four.join(".cdf/packages").join(second_package_id);
    assert_eq!(
        package_identity_semantics(&package),
        package_identity_semantics(&second_package)
    );
    assert_eq!(
        checkpoint_position_semantics(&package),
        checkpoint_position_semantics(&second_package)
    );
    assert_eq!(
        receipt_semantics(&package),
        receipt_semantics(&second_package)
    );
    assert_physical_reconciliation_stays_out_of_residual_variant(&package);
    std::fs::remove_file(jobs_one.join(".cdf/state.db")).unwrap();
    std::fs::remove_file(jobs_one.join(".cdf/dev.duckdb")).unwrap();
    let replay = invoke_public_cli(
        &jobs_one,
        true,
        &["replay", "package", package.to_str().unwrap()],
    );
    assert_eq!(replay.exit_code, 0, "{}", replay.stderr);
    assert_invocation_redacted(&replay, &secret);
}

fn assert_physical_reconciliation_stays_out_of_residual_variant(package: &std::path::Path) {
    super::mongodb_fixture::assert_physical_reconciliation_stays_out_of_residual_variant(package);
}

fn invoke_public_cli(
    root: &std::path::Path,
    json: bool,
    command: &[&str],
) -> cdf_cli_core::output::InvocationResult {
    let mut args = vec![std::ffi::OsString::from("cdf")];
    if json {
        args.push("--json".into());
    }
    args.extend(["--project".into(), root.as_os_str().to_owned()]);
    args.extend(command.iter().map(std::ffi::OsString::from));
    cdf_cli::invoke(args)
}

fn assert_invocation_redacted(result: &cdf_cli_core::output::InvocationResult, secret: &str) {
    let encoded = url::form_urlencoded::byte_serialize(secret.as_bytes()).collect::<String>();
    for sensitive in [secret, encoded.as_str()] {
        assert!(!result.stdout.contains(sensitive));
        assert!(!result.stderr.contains(sensitive));
    }
}

fn walk_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files = Vec::new();
    if !root.exists() {
        return files;
    }
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files.extend(walk_files(&path));
        } else {
            files.push(path);
        }
    }
    files
}

fn package_identity_semantics(package: &std::path::Path) -> serde_json::Value {
    let reader = cdf_package::PackageReader::open(package).unwrap();
    let header = reader.manifest();
    let mut files = Vec::new();
    reader
        .for_each_identity_file(&mut |file| {
            let semantics = identity_file_content_semantics(package, &file.path, file.sha256);
            files.push((file.path, file.byte_count, semantics));
            Ok(())
        })
        .unwrap();
    let mut segments = Vec::new();
    reader
        .for_each_identity_segment(&mut |segment| {
            segments.push((
                segment.segment_id.to_string(),
                segment.path,
                segment.package_row_ord_start,
                segment.row_count,
                segment.byte_count,
                segment.sha256,
            ));
            Ok(())
        })
        .unwrap();
    serde_json::json!({
        "manifest_version": header.manifest_version,
        "identity_manifest_version": header.identity.manifest_version,
        "layout": header.identity.layout,
        "file_count": header.identity.file_count,
        "file_bytes": header.identity.file_bytes,
        "files": files,
        "segment_count": header.identity.segment_count,
        "segments": segments,
        "lifecycle": header.lifecycle,
        "archives": header.archives,
    })
}

fn identity_file_content_semantics(
    package: &std::path::Path,
    path: &str,
    sha256: String,
) -> serde_json::Value {
    let pointer = match path {
        "plan/explain.json" => None,
        "plan/schema-admission.json" => Some("/source/compiled_source_plan_hash"),
        "schema/stream-admission-evidence.json" => Some("/compiled_admission_hash"),
        "state/proposed_delta.json" => Some("/checkpoint_id"),
        _ => return serde_json::Value::String(sha256),
    };
    let mut value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(package.join(path)).unwrap()).unwrap();
    let invocation_specific = if let Some(pointer) = pointer {
        value
            .pointer_mut(pointer)
            .unwrap_or_else(|| panic!("identity artifact {path} omitted {pointer}"))
    } else {
        value["operator_chain"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|operator| operator["kind"] == "package_sink")
            .and_then(|operator| operator.get_mut("package_id"))
            .unwrap_or_else(|| panic!("identity artifact {path} omitted package-sink identity"))
    };
    *invocation_specific = serde_json::Value::String("<invocation-specific>".to_owned());
    serde_json::Value::String(cdf_runtime::artifact_hash(&value).unwrap())
}

fn checkpoint_position_semantics(package: &std::path::Path) -> serde_json::Value {
    let delta = cdf_package::PackageReader::open(package)
        .unwrap()
        .state_delta_preimage()
        .unwrap();
    let segments = delta
        .segments
        .into_iter()
        .map(|segment| {
            serde_json::json!({
                "segment_id": segment.segment_id,
                "scope": segment.scope,
                "output_position": segment.output_position,
                "row_count": segment.row_count,
                "byte_count": segment.byte_count,
            })
        })
        .collect::<Vec<_>>();
    serde_json::json!({
        "pipeline_id": delta.pipeline_id,
        "resource_id": delta.resource_id,
        "scope": delta.scope,
        "state_version": delta.state_version,
        "input_position": delta.input_position,
        "output_position": delta.output_position,
        "output_watermark": delta.output_watermark,
        "partition_watermarks": delta.partition_watermarks,
        "late_data_carryover": delta.late_data_carryover,
        "source_continuation": delta.source_continuation,
        "schema_hash": delta.schema_hash,
        "segments": segments,
    })
}

fn receipt_semantics(package: &std::path::Path) -> serde_json::Value {
    let reader = cdf_package::PackageReader::open(package).unwrap();
    let project_root = package
        .parent()
        .and_then(std::path::Path::parent)
        .and_then(std::path::Path::parent)
        .expect("package must live below <project>/.cdf/packages");
    let mut receipts = Vec::new();
    reader
        .for_each_receipt(&mut |mut receipt| {
            let package_hash = receipt.package_hash.to_string();
            let idempotency_token = receipt.idempotency_token.to_string();
            assert!(receipt.receipt_id.as_str().ends_with(&idempotency_token));
            let verify_package_hash = receipt
                .verify
                .parameters
                .remove("package_hash")
                .expect("DuckDB receipt must bind package_hash");
            let verify_idempotency = receipt
                .verify
                .parameters
                .remove("idempotency_token")
                .expect("DuckDB receipt must bind idempotency_token");
            assert_eq!(verify_package_hash, package_hash);
            assert_eq!(verify_idempotency, idempotency_token);
            receipt.verify.parameters.insert(
                "package_hash".to_owned(),
                "<invocation-specific>".to_owned(),
            );
            receipt.verify.parameters.insert(
                "idempotency_token".to_owned(),
                "<invocation-specific>".to_owned(),
            );
            let transaction = receipt.transaction.map(|mut transaction| {
                let database_path = transaction
                    .values
                    .remove("database_path")
                    .expect("DuckDB receipt must record database_path");
                let writer_lock = transaction
                    .values
                    .remove("writer_lock")
                    .expect("DuckDB receipt must record writer_lock");
                let expected_database = project_root.join(".cdf/dev.duckdb");
                assert_eq!(std::path::Path::new(&database_path), expected_database);
                assert_eq!(
                    std::path::Path::new(&writer_lock),
                    expected_database.with_file_name("dev.duckdb.cdf.lock")
                );
                transaction
                    .values
                    .insert("database_path".to_owned(), "<project-database>".to_owned());
                transaction
                    .values
                    .insert("writer_lock".to_owned(), "<project-lock>".to_owned());
                serde_json::json!({
                    "system": transaction.system,
                    "values": transaction.values,
                })
            });
            receipts.push(serde_json::json!({
                "receipt_id": "<invocation-specific>",
                "destination": receipt.destination,
                "target": receipt.target,
                "package_hash": "<invocation-specific>",
                "idempotency_token": "<invocation-specific>",
                "segment_acks": receipt.segment_acks.into_iter().map(|ack| serde_json::json!({
                    "segment_id": ack.segment_id,
                    "row_count": ack.row_count,
                    "byte_count": ack.byte_count,
                })).collect::<Vec<_>>(),
                "disposition": receipt.disposition,
                "transaction": transaction,
                "counts": receipt.counts,
                "schema_hash": receipt.schema_hash,
                "migrations": receipt.migrations,
                "verify_kind": receipt.verify.kind,
                "verify_statement": receipt.verify.statement,
                "verify_parameters": receipt.verify.parameters,
            }));
            Ok(())
        })
        .unwrap();
    serde_json::Value::Array(receipts)
}

fn copy_tree(source: &std::path::Path, target: &std::path::Path, excluded: &[&str]) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if excluded.iter().any(|excluded| name == *excluded) {
            continue;
        }
        let from = entry.path();
        let to = target.join(&name);
        if from.is_dir() {
            copy_tree(&from, &to, excluded);
        } else {
            std::fs::copy(from, to).unwrap();
        }
    }
}

fn execute_cells(
    cells: Vec<RunMatrixCell>,
    environment: &ConformanceEnvironment,
) -> RunMatrixOutput {
    let mut output = RunMatrixOutput::default();
    for cell in cells {
        let cell_id = format!(
            "{}/{}/{}",
            cell.source_archetype,
            cell.destination.as_str(),
            cell.disposition.as_str()
        );
        println!("CDF_RUN_MATRIX_CELL_START={cell_id}");
        if let Some(reason) = core::sheet_exclusion_reason(&cell, environment).unwrap() {
            println!("CDF_RUN_MATRIX_CELL_EXCLUDED={cell_id}");
            output
                .excluded_cells
                .push(ExcludedMatrixCell { cell, reason });
            continue;
        }

        let executed = core::execute_cell(cell.clone(), environment).unwrap_or_else(|error| {
            panic!(
                "run-matrix cell {}/{}/{} failed: {error}",
                cell.source_archetype,
                cell.destination.as_str(),
                cell.disposition.as_str()
            )
        });
        output.executed_cells.push(executed);
        println!("CDF_RUN_MATRIX_CELL_PASS={cell_id}");
    }
    output
}

fn declared_shards() -> Vec<SourceArchetype> {
    serde_json::from_str::<Vec<String>>(RUN_MATRIX_SHARDS_JSON)
        .expect("run-matrix-shards.json must be a string array")
        .into_iter()
        .map(|source| {
            SourceArchetype::new(source).expect("declared run-matrix shard must be valid")
        })
        .collect()
}

fn assert_source_counts(output: &RunMatrixOutput, source: &SourceArchetype) {
    let expected = source_matrix_cells(source.clone()).len();
    assert_eq!(
        core::executed_for_source(&output.executed_cells, source).count()
            + core::excluded_for_source(&output.excluded_cells, source).count(),
        expected
    );
}

fn assert_required_cells(output: &RunMatrixOutput, cells: &[RunMatrixCell]) {
    for cell in cells {
        let executed = output
            .executed_cells
            .iter()
            .any(|executed| &executed.cell == cell);
        let excluded = output.excluded_cells.iter().any(|excluded| {
            &excluded.cell == cell && excluded.reason.contains("supported_dispositions=")
        });
        assert_ne!(
            executed, excluded,
            "cell must execute or be sheet-excluded: {cell:?}"
        );
    }
}
