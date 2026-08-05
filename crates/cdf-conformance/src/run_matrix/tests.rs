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
    location.set_username("reader@ops").unwrap();
    location
        .set_password(Some("private:mongo-password"))
        .unwrap();
    location.set_path(&format!("/cdf_conformance/{collection}"));
    let location = location.to_string();
    let secret = "private:mongo-password";

    let dry = invoke_public_cli(
        &project_root,
        false,
        &[
            "add",
            "warehouse.events",
            &location,
            "--option",
            "cursor=updated_at",
            "--dry-run",
        ],
    );
    assert_eq!(dry.exit_code, 0, "{}", dry.stderr);
    assert_invocation_redacted(&dry, secret);
    let add = invoke_public_cli(
        &project_root,
        true,
        &[
            "add",
            "warehouse.events",
            &location,
            "--option",
            "cursor=updated_at",
        ],
    );
    assert_eq!(add.exit_code, 0, "{}", add.stderr);
    assert_invocation_redacted(&add, secret);
    assert!(
        walk_files(&project_root.join(".cdf/secrets"))
            .iter()
            .any(|path| std::fs::read_to_string(path).unwrap() == secret)
    );

    // The local integration server intentionally has authentication disabled. Preserve the
    // private-file publication proof while removing credentials from the live connection config.
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
            let line = line.trim_start();
            !removing_local && !line.starts_with("username =") && !line.starts_with("password =")
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
        vec!["preview", "warehouse.events"],
        vec!["doctor"],
    ] {
        let result = invoke_public_cli(&project_root, true, &command);
        assert_eq!(
            result.exit_code, 0,
            "command {command:?} failed: {}",
            result.stderr
        );
        assert_invocation_redacted(&result, secret);
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
    assert_invocation_redacted(&first, secret);
    assert_invocation_redacted(&second, secret);
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
        package_segment_fingerprint(&package),
        package_segment_fingerprint(&second_package)
    );
    std::fs::remove_file(jobs_one.join(".cdf/state.db")).unwrap();
    std::fs::remove_file(jobs_one.join(".cdf/dev.duckdb")).unwrap();
    let replay = invoke_public_cli(
        &jobs_one,
        true,
        &["replay", "package", package.to_str().unwrap()],
    );
    assert_eq!(replay.exit_code, 0, "{}", replay.stderr);
    assert_invocation_redacted(&replay, secret);
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
    assert!(!result.stdout.contains(secret));
    assert!(!result.stderr.contains(secret));
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

fn package_segment_fingerprint(package: &std::path::Path) -> Vec<(u64, String)> {
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(package.join("manifest.json")).unwrap()).unwrap();
    manifest["identity"]["segments"]
        .as_array()
        .unwrap()
        .iter()
        .map(|segment| {
            (
                segment["row_count"].as_u64().unwrap(),
                segment["sha256"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
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
