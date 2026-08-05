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
fn mongodb_current_query_project_runtime_drift_fails_closed() {
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

    let error = futures_executor::block_on(cdf_engine::preview_resource(
        &plan,
        source.queryable(),
        cdf_engine::EnginePreviewLimits::default(),
    ))
    .unwrap_err();

    assert_eq!(error.kind, cdf_kernel::ErrorKind::Data);
    assert!(error.message.contains("produced physical schema hash"));
    assert!(
        error
            .message
            .contains("verified discovery evidence requires")
    );
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
