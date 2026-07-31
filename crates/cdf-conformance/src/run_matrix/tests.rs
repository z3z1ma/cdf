use super::{
    ExcludedMatrixCell, RunMatrixOutput, SourceArchetype, core,
    destinations::ConformanceEnvironment, run_spine_matrix_cells, source_catalog,
    source_matrix_cells,
};

const RUN_MATRIX_SOURCE_ENV: &str = "CDF_RUN_MATRIX_SOURCE";
const RUN_MATRIX_SHARDS_JSON: &str = include_str!("../../run-matrix-shards.json");

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
    let mut output = RunMatrixOutput::default();

    for cell in source_matrix_cells(source.clone()) {
        let cell_id = format!(
            "{}/{}/{}",
            cell.source_archetype,
            cell.destination.as_str(),
            cell.disposition.as_str()
        );
        println!("CDF_RUN_MATRIX_CELL_START={cell_id}");
        if let Some(reason) = core::sheet_exclusion_reason(&cell, &environment).unwrap() {
            println!("CDF_RUN_MATRIX_CELL_EXCLUDED={cell_id}");
            output
                .excluded_cells
                .push(ExcludedMatrixCell { cell, reason });
            continue;
        }

        let executed = core::execute_cell(cell.clone(), &environment).unwrap_or_else(|error| {
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

    assert_source_counts(&output, &source);
    assert_required_cells(&output, &source);

    let serialized = serde_json::to_string(&output).unwrap();
    assert!(!serialized.contains("run-matrix-token"));
    environment.assert_redacted(&serialized);
    println!("CDF_RUN_MATRIX_OUTPUT={serialized}");
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

fn assert_required_cells(output: &RunMatrixOutput, source: &SourceArchetype) {
    for cell in source_matrix_cells(source.clone()) {
        let executed = output
            .executed_cells
            .iter()
            .any(|executed| executed.cell == cell);
        let excluded = output.excluded_cells.iter().any(|excluded| {
            excluded.cell == cell && excluded.reason.contains("supported_dispositions=")
        });
        assert_ne!(
            executed, excluded,
            "cell must execute or be sheet-excluded: {cell:?}"
        );
    }
}
