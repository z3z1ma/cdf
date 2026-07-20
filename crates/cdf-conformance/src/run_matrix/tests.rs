use super::{
    ExcludedMatrixCell, RunMatrixOutput, SourceArchetype, core,
    destinations::ConformanceEnvironment, run_spine_matrix_cells, source_catalog,
    source_matrix_cells,
};

#[test]
fn registered_source_catalog_cells_persist_output() {
    let environment = ConformanceEnvironment::start().expect(
        "C2 run matrix requires Postgres coverage; set TEST_DATABASE_URL or install initdb/pg_ctl",
    );
    let mut output = RunMatrixOutput::default();

    for cell in run_spine_matrix_cells() {
        if let Some(reason) = core::sheet_exclusion_reason(&cell, &environment).unwrap() {
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
    }

    for source in source_catalog::archetypes() {
        assert_source_counts(&output, &source);
        assert_required_cells(&output, &source);
    }

    let serialized = serde_json::to_string_pretty(&output).unwrap();
    assert!(!serialized.contains("run-matrix-token"));
    environment.assert_redacted(&serialized);
    println!("CDF_RUN_MATRIX_OUTPUT={serialized}");
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
