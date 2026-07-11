use super::{
    ExcludedMatrixCell, MatrixDestination, MatrixDisposition, RunMatrixCell, RunMatrixOutput,
    SourceArchetype, core, local_postgres::LivePostgres, run_spine_matrix_cells,
};

#[test]
fn run_matrix_file_python_rest_sql_source_cells_persist_output() {
    let postgres = LivePostgres::start().expect(
        "C2 run matrix requires Postgres coverage; set TEST_DATABASE_URL or install initdb/pg_ctl",
    );
    let mut output = RunMatrixOutput::default();

    for cell in run_spine_matrix_cells() {
        if let Some(reason) = core::sheet_exclusion_reason(&cell) {
            output
                .excluded_cells
                .push(ExcludedMatrixCell { cell, reason });
            continue;
        }

        output
            .executed_cells
            .push(core::execute_cell(cell, &postgres).unwrap());
    }

    assert_source_counts(&output, SourceArchetype::File);
    assert_source_counts(&output, SourceArchetype::Python);
    assert_source_counts(&output, SourceArchetype::Rest);
    assert_source_counts(&output, SourceArchetype::Sql);
    assert_required_cells(&output, SourceArchetype::File);
    assert_required_cells(&output, SourceArchetype::Python);
    assert_required_cells(&output, SourceArchetype::Rest);
    assert_required_cells(&output, SourceArchetype::Sql);

    let serialized = serde_json::to_string_pretty(&output).unwrap();
    assert!(!serialized.contains("run-matrix-token"));
    assert!(!serialized.contains(postgres.url()));
    println!("CDF_RUN_MATRIX_OUTPUT={serialized}");
}

fn assert_source_counts(output: &RunMatrixOutput, source: SourceArchetype) {
    assert_eq!(
        core::executed_for_source(&output.executed_cells, source).count(),
        8
    );
    assert_eq!(
        core::excluded_for_source(&output.excluded_cells, source).count(),
        1
    );
}

fn assert_required_cells(output: &RunMatrixOutput, source: SourceArchetype) {
    for destination in [MatrixDestination::DuckDb, MatrixDestination::Postgres] {
        assert_executed(
            output,
            RunMatrixCell::new(source, destination, MatrixDisposition::Append),
        );
        assert_executed(
            output,
            RunMatrixCell::new(source, destination, MatrixDisposition::Replace),
        );
        assert_executed(
            output,
            RunMatrixCell::new(source, destination, MatrixDisposition::Merge),
        );
    }

    assert_executed(
        output,
        RunMatrixCell::new(
            source,
            MatrixDestination::ParquetFilesystem,
            MatrixDisposition::Append,
        ),
    );
    assert_executed(
        output,
        RunMatrixCell::new(
            source,
            MatrixDestination::ParquetFilesystem,
            MatrixDisposition::Replace,
        ),
    );
    assert_excluded(
        output,
        RunMatrixCell::new(
            source,
            MatrixDestination::ParquetFilesystem,
            MatrixDisposition::Merge,
        ),
    );
}

fn assert_executed(output: &RunMatrixOutput, cell: RunMatrixCell) {
    assert!(
        output
            .executed_cells
            .iter()
            .any(|executed| executed.cell == cell)
    );
}

fn assert_excluded(output: &RunMatrixOutput, cell: RunMatrixCell) {
    assert!(output.excluded_cells.iter().any(|excluded| {
        excluded.cell == cell
            && excluded
                .reason
                .contains("supported_dispositions=[append, replace]")
    }));
}
