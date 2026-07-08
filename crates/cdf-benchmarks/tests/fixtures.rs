use cdf_benchmarks::{
    BenchmarkSuite, MetricClass, benchmark_cases, cases_for, coverage_matrix, fixture_spec,
    write_all_local_fixture_formats,
};

#[test]
fn generated_local_fixtures_are_deterministic_from_committed_specs() {
    let spec = fixture_spec("medium").unwrap();
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();

    let first_files = write_all_local_fixture_formats(first.path(), &spec).unwrap();
    let second_files = write_all_local_fixture_formats(second.path(), &spec).unwrap();

    assert_eq!(
        first_files.keys().collect::<Vec<_>>(),
        second_files.keys().collect::<Vec<_>>()
    );
    for (name, first_bytes) in first_files {
        assert_eq!(first_bytes, second_files[&name], "fixture {name} drifted");
    }
}

#[test]
fn benchmark_matrix_declares_runtime_budget_and_metric_classes() {
    let cases = benchmark_cases();
    assert!(cases.iter().any(|case| case.suite == BenchmarkSuite::Smoke));
    assert!(cases.iter().any(|case| case.suite == BenchmarkSuite::Full));
    assert!(
        cases
            .iter()
            .any(|case| case.suite == BenchmarkSuite::Postgres)
    );
    assert_eq!(
        BenchmarkSuite::parse("postgres").unwrap(),
        BenchmarkSuite::Postgres
    );
    assert!(
        cases
            .iter()
            .any(|case| case.metric_class == MetricClass::TrendOnly)
    );
    assert!(
        cases
            .iter()
            .any(|case| case.metric_class == MetricClass::AdHoc)
    );
    assert_eq!(MetricClass::ReleaseGate.as_str(), "release_gate");

    for required in [
        "trend.cdf_file_to_package.csv.medium",
        "trend.cdf_file_to_package.json.medium",
        "trend.cdf_file_to_package.ndjson.medium",
        "trend.cdf_file_to_package.parquet.medium",
        "trend.cdf_package_replay.duckdb_package_receipt_checkpoint.medium",
        "trend.cdf_package_replay.parquet_package_receipt_checkpoint.medium",
        "trend.cdf_package_replay.postgres_package_receipt_checkpoint.medium",
        "trend.cdf_rest_decode.local.medium",
        "trend.cdf_archive.ipc_to_parquet.medium",
        "trend.cdf_startup.file_to_duckdb.tiny",
        "trend.cdf_engine.package_filter_project.wide",
    ] {
        assert!(
            cases.iter().any(|case| case.label == required),
            "missing benchmark case {required}"
        );
    }

    assert!(
        cases_for(BenchmarkSuite::Smoke)
            .iter()
            .all(|case| case.suite == BenchmarkSuite::Smoke)
    );
    assert!(
        cases_for(BenchmarkSuite::Full)
            .iter()
            .all(|case| case.suite != BenchmarkSuite::Postgres)
    );
    assert!(
        cases_for(BenchmarkSuite::Postgres)
            .iter()
            .all(|case| case.suite == BenchmarkSuite::Postgres)
    );
}

#[test]
fn coverage_matrix_records_executable_and_deferred_cells() {
    let coverage = coverage_matrix();
    assert!(coverage.iter().any(|cell| {
        cell.area == "package_replay_postgres" && cell.status == "implemented_opt_in"
    }));
    assert!(coverage.iter().any(|cell| {
        cell.area == "file_to_package_csv_json_ndjson_parquet" && cell.status == "implemented"
    }));
    assert!(
        coverage
            .iter()
            .any(|cell| { cell.area == "native_polars_style" && cell.status == "deferred" })
    );
}
