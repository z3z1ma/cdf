use cdf_benchmarks::{
    BENCHMARK_REPORT_SCHEMA_VERSION, BenchmarkSuite, Capability, DatasetRecipe, GeneratorDelivery,
    IoMode, MetricClass, ObservationStatus, benchmark_cases, canonical_json_bytes,
    canonical_sha256, cases_for, coverage_matrix, dataset_catalog, fixture_spec,
    import_incomparable_trend, report_fixture, validate_dataset_catalog, validate_report,
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
    assert!(
        first_files
            .values()
            .all(|bytes| bytes.len() <= 8 * 1024 * 1024)
    );
    for (name, first_bytes) in first_files {
        assert_eq!(first_bytes, second_files[&name], "fixture {name} drifted");
    }

    let tiny = fixture_spec("tiny").unwrap();
    let tiny_root = tempfile::tempdir().unwrap();
    let tiny_files = write_all_local_fixture_formats(tiny_root.path(), &tiny).unwrap();
    assert!(tiny_files.values().all(|bytes| bytes.len() <= 1024 * 1024));
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

#[test]
fn p3_dataset_catalog_is_regeneration_grade_and_bounded() {
    let catalog = dataset_catalog().unwrap();
    for required in [
        "control_tiny_startup",
        "control_medium_throughput",
        "nyc_tlc_yellow_2024",
        "tpch_sf10",
        "tpch_sf100",
        "json_wide_10g",
        "json_nested_10g",
        "json_dirty_10g",
        "json_schema_varying_10g",
        "constant_memory_100g",
    ] {
        assert!(
            catalog
                .datasets
                .iter()
                .any(|dataset| dataset.id == required)
        );
    }
    assert!(catalog.datasets.iter().all(|dataset| {
        !dataset.schema_ref.is_empty()
            && !dataset.provenance.version.is_empty()
            && !dataset.provenance.license.is_empty()
    }));
    assert!(catalog.datasets.iter().any(|dataset| {
        matches!(
            dataset.recipe,
            DatasetRecipe::SyntheticStream {
                delivery: GeneratorDelivery::Streaming,
                logical_bytes: 107_374_182_400,
                chunk_bytes: 8_388_608,
                ..
            }
        )
    }));
    assert!(catalog.datasets.iter().any(|dataset| {
        matches!(
            dataset.recipe,
            DatasetRecipe::BenchmarkFixture {
                fixture_catalog_version: 1,
                ref fixture_name,
                rows: 8,
                batch_rows: 8,
                max_generated_bytes: 1_048_576,
                ..
            } if fixture_name == "tiny"
        )
    }));
    for required in ["control_tiny_startup_e2e", "control_medium_ndjson_package"] {
        assert!(
            catalog
                .workloads
                .iter()
                .any(|workload| workload.id == required)
        );
    }
    assert!(catalog.workloads.iter().all(|workload| {
        !workload.timed_region.includes.is_empty()
            && !workload.logical_byte_counter.method.is_empty()
            && !workload.physical_byte_counter.method.is_empty()
    }));

    let canonical = canonical_json_bytes(&catalog).unwrap();
    assert!(canonical.starts_with(b"{\"datasets\":"));
    assert_eq!(
        canonical,
        canonical_json_bytes(&dataset_catalog().unwrap()).unwrap()
    );
    assert_eq!(
        canonical_sha256(&catalog).unwrap(),
        "sha256:1972d2ce08d16a987413a6325bf4bf216aac5b5d3072d552e61ff195fa3b58c6"
    );
}

#[test]
fn p3_report_fixture_is_deterministic_sanitized_and_explicit() {
    let report = report_fixture().unwrap();
    assert_eq!(report.schema_version, BENCHMARK_REPORT_SCHEMA_VERSION);
    assert!(!report.host.cpu_label.contains('/'));
    assert!(matches!(
        report.host.effective_cpu,
        Capability::Supported { .. }
    ));
    assert!(matches!(
        report.host.effective_memory_bytes,
        Capability::Unavailable { .. }
    ));
    assert!(report.observations.iter().any(|observation| {
        matches!(observation.status, ObservationStatus::Observed)
            && observation.summary.as_ref().unwrap().sample_count == 3
            && observation.comparability.io_mode == IoMode::Warm
    }));
    assert!(report.observations.iter().any(|observation| {
        matches!(observation.status, ObservationStatus::Unavailable { .. })
            && observation.samples.is_empty()
            && observation.summary.is_none()
    }));
    assert_eq!(
        canonical_json_bytes(&report).unwrap(),
        canonical_json_bytes(&report_fixture().unwrap()).unwrap()
    );
    assert_eq!(
        canonical_sha256(&report).unwrap(),
        "sha256:3f2516d08d09f2a59cade019ab85d6648ba4ecc85828ecc490f0de206107bda1"
    );
}

#[test]
fn p3_catalog_and_report_fail_closed_when_incomparable_or_malformed() {
    let mut catalog = dataset_catalog().unwrap();
    catalog.workloads[0].dataset_id = "missing".to_owned();
    assert!(validate_dataset_catalog(&catalog).is_err());

    let mut report = report_fixture().unwrap();
    report.observations[0]
        .summary
        .as_mut()
        .unwrap()
        .sample_count = 99;
    assert!(validate_report(&report).is_err());

    let mut report = report_fixture().unwrap();
    report.host.cpu_label = "/Users/alex/private-host".to_owned();
    assert!(validate_report(&report).is_err());
}

#[test]
fn imported_trends_load_only_as_incomparable_imports() {
    let trend = br#"{"observed_at_ms":1,"suite":"smoke","label":"trend.old","metric_class":"trend_only","elapsed_ns":10,"rows":1,"bytes":8}"#;
    let imported = import_incomparable_trend(trend).unwrap();
    assert_eq!(imported.record.label, "trend.old");
    assert!(matches!(
        imported.status,
        ObservationStatus::Inconclusive { .. }
    ));
}
