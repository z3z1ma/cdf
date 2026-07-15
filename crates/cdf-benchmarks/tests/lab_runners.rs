use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cdf_benchmarks::{
    BENCHMARK_REPORT_SCHEMA_VERSION, BenchmarkReport, BiasLabel, Capability, ChildCommand,
    ChildObservationStatus, ComparabilityKey, ExternalFileFormat, HostCapabilityProvider,
    HostProbeConfig, IoMode, MacroRunRequest, ObservationStatus, PreoptimizationBaselineConfig,
    PreparedDestinationKind, PreparedFileDestinationWorkload, PreparedFileFormat,
    PreparedFilePackageWorkload, ProfileTool, ReferenceIdentity, ReferenceWorkload,
    SystemHostProvider, ToolIdentity, discover_polars, fixture_spec, host_class, plan_profile,
    polars_scan_command, run_macro_cell, run_preoptimization_baseline,
    run_prepared_file_to_destination, run_prepared_file_to_package, run_reference,
    unavailable_reference_cell, validate_report, write_all_local_fixture_formats,
};

fn provider() -> SystemHostProvider {
    SystemHostProvider::new(HostProbeConfig {
        cdf_version: env!("CARGO_PKG_VERSION").to_owned(),
        dependency_versions: BTreeMap::from([
            ("arrow".to_owned(), "59.1.0".to_owned()),
            ("duckdb".to_owned(), "1.10504.0".to_owned()),
        ]),
        benchmark_profile: "test".to_owned(),
        storage_target: std::env::current_dir().ok(),
    })
}

fn command(args: &[&str]) -> ChildCommand {
    ChildCommand {
        program: PathBuf::from(env!("CARGO_BIN_EXE_cdf-p3-lab")),
        args: args.iter().map(|value| (*value).to_owned()).collect(),
        environment: BTreeMap::new(),
        current_dir: None,
    }
}

fn key(provider: &dyn HostCapabilityProvider, mode: IoMode) -> ComparabilityKey {
    ComparabilityKey {
        dataset_id: "fixture-medium".to_owned(),
        workload_id: "sequential-read".to_owned(),
        timed_region_version: 1,
        cdf_revision: "fixture-revision".to_owned(),
        dependency_tuple: "arrow59-duckdb1".to_owned(),
        host_class: host_class(&provider.fingerprint().unwrap()).unwrap(),
        os_toolchain: "fixture-rust1.96".to_owned(),
        io_mode: mode,
    }
}

#[test]
fn isolated_macro_runner_retains_samples_and_derives_distribution() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input.bin");
    fs::write(&input, vec![0x5A; 512 * 1024]).unwrap();
    let request_path = temp.path().join("request.json");
    fs::write(
        &request_path,
        serde_json::to_vec(&ReferenceWorkload::SequentialRead {
            path: input,
            buffer_bytes: 64 * 1024,
        })
        .unwrap(),
    )
    .unwrap();
    let provider = provider();
    let comparability = key(&provider, IoMode::Warm);
    let observation = run_macro_cell(
        &provider,
        &MacroRunRequest {
            expected_host_class: Some(comparability.host_class.clone()),
            comparability,
            sample_count: 3,
            timeout: Duration::from_secs(5),
            allow_privileged_cache_control: false,
            command: command(&["reference-worker", request_path.to_str().unwrap()]),
            reference: Some(ReferenceIdentity {
                kind: "internal".to_owned(),
                name: "sequential-read".to_owned(),
                version: "v1".to_owned(),
                semantic_work: "read every input byte".to_owned(),
            }),
            bias: vec![BiasLabel {
                code: "omits_cdf_evidence".to_owned(),
                description: "reference omits validation package receipt and checkpoint work"
                    .to_owned(),
            }],
        },
    )
    .unwrap();

    assert!(
        matches!(observation.status, ObservationStatus::Observed),
        "reference worker observation was not recorded: {observation:#?}"
    );
    assert_eq!(observation.samples.len(), 3);
    assert!(observation.samples.iter().all(|sample| {
        sample.wall_time_ns > 0
            && sample.logical_bytes == 512 * 1024
            && sample.physical_bytes == 512 * 1024
    }));
    if cfg!(any(target_os = "macos", target_os = "linux"))
        && PathBuf::from("/usr/bin/time").is_file()
    {
        assert!(
            observation
                .samples
                .iter()
                .all(|sample| sample.cpu_time_ns.is_some() && sample.peak_rss_bytes.is_some())
        );
    }
    assert_eq!(observation.summary.as_ref().unwrap().sample_count, 3);
    let report = BenchmarkReport {
        schema_version: BENCHMARK_REPORT_SCHEMA_VERSION,
        host: provider.fingerprint().unwrap(),
        observations: vec![observation],
    };
    validate_report(&report).unwrap();
}

#[test]
fn timeout_cold_cache_and_host_change_remain_visible_cells() {
    let provider = provider();
    let warm_key = key(&provider, IoMode::Warm);
    let timed_out = run_macro_cell(
        &provider,
        &MacroRunRequest {
            comparability: warm_key.clone(),
            expected_host_class: Some(warm_key.host_class.clone()),
            sample_count: 1,
            timeout: Duration::from_millis(10),
            allow_privileged_cache_control: false,
            command: command(&["sleep-worker", "1000"]),
            reference: None,
            bias: Vec::new(),
        },
    )
    .unwrap();
    assert!(matches!(
        timed_out.status,
        ObservationStatus::TimedOut { .. }
    ));

    let cold = run_macro_cell(
        &provider,
        &MacroRunRequest {
            comparability: key(&provider, IoMode::Cold),
            expected_host_class: None,
            sample_count: 1,
            timeout: Duration::from_secs(1),
            allow_privileged_cache_control: false,
            command: command(&["sleep-worker", "0"]),
            reference: None,
            bias: Vec::new(),
        },
    )
    .unwrap();
    assert!(matches!(cold.status, ObservationStatus::Unavailable { .. }));

    let changed = run_macro_cell(
        &provider,
        &MacroRunRequest {
            comparability: ComparabilityKey {
                host_class: "different-host-class".to_owned(),
                ..warm_key
            },
            expected_host_class: None,
            sample_count: 1,
            timeout: Duration::from_secs(1),
            allow_privileged_cache_control: false,
            command: command(&["sleep-worker", "0"]),
            reference: None,
            bias: Vec::new(),
        },
    )
    .unwrap();
    assert!(matches!(
        changed.status,
        ObservationStatus::Inconclusive { .. }
    ));

    let mut credential_command = command(&["sleep-worker", "0"]);
    credential_command.environment.insert(
        "DATABASE_PASSWORD".to_owned(),
        "must-not-enter-report-input".to_owned(),
    );
    let error = run_macro_cell(
        &provider,
        &MacroRunRequest {
            comparability: key(&provider, IoMode::Uncontrolled),
            expected_host_class: None,
            sample_count: 1,
            timeout: Duration::from_secs(1),
            allow_privileged_cache_control: false,
            command: credential_command,
            reference: None,
            bias: Vec::new(),
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("cannot embed credential"));
}

#[test]
fn raw_arrow_duckdb_and_io_references_cross_check_fixture_rows_and_bytes() {
    let temp = tempfile::tempdir().unwrap();
    let spec = fixture_spec("medium").unwrap();
    write_all_local_fixture_formats(temp.path(), &spec).unwrap();
    for workload in [
        ReferenceWorkload::ArrowParquet {
            path: temp.path().join("orders.parquet"),
            batch_rows: 1024,
        },
        ReferenceWorkload::ArrowCsv {
            path: temp.path().join("orders.csv"),
            batch_rows: 1024,
            has_header: true,
        },
        ReferenceWorkload::ArrowNdjson {
            path: temp.path().join("orders.ndjson"),
            batch_rows: 1024,
            infer_rows: spec.rows,
        },
        ReferenceWorkload::DuckDbParquet {
            path: temp.path().join("orders.parquet"),
        },
    ] {
        let measured = run_reference(&workload).unwrap();
        assert_eq!(measured.rows, spec.rows as u64);
        assert!(measured.logical_bytes > 0);
        assert!(measured.physical_bytes > 0);
    }

    let write_path = temp.path().join("sequential.bin");
    let written = run_reference(&ReferenceWorkload::SequentialWrite {
        path: write_path.clone(),
        logical_bytes: 2 * 1024 * 1024,
        buffer_bytes: 64 * 1024,
        sync: false,
    })
    .unwrap();
    let read = run_reference(&ReferenceWorkload::SequentialRead {
        path: write_path,
        buffer_bytes: 64 * 1024,
    })
    .unwrap();
    let copied = run_reference(&ReferenceWorkload::Memcpy {
        logical_bytes: 2 * 1024 * 1024,
        buffer_bytes: 64 * 1024,
    })
    .unwrap();
    assert_eq!(written.physical_bytes, 2 * 1024 * 1024);
    assert_eq!(read.logical_bytes, written.logical_bytes);
    assert_eq!(copied.logical_bytes, written.logical_bytes);
}

#[test]
fn prepared_cdf_worker_emits_real_phase_breakdown_without_timing_fixture_setup() {
    let temp = tempfile::tempdir().unwrap();
    let spec = fixture_spec("medium").unwrap();
    write_all_local_fixture_formats(temp.path(), &spec).unwrap();
    let request_path = temp.path().join("cdf-worker.json");
    fs::write(
        &request_path,
        serde_json::to_vec(&PreparedFilePackageWorkload {
            fixture_name: "medium".to_owned(),
            source_root: temp.path().to_path_buf(),
            glob: "orders.ndjson".to_owned(),
            package_dir: temp.path().join("packages"),
            format: PreparedFileFormat::Ndjson,
            jobs: None,
            execution_host_jobs: 4,
        })
        .unwrap(),
    )
    .unwrap();
    let provider = provider();
    let observation = run_macro_cell(
        &provider,
        &MacroRunRequest {
            comparability: key(&provider, IoMode::Warm),
            expected_host_class: None,
            sample_count: 3,
            timeout: Duration::from_secs(15),
            allow_privileged_cache_control: false,
            command: command(&["cdf-file-package-worker", request_path.to_str().unwrap()]),
            reference: None,
            bias: vec![BiasLabel {
                code: "includes_cdf_evidence".to_owned(),
                description: "includes validation package hashing and finalization".to_owned(),
            }],
        },
    )
    .unwrap();
    assert!(
        matches!(observation.status, ObservationStatus::Observed),
        "prepared worker observation was not recorded: {observation:#?}"
    );
    assert_eq!(observation.samples.len(), 3);
    assert!(observation.samples.iter().all(|sample| {
        sample.rows == spec.rows as u64
            && sample.physical_bytes > 0
            && sample.phases.iter().any(|phase| phase.phase == "decode")
            && sample
                .phases
                .iter()
                .any(|phase| phase.phase == "package_finalize")
    }));
}

#[test]
fn prepared_multi_file_jobs_matrix_preserves_canonical_package_identity() {
    let temp = tempfile::tempdir().unwrap();
    let spec = fixture_spec("medium").unwrap();
    write_all_local_fixture_formats(temp.path(), &spec).unwrap();
    for (format_label, format, extension) in [
        ("csv", PreparedFileFormat::Csv, "csv"),
        ("json", PreparedFileFormat::Json, "json"),
        ("ndjson", PreparedFileFormat::Ndjson, "ndjson"),
        ("parquet", PreparedFileFormat::Parquet, "parquet"),
    ] {
        let source = fs::read(temp.path().join(format!("orders.{extension}"))).unwrap();
        for ordinal in 0..4 {
            fs::write(
                temp.path()
                    .join(format!("{format_label}-part-{ordinal:02}.{extension}")),
                &source,
            )
            .unwrap();
        }

        let mut runs = Vec::new();
        for (jobs_label, jobs) in [
            ("one", Some(1)),
            ("two", Some(2)),
            ("auto", None),
            ("n", Some(4)),
        ] {
            let run = run_prepared_file_to_package(&PreparedFilePackageWorkload {
                fixture_name: "medium".to_owned(),
                source_root: temp.path().to_path_buf(),
                glob: format!("{format_label}-part-*.{extension}"),
                package_dir: temp
                    .path()
                    .join(format!("package-{format_label}-{jobs_label}")),
                format,
                jobs,
                execution_host_jobs: 4,
            })
            .unwrap();
            assert_eq!(run.configured_jobs, jobs);
            assert_eq!(run.partition_count, 4);
            assert_eq!(run.measurement.rows, (spec.rows * 4) as u64);
            runs.push(run);
        }

        assert_eq!(runs[0].effective_jobs, 1);
        assert_eq!(runs[1].effective_jobs, 2);
        assert_eq!(runs[2].effective_jobs, 4);
        assert_eq!(runs[3].effective_jobs, 4);
        for run in &runs[1..] {
            assert_eq!(
                run.package_hash, runs[0].package_hash,
                "{format_label} package identity changed with jobs"
            );
            assert_eq!(
                run.segments, runs[0].segments,
                "{format_label} canonical segments changed with jobs"
            );
        }
    }
}

#[test]
fn prepared_jobs_zero_is_rejected_before_source_contact() {
    let error = run_prepared_file_to_package(&PreparedFilePackageWorkload {
        fixture_name: "medium".to_owned(),
        source_root: PathBuf::from("does-not-exist"),
        glob: "*.ndjson".to_owned(),
        package_dir: PathBuf::from("must-not-be-created"),
        format: PreparedFileFormat::Ndjson,
        jobs: Some(0),
        execution_host_jobs: 4,
    })
    .unwrap_err();

    assert!(
        error.to_string().contains("jobs must be nonzero"),
        "{error}"
    );
    assert!(!PathBuf::from("must-not-be-created").exists());
}

#[test]
fn destination_ingress_categories_preserve_jobs_identity() {
    let temp = tempfile::tempdir().unwrap();
    let spec = fixture_spec("medium").unwrap();
    write_all_local_fixture_formats(temp.path(), &spec).unwrap();
    let source = fs::read(temp.path().join("orders.parquet")).unwrap();
    for ordinal in 0..4 {
        fs::write(
            temp.path().join(format!("part-{ordinal:02}.parquet")),
            &source,
        )
        .unwrap();
    }

    for (label, destination) in [
        ("duckdb", PreparedDestinationKind::DuckDb),
        ("parquet", PreparedDestinationKind::Parquet),
    ] {
        let mut runs = Vec::new();
        for (jobs_label, jobs) in [
            ("one", Some(1)),
            ("two", Some(2)),
            ("auto", None),
            ("four", Some(4)),
        ] {
            runs.push(
                run_prepared_file_to_destination(&PreparedFileDestinationWorkload {
                    fixture_name: "medium".to_owned(),
                    source_root: temp.path().to_path_buf(),
                    glob: "part-*.parquet".to_owned(),
                    format: PreparedFileFormat::Parquet,
                    output_root: temp
                        .path()
                        .join(format!("destination-{label}-{jobs_label}")),
                    destination: destination.clone(),
                    jobs,
                    execution_host_jobs: 4,
                })
                .unwrap(),
            );
        }

        assert_eq!(runs[0].effective_jobs, 1);
        assert_eq!(runs[1].effective_jobs, 2);
        assert_eq!(runs[2].effective_jobs, 4);
        assert_eq!(runs[3].effective_jobs, 4);
        for run in &runs {
            assert_eq!(run.partition_count, 4);
            assert_eq!(run.row_count, (spec.rows * 4) as u64);
        }
        for run in &runs[1..] {
            assert_eq!(run.package_hash, runs[0].package_hash, "{label}");
            assert_eq!(
                run.receipt_package_hash, runs[0].receipt_package_hash,
                "{label}"
            );
            assert_eq!(
                run.receipt_segment_ids, runs[0].receipt_segment_ids,
                "{label}"
            );
            assert_eq!(run.state_segment_ids, runs[0].state_segment_ids, "{label}");
            assert_eq!(run.logical_receipt, runs[0].logical_receipt, "{label}");
            assert_eq!(
                run.logical_manifest_sha256, runs[0].logical_manifest_sha256,
                "{label}"
            );
            assert_eq!(run.logical_manifest, runs[0].logical_manifest, "{label}");
        }
    }
}

#[test]
#[ignore = "requires CDF_BENCH_POSTGRES_URL for the live PostgreSQL destination matrix"]
fn postgres_destination_preserves_jobs_identity() {
    let database_url = std::env::var("CDF_BENCH_POSTGRES_URL")
        .expect("CDF_BENCH_POSTGRES_URL must name the live benchmark database");
    let temp = tempfile::tempdir().unwrap();
    let spec = fixture_spec("medium").unwrap();
    write_all_local_fixture_formats(temp.path(), &spec).unwrap();
    let source = fs::read(temp.path().join("orders.parquet")).unwrap();
    for ordinal in 0..4 {
        fs::write(
            temp.path().join(format!("part-{ordinal:02}.parquet")),
            &source,
        )
        .unwrap();
    }
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("cdf_c4_jobs_{}_{}_orders", std::process::id(), unique);
    let mut runs = Vec::new();
    for (jobs_label, jobs) in [
        ("one", Some(1)),
        ("two", Some(2)),
        ("auto", None),
        ("four", Some(4)),
    ] {
        runs.push(
            run_prepared_file_to_destination(&PreparedFileDestinationWorkload {
                fixture_name: "medium".to_owned(),
                source_root: temp.path().to_path_buf(),
                glob: "part-*.parquet".to_owned(),
                format: PreparedFileFormat::Parquet,
                output_root: temp.path().join(format!("postgres-{jobs_label}")),
                destination: PreparedDestinationKind::Postgres {
                    database_url: database_url.clone(),
                    schema: None,
                    table: table.clone(),
                },
                jobs,
                execution_host_jobs: 4,
            })
            .unwrap(),
        );
        let mut client = postgres::Client::connect(&database_url, postgres::NoTls).unwrap();
        client
            .batch_execute(&format!(
                "DROP TABLE IF EXISTS \"{table}\", \"{}\", \"{}\", \"{}\", \"{}\", \"{}\" CASCADE",
                cdf_dest_postgres::CDF_LOADS_TABLE,
                cdf_dest_postgres::CDF_STATE_TABLE,
                cdf_dest_postgres::CDF_QUARANTINE_TABLE,
                cdf_dest_postgres::CDF_ROW_KEY_ALLOCATOR_TABLE,
                cdf_dest_postgres::CDF_SEGMENTS_TABLE,
            ))
            .unwrap();
    }

    assert_eq!(runs[0].effective_jobs, 1);
    assert_eq!(runs[1].effective_jobs, 2);
    assert_eq!(runs[2].effective_jobs, 4);
    assert_eq!(runs[3].effective_jobs, 4);
    for run in &runs {
        assert_eq!(run.partition_count, 4);
        assert_eq!(run.row_count, (spec.rows * 4) as u64);
    }
    for run in &runs[1..] {
        assert_eq!(run.package_hash, runs[0].package_hash);
        assert_eq!(run.receipt_package_hash, runs[0].receipt_package_hash);
        assert_eq!(run.receipt_segment_ids, runs[0].receipt_segment_ids);
        assert_eq!(run.state_segment_ids, runs[0].state_segment_ids);
    }
}

#[test]
fn preoptimization_baseline_covers_every_target_and_retains_phases() {
    let temp = tempfile::tempdir().unwrap();
    let provider = provider();
    let report = run_preoptimization_baseline(
        &provider,
        &PreoptimizationBaselineConfig {
            worker_executable: PathBuf::from(env!("CARGO_BIN_EXE_cdf-p3-lab")),
            output_root: temp.path().join("baseline"),
            cdf_revision: "fixture-revision".to_owned(),
            dependency_tuple: "arrow59-duckdb1".to_owned(),
            os_toolchain: "fixture-rust1.96".to_owned(),
            sample_count: 3,
            timeout: Duration::from_secs(30),
        },
    )
    .unwrap();
    validate_report(&report).unwrap();
    for workload in [
        "tlc_parquet_to_package",
        "tpch_csv_to_package",
        "json_ndjson_to_package",
        "validation_kernel",
        "package_build",
        "duckdb_commit",
        "postgres_commit",
        "parquet_destination",
        "tlc_e2e_duckdb",
        "constant_memory_stress",
    ] {
        assert!(
            report
                .observations
                .iter()
                .any(|observation| observation.comparability.workload_id == workload),
            "missing {workload}"
        );
    }
    let cdf = report
        .observations
        .iter()
        .find(|observation| observation.comparability.workload_id == "json_ndjson_to_package")
        .unwrap();
    assert!(matches!(cdf.status, ObservationStatus::Observed));
    assert_eq!(cdf.samples.len(), 3);
    assert!(cdf.samples.iter().all(|sample| {
        sample.peak_rss_bytes.is_some()
            && sample.phases.iter().any(|phase| phase.phase == "decode")
            && sample
                .phases
                .iter()
                .any(|phase| phase.phase == "persist_hash")
    }));
    assert!(report.observations.iter().any(|observation| {
        observation.comparability.workload_id == "legacy_tiny_startup_e2e"
            && matches!(observation.status, ObservationStatus::Observed)
    }));
}

struct ProfileProvider {
    host: cdf_benchmarks::HostFingerprint,
}

impl HostCapabilityProvider for ProfileProvider {
    fn fingerprint(&self) -> cdf_benchmarks::BenchResult<cdf_benchmarks::HostFingerprint> {
        Ok(self.host.clone())
    }

    fn prepare_io_mode(
        &self,
        _mode: IoMode,
        _allow_privileged: bool,
    ) -> Capability<cdf_benchmarks::CachePreparation> {
        unreachable!()
    }

    fn observe_child(
        &self,
        _command: &ChildCommand,
        _timeout: Duration,
    ) -> cdf_benchmarks::BenchResult<ChildObservationStatus> {
        unreachable!()
    }

    fn discover_tool(&self, name: &str) -> Capability<ToolIdentity> {
        Capability::Supported {
            value: ToolIdentity {
                name: name.to_owned(),
                version: "fixture-version".to_owned(),
                executable: name.to_owned(),
            },
            method: "fixture".to_owned(),
            provider_version: "fixture-v1".to_owned(),
        }
    }

    fn process_observer_identity(&self) -> cdf_benchmarks::MeasurementProviderIdentity {
        cdf_benchmarks::MeasurementProviderIdentity {
            method: "fixture".to_owned(),
            version: "fixture-v1".to_owned(),
            observes_cpu_time: false,
            observes_peak_rss: false,
        }
    }
}

#[test]
fn profiling_dry_run_records_exact_tool_command_and_ignored_artifact_path() {
    let provider = ProfileProvider {
        host: provider().fingerprint().unwrap(),
    };
    let root = tempfile::tempdir().unwrap();
    let planned = plan_profile(
        &provider,
        ProfileTool::PerfStat,
        &command(&["host"]),
        root.path(),
        "fixture-perf",
    )
    .unwrap();
    let Capability::Supported { value, .. } = planned else {
        panic!("fixture tool should be supported");
    };
    assert_eq!(value.tool_identity.version, "fixture-version");
    assert!(value.command.iter().any(|part| part == "stat"));
    assert!(
        value
            .artifact
            .ends_with("target/cdf-benchmarks/profiles/fixture-perf.txt")
    );
}

#[test]
fn unavailable_external_reference_is_reported_instead_of_omitted() {
    let provider = provider();
    let observation = unavailable_reference_cell(
        key(&provider, IoMode::Uncontrolled),
        ReferenceIdentity {
            kind: "external".to_owned(),
            name: "polars".to_owned(),
            version: "unavailable".to_owned(),
            semantic_work: "scan only".to_owned(),
        },
        vec![BiasLabel {
            code: "omits_cdf_evidence".to_owned(),
            description: "reference omits package receipt and checkpoint work".to_owned(),
        }],
        Capability::Unavailable {
            reason: "Polars executable is not installed".to_owned(),
            method: "fixture".to_owned(),
            provider_version: "fixture-v1".to_owned(),
        },
    )
    .unwrap();
    assert!(matches!(
        observation.status,
        ObservationStatus::Unavailable { .. }
    ));
    assert!(observation.samples.is_empty());
    assert!(observation.reference.is_some());
}

#[test]
fn polars_stays_external_and_is_either_runnable_or_typed_unavailable() {
    let provider = provider();
    match discover_polars(&provider).unwrap() {
        Capability::Supported { value, .. } => {
            let command = polars_scan_command(
                &value,
                PathBuf::from("fixture.parquet"),
                ExternalFileFormat::Parquet,
            );
            assert_eq!(command.program, PathBuf::from(value.executable));
            assert!(command.args.iter().any(|argument| argument == "parquet"));
        }
        Capability::Unavailable { reason, .. } => {
            assert!(reason.contains("Polars") || reason.contains("python3"));
        }
        Capability::Failed { error, .. } => panic!("unexpected Polars probe failure: {error}"),
    }
}
