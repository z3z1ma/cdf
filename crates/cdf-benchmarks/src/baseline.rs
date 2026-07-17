use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{
    BENCHMARK_REPORT_SCHEMA_VERSION, BenchmarkObservation, BenchmarkReport, BiasLabel,
    ChildCommand, ComparabilityKey, HostCapabilityProvider, IoMode, LegacyCaseWorkload,
    MacroRunRequest, MeasurementProviderIdentity, ObservationStatus, PreparedDestinationKind,
    PreparedFileDestinationWorkload, PreparedFileFormat, PreparedFilePackageWorkload,
    ReferenceIdentity, ReferenceWorkload, bench_error, canonical_json_bytes, host_class,
    run_macro_cell, validate_report, write_all_local_fixture_formats,
};

#[derive(Clone, Debug)]
pub struct PreoptimizationBaselineConfig {
    pub worker_executable: PathBuf,
    pub output_root: PathBuf,
    pub cdf_revision: String,
    pub dependency_tuple: String,
    pub os_toolchain: String,
    pub sample_count: u32,
    pub timeout: Duration,
}

pub fn run_preoptimization_baseline(
    provider: &dyn HostCapabilityProvider,
    config: &PreoptimizationBaselineConfig,
) -> crate::BenchResult<BenchmarkReport> {
    if config.sample_count < 3 || config.timeout.is_zero() {
        return Err(bench_error(
            "preoptimization baseline requires at least three samples and a positive timeout",
        ));
    }
    fs::create_dir_all(&config.output_root)?;
    let fixture_root = config.output_root.join("fixtures");
    let request_root = config.output_root.join("requests");
    fs::create_dir_all(&request_root)?;
    let medium = crate::fixture_spec("medium")?;
    write_all_local_fixture_formats(&fixture_root, &medium)?;
    let host = provider.fingerprint()?;
    let host_class = host_class(&host)?;
    let measurement_provider = Some(provider.process_observer_identity());
    let mut observations = Vec::new();

    let raw_request = request_root.join("raw-ndjson.json");
    fs::write(
        &raw_request,
        canonical_json_bytes(&ReferenceWorkload::ArrowNdjson {
            path: fixture_root.join("orders.ndjson"),
            batch_rows: medium.batch_size,
            infer_rows: medium.rows,
        })?,
    )?;
    observations.push(run_cell(
        provider,
        config,
        key(
            config,
            &host_class,
            "legacy_medium_throughput",
            "raw_arrow_ndjson",
        ),
        command(&config.worker_executable, "reference-worker", &raw_request),
        Some(ReferenceIdentity {
            kind: "internal".to_owned(),
            name: "arrow-json".to_owned(),
            version: "59.1.0".to_owned(),
            semantic_work: "infer and decode prepared NDJSON into Arrow batches".to_owned(),
        }),
        vec![BiasLabel {
            code: "omits_cdf_evidence".to_owned(),
            description: "omits contract validation package hashing receipts and checkpoints"
                .to_owned(),
        }],
    )?);

    let cdf_request = request_root.join("cdf-medium-ndjson.json");
    fs::write(
        &cdf_request,
        canonical_json_bytes(&PreparedFilePackageWorkload {
            fixture_name: "medium".to_owned(),
            source_root: fixture_root.clone(),
            glob: "orders.ndjson".to_owned(),
            package_dir: config.output_root.join("cdf-packages"),
            format: PreparedFileFormat::Ndjson,
            jobs: None,
            execution_host_jobs: std::thread::available_parallelism()
                .map(|jobs| u16::try_from(jobs.get()).unwrap_or(u16::MAX))
                .unwrap_or(1),
        })?,
    )?;
    observations.push(run_cell(
        provider,
        config,
        key(
            config,
            &host_class,
            "legacy_medium_throughput",
            "json_ndjson_to_package",
        ),
        command(
            &config.worker_executable,
            "cdf-file-package-worker",
            &cdf_request,
        ),
        None,
        vec![
            BiasLabel {
                code: "includes_cdf_evidence".to_owned(),
                description:
                    "includes decode validation normalization package encode hash and finalize"
                        .to_owned(),
            },
            BiasLabel {
                code: "fixture_scale".to_owned(),
                description: "medium fixture exposes current costs but is not a large scale claim"
                    .to_owned(),
            },
        ],
    )?);

    let package_build_request = request_root.join("package_build.json");
    fs::write(
        &package_build_request,
        canonical_json_bytes(&PreparedFilePackageWorkload {
            fixture_name: "medium".to_owned(),
            source_root: fixture_root.clone(),
            glob: "orders.parquet".to_owned(),
            package_dir: config.output_root.join("package-build-packages"),
            format: PreparedFileFormat::Parquet,
            jobs: None,
            execution_host_jobs: std::thread::available_parallelism()
                .map(|jobs| u16::try_from(jobs.get()).unwrap_or(u16::MAX))
                .unwrap_or(1),
        })?,
    )?;
    observations.push(run_cell(
        provider,
        config,
        key(
            config,
            &host_class,
            "legacy_medium_throughput",
            "package_build",
        ),
        command(
            &config.worker_executable,
            "cdf-file-package-worker",
            &package_build_request,
        ),
        None,
        vec![BiasLabel {
            code: "includes_cdf_evidence".to_owned(),
            description: "current file source package path includes decode validation normalization package encode hash and finalize".to_owned(),
        }],
    )?);

    for (workload_id, destination, description) in [
        (
            "duckdb_commit",
            PreparedDestinationKind::DuckDb,
            "current file source to DuckDB destination path",
        ),
        (
            "parquet_destination",
            PreparedDestinationKind::Parquet,
            "current file source to Parquet destination path",
        ),
    ] {
        let request_path = request_root.join(format!("{workload_id}.json"));
        fs::write(
            &request_path,
            canonical_json_bytes(&PreparedFileDestinationWorkload {
                fixture_name: "medium".to_owned(),
                source_root: fixture_root.clone(),
                glob: "orders.parquet".to_owned(),
                format: PreparedFileFormat::Parquet,
                output_root: config.output_root.join(format!("{workload_id}-outputs")),
                destination,
                jobs: None,
                execution_host_jobs: std::thread::available_parallelism()
                    .map(|jobs| u16::try_from(jobs.get()).unwrap_or(u16::MAX))
                    .unwrap_or(1),
            })?,
        )?;
        observations.push(run_cell(
            provider,
            config,
            key(config, &host_class, "legacy_medium_throughput", workload_id),
            command(
                &config.worker_executable,
                "cdf-file-destination-worker",
                &request_path,
            ),
            None,
            vec![BiasLabel {
                code: "includes_cdf_evidence".to_owned(),
                description: description.to_owned(),
            }],
        )?);
    }

    {
        let (workload_id, dataset_id, case_label, description) = (
            "legacy_tiny_startup_e2e",
            "legacy_tiny_startup",
            "trend.cdf_startup.file_to_duckdb.tiny",
            "startup case intentionally includes child fixture compile package destination and checkpoint",
        );
        let request_path = request_root.join(format!("{workload_id}.json"));
        fs::write(
            &request_path,
            canonical_json_bytes(&LegacyCaseWorkload {
                case_label: case_label.to_owned(),
                output_root: config.output_root.join("legacy-cases"),
            })?,
        )?;
        observations.push(run_cell(
            provider,
            config,
            key(config, &host_class, dataset_id, workload_id),
            command(
                &config.worker_executable,
                "legacy-case-worker",
                &request_path,
            ),
            None,
            vec![BiasLabel {
                code: "includes_legacy_setup".to_owned(),
                description: description.to_owned(),
            }],
        )?);
    }

    for (dataset, workload, status) in [
        (
            "nyc_tlc_yellow_2024",
            "tlc_parquet_to_package",
            ObservationStatus::Unavailable {
                reason: "full year TLC acquisition manifest is not present on this host".to_owned(),
            },
        ),
        (
            "tpch_sf10",
            "tpch_csv_to_package",
            ObservationStatus::Unavailable {
                reason: "TPC H SF10 generated dataset is not present on this host".to_owned(),
            },
        ),
        (
            "validation_generated",
            "validation_kernel",
            ObservationStatus::Failed {
                error: "dedicated P3 vector validation baseline runner is not implemented"
                    .to_owned(),
            },
        ),
        (
            "legacy_medium_throughput",
            "postgres_commit",
            ObservationStatus::Unavailable {
                reason: "disposable Postgres benchmark service is not configured".to_owned(),
            },
        ),
        (
            "nyc_tlc_yellow_2024",
            "tlc_e2e_duckdb",
            ObservationStatus::Unavailable {
                reason: "full year TLC acquisition and live network benchmark are not enabled"
                    .to_owned(),
            },
        ),
        (
            "constant_memory_100g",
            "constant_memory_stress",
            ObservationStatus::Failed {
                error: "preoptimization materializing data plane cannot safely execute the 100 GiB fixed budget stress law"
                    .to_owned(),
            },
        ),
    ] {
        observations.push(non_observed(
            key(config, &host_class, dataset, workload),
            status,
            measurement_provider.clone(),
        ));
    }

    let report = BenchmarkReport {
        schema_version: BENCHMARK_REPORT_SCHEMA_VERSION,
        host,
        observations,
    };
    validate_report(&report)?;
    Ok(report)
}

fn run_cell(
    provider: &dyn HostCapabilityProvider,
    config: &PreoptimizationBaselineConfig,
    comparability: ComparabilityKey,
    command: ChildCommand,
    reference: Option<ReferenceIdentity>,
    bias: Vec<BiasLabel>,
) -> crate::BenchResult<BenchmarkObservation> {
    run_macro_cell(
        provider,
        &MacroRunRequest {
            expected_host_class: Some(comparability.host_class.clone()),
            comparability,
            sample_count: config.sample_count,
            timeout: config.timeout,
            allow_privileged_cache_control: false,
            command,
            reference,
            bias,
        },
    )
}

fn key(
    config: &PreoptimizationBaselineConfig,
    host_class: &str,
    dataset_id: &str,
    workload_id: &str,
) -> ComparabilityKey {
    ComparabilityKey {
        dataset_id: dataset_id.to_owned(),
        workload_id: workload_id.to_owned(),
        timed_region_version: 1,
        cdf_revision: config.cdf_revision.clone(),
        dependency_tuple: config.dependency_tuple.clone(),
        host_class: host_class.to_owned(),
        os_toolchain: config.os_toolchain.clone(),
        io_mode: IoMode::Warm,
    }
}

fn command(executable: &Path, mode: &str, request: &Path) -> ChildCommand {
    ChildCommand {
        program: executable.to_path_buf(),
        args: vec![mode.to_owned(), request.display().to_string()],
        environment: BTreeMap::new(),
        current_dir: None,
    }
}

fn non_observed(
    comparability: ComparabilityKey,
    status: ObservationStatus,
    measurement_provider: Option<MeasurementProviderIdentity>,
) -> BenchmarkObservation {
    BenchmarkObservation {
        comparability,
        status,
        samples: Vec::new(),
        summary: None,
        reference: None,
        bias: Vec::new(),
        measurement_provider,
        destination_path: None,
    }
}
