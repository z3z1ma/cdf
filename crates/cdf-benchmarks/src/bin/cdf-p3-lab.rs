use std::{collections::BTreeMap, fs, path::Path};

use cdf_benchmarks::{
    BenchmarkReport, ChildCommand, HostCapabilityProvider, HostProbeConfig, InteropFixtureWorkload,
    MacroRunSpec, PreoptimizationBaselineConfig, PreparedFileDestinationWorkload,
    PreparedFilePackageWorkload, PreparedIcebergPackageWorkload, ProfileTool, ReferenceWorkload,
    StartupControlWorkload, SystemHostProvider, WorkerMeasurement, canonical_json_bytes,
    compare_reports, comparison_fails, host_class, install_baseline, plan_profile,
    read_package_batches, run_cdf_command_workload, run_interop_fixture_workload,
    run_preoptimization_baseline, run_prepared_file_to_destination, run_prepared_file_to_package,
    run_prepared_iceberg_to_package, run_reference, run_startup_control_workload,
    summarize_package_shape,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("cdf-p3-lab: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [command, request] if command == "reference-worker" => {
            let workload: ReferenceWorkload = serde_json::from_slice(&fs::read(request)?)?;
            let started = std::time::Instant::now();
            let mut measurement = run_reference(&workload)?;
            measurement.timed_wall_time_ns =
                Some(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            write_stdout(&canonical_json_bytes(&measurement)?)
        }
        [command, request] if command == "cdf-file-package-worker" => {
            let mut workload: PreparedFilePackageWorkload =
                serde_json::from_slice(&fs::read(request)?)?;
            fs::create_dir_all(&workload.package_dir)?;
            let package_root = tempfile::tempdir_in(&workload.package_dir)?;
            workload.package_dir = package_root.path().join("package");
            let started = std::time::Instant::now();
            let mut run = run_prepared_file_to_package(&workload)?;
            run.measurement.timed_wall_time_ns =
                Some(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            write_stdout(&canonical_json_bytes(&run)?)
        }
        [command, request] if command == "cdf-iceberg-package-worker" => {
            let mut workload: PreparedIcebergPackageWorkload =
                serde_json::from_slice(&fs::read(request)?)?;
            fs::create_dir_all(&workload.package_dir)?;
            let package_root = (!workload.retain_package)
                .then(|| tempfile::tempdir_in(&workload.package_dir))
                .transpose()?;
            if let Some(package_root) = &package_root {
                workload.package_dir = package_root.path().join("package");
            }
            let started = std::time::Instant::now();
            let mut run = run_prepared_iceberg_to_package(&workload)?;
            run.measurement.timed_wall_time_ns =
                Some(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            write_stdout(&canonical_json_bytes(&run)?)
        }
        [command, request] if command == "cdf-file-destination-worker" => {
            let mut workload: PreparedFileDestinationWorkload =
                serde_json::from_slice(&fs::read(request)?)?;
            let input_bytes = exact_input_bytes(&workload.source_root, &workload.glob)?;
            fs::create_dir_all(&workload.output_root)?;
            let output_root = tempfile::tempdir_in(&workload.output_root)?;
            workload.output_root = output_root.path().to_path_buf();
            let started = std::time::Instant::now();
            let run = run_prepared_file_to_destination(&workload)?;
            write_stdout(&canonical_json_bytes(&WorkerMeasurement {
                timed_wall_time_ns: Some(
                    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
                ),
                rows: run.row_count,
                logical_bytes: input_bytes,
                physical_bytes: input_bytes,
                spill_bytes: 0,
                phases: Vec::new(),
                })?)
        }
        [command, request] if command == "cdf-command-worker" => {
            let workload: cdf_benchmarks::CdfCommandWorkload =
                serde_json::from_slice(&fs::read(request)?)?;
            write_stdout(&canonical_json_bytes(&run_cdf_command_workload(&workload)?)?)
        }
        [command, request, iterations] if command == "profile-repeat-cdf" => {
            let workload: PreparedFilePackageWorkload =
                serde_json::from_slice(&fs::read(request)?)?;
            fs::create_dir_all(&workload.package_dir)?;
            let iterations = iterations.parse::<u32>()?;
            if !(1..=10_000).contains(&iterations) {
                return Err("profile repeat iterations must be between 1 and 10000".into());
            }
            for _ in 0..iterations {
                let package_root = tempfile::tempdir_in(&workload.package_dir)?;
                let mut iteration = workload.clone();
                iteration.package_dir = package_root.path().join("package");
                std::hint::black_box(run_prepared_file_to_package(&iteration)?);
            }
            Ok(())
        }
        [command, request] if command == "startup-control-worker" => {
            let mut workload: StartupControlWorkload = serde_json::from_slice(&fs::read(request)?)?;
            fs::create_dir_all(&workload.output_root)?;
            let output_root = tempfile::tempdir_in(&workload.output_root)?;
            workload.output_root = output_root.path().to_path_buf();
            let started = std::time::Instant::now();
            let mut measurement = run_startup_control_workload(&workload)?;
            measurement.timed_wall_time_ns =
                Some(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            write_stdout(&canonical_json_bytes(&measurement)?)
        }
        [command] if command == "host" => {
            let provider = provider();
            write_stdout(&canonical_json_bytes(&provider.fingerprint()?)?)
        }
        [command] if command == "host-class" => {
            write_stdout(host_class(&provider().fingerprint()?)?.as_bytes())
        }
        [command, package_dir] if command == "package-shape" => {
            write_stdout(&canonical_json_bytes(&summarize_package_shape(package_dir)?)?)
        }
        [command, package_dir] if command == "package-read" => {
            write_stdout(&canonical_json_bytes(&read_package_batches(package_dir)?)?)
        }
        [command, request] if command == "run-cell" => {
            let spec: MacroRunSpec = serde_json::from_slice(&fs::read(request)?)?;
            write_stdout(&canonical_json_bytes(&spec.execute(&provider())?)?)
        }
        [
            command,
            output_root,
            revision,
            dependencies,
            toolchain,
            samples,
        ] if command == "baseline-run" => {
            let report = run_preoptimization_baseline(
                &provider(),
                &PreoptimizationBaselineConfig {
                    worker_executable: std::env::current_exe()?,
                    output_root: Path::new(output_root).to_path_buf(),
                    cdf_revision: revision.to_owned(),
                    dependency_tuple: dependencies.to_owned(),
                    os_toolchain: toolchain.to_owned(),
                    sample_count: samples.parse()?,
                    timeout: std::time::Duration::from_secs(60),
                },
            )?;
            write_stdout(&canonical_json_bytes(&report)?)
        }
        [command, baseline, current] if command == "compare" => {
            let baseline: BenchmarkReport = serde_json::from_slice(&fs::read(baseline)?)?;
            let current: BenchmarkReport = serde_json::from_slice(&fs::read(current)?)?;
            let comparison = compare_reports(&baseline, &current)?;
            write_stdout(&canonical_json_bytes(&comparison)?)?;
            if comparison_fails(&comparison) {
                std::process::exit(3);
            }
            Ok(())
        }
        [command, report, baseline_root, repository_root, evidence]
            if command == "baseline-install" =>
        {
            let report: BenchmarkReport = serde_json::from_slice(&fs::read(report)?)?;
            let index = install_baseline(
                Path::new(baseline_root),
                Path::new(repository_root),
                &report,
                evidence,
            )?;
            write_stdout(&canonical_json_bytes(&index)?)
        }
        [command, tool, artifact, program, rest @ ..] if command == "profile-dry-run" => {
            let tool = match tool.as_str() {
                "flamegraph" => ProfileTool::Flamegraph,
                "perf-stat" => ProfileTool::PerfStat,
                _ => return Err("profile tool must be flamegraph or perf-stat".into()),
            };
            let planned = plan_profile(
                &provider(),
                tool,
                &ChildCommand {
                    program: Path::new(program).to_path_buf(),
                    args: rest.to_vec(),
                    environment: BTreeMap::new(),
                    current_dir: None,
                },
                Path::new("."),
                artifact,
            )?;
            write_stdout(&canonical_json_bytes(&planned)?)
        }
        [command, millis] if command == "sleep-worker" => {
            std::thread::sleep(std::time::Duration::from_millis(millis.parse()?));
            write_stdout(&canonical_json_bytes(&cdf_benchmarks::WorkerMeasurement {
                timed_wall_time_ns: None,
                rows: 0,
                logical_bytes: 0,
                physical_bytes: 0,
                spill_bytes: 0,
                phases: Vec::new(),
            })?)
        }
        [command, request] if command == "interop-fixture-worker" => {
            let workload: InteropFixtureWorkload = serde_json::from_slice(&fs::read(request)?)?;
            let started = std::time::Instant::now();
            let mut run = run_interop_fixture_workload(&workload)?;
            run.measurement.timed_wall_time_ns =
                Some(u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX));
            write_stdout(&canonical_json_bytes(&run)?)
        }
        _ => Err(format!(
            "usage: {} reference-worker REQUEST.json | cdf-command-worker REQUEST.json | host | package-shape PACKAGE | package-read PACKAGE | run-cell REQUEST.json | baseline-run OUTPUT_ROOT REVISION DEPENDENCIES TOOLCHAIN SAMPLES | compare BASELINE.json CURRENT.json",
            executable_name()
        )
        .into()),
    }
}

fn provider() -> SystemHostProvider {
    SystemHostProvider::new(HostProbeConfig {
        cdf_version: env!("CARGO_PKG_VERSION").to_owned(),
        dependency_versions: BTreeMap::from([
            ("arrow".to_owned(), "58.3.0".to_owned()),
            ("duckdb".to_owned(), "1.10504.0".to_owned()),
        ]),
        benchmark_profile: "release".to_owned(),
        storage_target: std::env::current_dir().ok(),
    })
}

fn write_stdout(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::io::Write;
    std::io::stdout().write_all(bytes)?;
    Ok(())
}

fn exact_input_bytes(
    source_root: &Path,
    glob: &str,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    if glob.contains('*') || glob.contains('?') || glob.contains('[') || glob.contains('{') {
        return Ok(0);
    }
    Ok(fs::metadata(source_root.join(glob))?.len())
}

fn executable_name() -> String {
    std::env::args()
        .next()
        .as_deref()
        .and_then(|value| Path::new(value).file_name())
        .and_then(|name| name.to_str())
        .unwrap_or("cdf-p3-lab")
        .to_owned()
}
