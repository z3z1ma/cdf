use std::{collections::BTreeMap, fs, path::Path};

use cdf_benchmarks::{
    BenchmarkReport, ChildCommand, EnvelopeSpec, HostCapabilityProvider, HostProbeConfig,
    MacroRunSpec, ProfileTool, ReferenceWorkload, SystemHostProvider, canonical_json_bytes,
    compare_reports, comparison_fails, generate_envelope, host_class, install_baseline,
    plan_profile, run_reference,
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
        [command] if command == "host" => {
            let provider = provider();
            write_stdout(&canonical_json_bytes(&provider.fingerprint()?)?)
        }
        [command] if command == "host-class" => {
            write_stdout(host_class(&provider().fingerprint()?)?.as_bytes())
        }
        [command, request] if command == "run-cell" => {
            let spec: MacroRunSpec = serde_json::from_slice(&fs::read(request)?)?;
            write_stdout(&canonical_json_bytes(&spec.execute(&provider())?)?)
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
        [command, report, spec] if command == "envelope" => {
            let report: BenchmarkReport = serde_json::from_slice(&fs::read(report)?)?;
            let spec: EnvelopeSpec = serde_json::from_slice(&fs::read(spec)?)?;
            write_stdout(generate_envelope(&report, &spec)?.as_bytes())
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
        _ => Err(format!(
            "usage: {} reference-worker REQUEST.json | host",
            executable_name()
        )
        .into()),
    }
}

fn provider() -> SystemHostProvider {
    SystemHostProvider::new(HostProbeConfig {
        cdf_version: env!("CARGO_PKG_VERSION").to_owned(),
        dependency_versions: BTreeMap::from([
            ("arrow".to_owned(), "59.1.0".to_owned()),
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

fn executable_name() -> String {
    std::env::args()
        .next()
        .as_deref()
        .and_then(|value| Path::new(value).file_name())
        .and_then(|name| name.to_str())
        .unwrap_or("cdf-p3-lab")
        .to_owned()
}
