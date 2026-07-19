use std::{error::Error, fs, path::Path};

use cdf_bench_core::{
    CdfCommandWorkload, HostCapabilityProvider, HostProbeConfig, MacroRunSpec, SystemHostProvider,
    canonical_json_bytes, host_class, run_cdf_command_workload,
};

mod validation;

use validation::{ValidationEnvelopeConfig, run_validation_envelope, validation_envelope_passes};

fn main() {
    if let Err(error) = run() {
        eprintln!("cdf-p3-measure: {error}");
        std::process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [command, request] if command == "cdf-command-worker" => {
            let workload: CdfCommandWorkload = serde_json::from_slice(&fs::read(request)?)?;
            write_stdout(&canonical_json_bytes(&run_cdf_command_workload(
                &workload,
            )?)?)
        }
        [command, request] if command == "run-cell" => {
            let spec: MacroRunSpec = serde_json::from_slice(&fs::read(request)?)?;
            write_stdout(&canonical_json_bytes(&spec.execute(&provider())?)?)
        }
        [command] if command == "host" => {
            let provider = provider();
            write_stdout(&canonical_json_bytes(&provider.fingerprint()?)?)
        }
        [command] if command == "host-class" => {
            write_stdout(host_class(&provider().fingerprint()?)?.as_bytes())
        }
        [command, samples, target_rows] if command == "validation-envelope" => {
            let report = run_validation_envelope(
                provider().fingerprint()?,
                ValidationEnvelopeConfig {
                    samples: samples.parse()?,
                    target_rows_per_sample: target_rows.parse()?,
                },
            )?;
            write_stdout(&canonical_json_bytes(&report)?)?;
            if !validation_envelope_passes(&report) {
                std::process::exit(3);
            }
            Ok(())
        }
        _ => Err(format!(
            "usage: {} cdf-command-worker REQUEST.json | run-cell REQUEST.json | host | host-class | validation-envelope SAMPLES TARGET_ROWS",
            executable_name()
        )
        .into()),
    }
}

fn provider() -> SystemHostProvider {
    SystemHostProvider::new(HostProbeConfig {
        cdf_version: env!("CARGO_PKG_VERSION").to_owned(),
        dependency_versions: std::collections::BTreeMap::from([
            ("arrow".to_owned(), "59.1.0".to_owned()),
            ("duckdb".to_owned(), "1.10504.0".to_owned()),
        ]),
        benchmark_profile: "release".to_owned(),
        storage_target: std::env::current_dir().ok(),
    })
}

fn write_stdout(bytes: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
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
        .unwrap_or("cdf-p3-measure")
        .to_owned()
}
