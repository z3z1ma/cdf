use std::{
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use cdf_benchmarks::{BenchResult, BenchmarkSuite, CaseOutcome, cases_for, run_case};
use clap::{Arg, Command, error::ErrorKind};
use serde::Serialize;

#[derive(Serialize)]
struct TrendRecord {
    observed_at_ms: u128,
    suite: &'static str,
    label: &'static str,
    metric_class: &'static str,
    elapsed_ns: u128,
    rows: u64,
    bytes: u64,
}

fn main() -> BenchResult<()> {
    let args = Args::parse()?;
    if let Some(parent) = args.out.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&args.out)?;

    for case in cases_for(args.suite) {
        let temp = tempfile::tempdir()?;
        let started = Instant::now();
        let outcome = run_case(case, temp.path())?;
        write_record(&mut file, args.suite, elapsed_record(outcome, started))?;
    }

    println!("{}", args.out.display());
    Ok(())
}

fn elapsed_record(outcome: CaseOutcome, started: Instant) -> TrendRecord {
    TrendRecord {
        observed_at_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_millis(),
        suite: "",
        label: outcome.label,
        metric_class: outcome.metric_class.as_str(),
        elapsed_ns: started.elapsed().as_nanos(),
        rows: outcome.rows,
        bytes: outcome.bytes,
    }
}

fn write_record(
    file: &mut std::fs::File,
    suite: BenchmarkSuite,
    mut record: TrendRecord,
) -> BenchResult<()> {
    record.suite = suite.as_str();
    serde_json::to_writer(&mut *file, &record)?;
    file.write_all(b"\n")?;
    Ok(())
}

struct Args {
    suite: BenchmarkSuite,
    out: PathBuf,
}

impl Args {
    fn parse() -> BenchResult<Self> {
        let matches = command().try_get_matches().map_err(|error| {
            if error.kind() == ErrorKind::DisplayHelp {
                let _ = error.print();
                std::process::exit(0);
            }
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                error.to_string(),
            )) as Box<dyn std::error::Error + Send + Sync>
        })?;
        let suite_value = raw_value(&matches, "suite")
            .map(|value| utf8_value(value, "--suite value"))
            .transpose()?
            .unwrap_or("smoke");
        let suite = BenchmarkSuite::parse(suite_value)?;
        let out = raw_value(&matches, "out")
            .map(|value| utf8_value(value, "--out value").map(PathBuf::from))
            .transpose()?
            .unwrap_or_else(|| {
                PathBuf::from(format!(
                    "target/cdf-benchmarks/trends/{}.jsonl",
                    suite.as_str()
                ))
            });
        Ok(Self { suite, out })
    }
}

fn command() -> Command {
    Command::new("cdf-benchmark-trend")
        .arg(
            Arg::new("suite")
                .long("suite")
                .value_name("smoke|full|postgres")
                .num_args(1),
        )
        .arg(
            Arg::new("out")
                .long("out")
                .value_name("target/cdf-benchmarks/trends/<suite>.jsonl")
                .num_args(1),
        )
}

fn raw_value<'a>(matches: &'a clap::ArgMatches, name: &str) -> Option<&'a OsStr> {
    matches.get_raw(name).and_then(|mut values| values.next())
}

fn utf8_value<'a>(value: &'a OsStr, label: &str) -> BenchResult<&'a str> {
    value.to_str().ok_or_else(|| {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{label} is not valid UTF-8: {value:?}"),
        )) as Box<dyn std::error::Error + Send + Sync>
    })
}
