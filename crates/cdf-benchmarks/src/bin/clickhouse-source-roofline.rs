use std::{env, path::PathBuf};

fn main() {
    if let Err(error) = run() {
        eprintln!("ClickHouse source roofline failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> cdf_benchmarks::BenchResult<()> {
    let endpoint = env::var("CDF_CLICKHOUSE_ENDPOINT")?;
    let output = env::var_os("CDF_CLICKHOUSE_ROOFLINE_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(".10x/evidence/.storage/2026-08-02-clickhouse-source-roofline.json")
        });
    let samples = env::var("CDF_CLICKHOUSE_ROOFLINE_SAMPLES")
        .ok()
        .map(|value| value.parse::<u32>())
        .transpose()?
        .unwrap_or(5);
    let rows = env::var("CDF_CLICKHOUSE_ROOFLINE_ROWS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(1_000_000);
    let report = cdf_benchmarks::run_clickhouse_source_roofline(&endpoint, &output, samples, rows)?;
    println!(
        "ClickHouse source roofline {}: ratio={:.3}, report={}",
        report.status,
        report.roofline_ratio_ppm as f64 / 1_000_000.0,
        output.display()
    );
    Ok(())
}
