use std::{env, path::PathBuf};

fn main() {
    if let Err(error) = run() {
        eprintln!("ClickHouse destination roofline failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> cdf_benchmarks::BenchResult<()> {
    let mut arguments = env::args().skip(1);
    if arguments.next().as_deref() == Some("worker") {
        let mode = arguments
            .next()
            .ok_or_else(|| cdf_benchmarks::bench_error("worker mode is required"))?;
        let disposition = arguments
            .next()
            .ok_or_else(|| cdf_benchmarks::bench_error("worker disposition is required"))?;
        let endpoint = arguments
            .next()
            .ok_or_else(|| cdf_benchmarks::bench_error("worker endpoint is required"))?;
        let rows = arguments
            .next()
            .ok_or_else(|| cdf_benchmarks::bench_error("worker row count is required"))?
            .parse::<u64>()?;
        let measurement = cdf_benchmarks::run_clickhouse_destination_roofline_worker(
            &mode,
            &disposition,
            &endpoint,
            rows,
        )?;
        serde_json::to_writer(std::io::stdout(), &measurement)?;
        return Ok(());
    }

    let endpoint = env::var("CDF_CLICKHOUSE_ENDPOINT")?;
    let output = env::var_os("CDF_CLICKHOUSE_DESTINATION_ROOFLINE_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(".10x/evidence/.storage/2026-08-03-clickhouse-destination-roofline.json")
        });
    let samples = env::var("CDF_CLICKHOUSE_DESTINATION_ROOFLINE_SAMPLES")
        .ok()
        .map(|value| value.parse::<u32>())
        .transpose()?
        .unwrap_or(5);
    let rows = env::var("CDF_CLICKHOUSE_DESTINATION_ROOFLINE_ROWS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(1_000_000);
    let report =
        cdf_benchmarks::run_clickhouse_destination_roofline(&endpoint, &output, samples, rows)?;
    println!(
        "ClickHouse destination roofline {}: append={:.3}, native_merge={:.3}, report={}",
        report.status,
        report.cells[0].roofline_ratio_ppm as f64 / 1_000_000.0,
        report.cells[1].roofline_ratio_ppm as f64 / 1_000_000.0,
        output.display()
    );
    Ok(())
}
