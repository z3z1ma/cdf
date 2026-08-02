use std::{env, path::PathBuf};

fn main() {
    if let Err(error) = run() {
        eprintln!("SQLite source roofline failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> cdf_benchmarks::BenchResult<()> {
    let mut arguments = env::args().skip(1);
    if arguments.next().as_deref() == Some("worker") {
        let mode = arguments
            .next()
            .ok_or_else(|| cdf_benchmarks::bench_error("worker mode is required"))?;
        let database = PathBuf::from(
            arguments
                .next()
                .ok_or_else(|| cdf_benchmarks::bench_error("worker database path is required"))?,
        );
        let expected_rows = arguments
            .next()
            .ok_or_else(|| cdf_benchmarks::bench_error("worker row count is required"))?
            .parse::<u64>()?;
        let measurement =
            cdf_benchmarks::run_sqlite_source_roofline_worker(&mode, &database, expected_rows)?;
        serde_json::to_writer(std::io::stdout(), &measurement)?;
        return Ok(());
    }

    let output = env::var_os("CDF_SQLITE_ROOFLINE_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(".10x/evidence/.storage/2026-08-02-sqlite-source-roofline.json")
        });
    let samples = env::var("CDF_SQLITE_ROOFLINE_SAMPLES")
        .ok()
        .map(|value| value.parse::<u32>())
        .transpose()?
        .unwrap_or(5);
    let rows = env::var("CDF_SQLITE_ROOFLINE_ROWS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(1_000_000);
    let report = cdf_benchmarks::run_sqlite_source_roofline(&output, samples, rows)?;
    println!(
        "SQLite source roofline {}: ratio={:.3}, report={}",
        report.status,
        report.roofline_ratio_ppm as f64 / 1_000_000.0,
        output.display()
    );
    Ok(())
}
