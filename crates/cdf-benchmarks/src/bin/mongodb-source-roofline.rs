use std::{env, path::PathBuf};

fn main() {
    if let Err(error) = run() {
        eprintln!("MongoDB source roofline failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> cdf_benchmarks::BenchResult<()> {
    let endpoint = env::var("CDF_MONGODB_SOURCE_ENDPOINT")?;
    let output = env::var_os("CDF_MONGODB_SOURCE_ROOFLINE_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(".10x/evidence/.storage/2026-08-04-mongodb-source-roofline.json")
        });
    let samples = env::var("CDF_MONGODB_SOURCE_ROOFLINE_SAMPLES")
        .ok()
        .map(|value| value.parse::<u32>())
        .transpose()?
        .unwrap_or(5);
    let rows = env::var("CDF_MONGODB_SOURCE_ROOFLINE_ROWS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(250_000);
    let report = cdf_benchmarks::run_mongodb_source_roofline(&endpoint, &output, samples, rows)?;
    println!(
        "MongoDB source {}: batch_rows={} max_pool_size={} ratio={:.3}",
        report.status,
        report.batch_rows,
        report.max_pool_size,
        report.roofline_ratio_ppm as f64 / 1_000_000.0,
    );
    println!("report={}", output.display());
    Ok(())
}
