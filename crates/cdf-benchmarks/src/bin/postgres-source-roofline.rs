use std::{env, path::PathBuf};

fn main() {
    if let Err(error) = run() {
        eprintln!("Postgres source roofline failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> cdf_benchmarks::BenchResult<()> {
    let database_url = env::var("CDF_POSTGRES_SOURCE_URL")?;
    let output = env::var_os("CDF_POSTGRES_SOURCE_ROOFLINE_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(".10x/evidence/.storage/2026-08-03-postgres-source-roofline.json")
        });
    let samples = env::var("CDF_POSTGRES_SOURCE_ROOFLINE_SAMPLES")
        .ok()
        .map(|value| value.parse::<u32>())
        .transpose()?
        .unwrap_or(5);
    let rows = env::var("CDF_POSTGRES_SOURCE_ROOFLINE_ROWS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(500_000);
    let report =
        cdf_benchmarks::run_postgres_source_roofline(&database_url, &output, samples, rows)?;
    for cell in &report.cells {
        println!(
            "Postgres source {} {}: ratio={:.3}",
            cell.shape,
            cell.status,
            cell.roofline_ratio_ppm as f64 / 1_000_000.0,
        );
    }
    println!("report={}", output.display());
    Ok(())
}
