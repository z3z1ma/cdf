use std::{env, path::PathBuf};

fn main() {
    if let Err(error) = run() {
        eprintln!("MySQL source roofline failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> cdf_benchmarks::BenchResult<()> {
    let database_url = env::var("CDF_MYSQL_SOURCE_URL")?;
    let output = env::var_os("CDF_MYSQL_SOURCE_ROOFLINE_OUTPUT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(".10x/evidence/.storage/2026-08-09-mysql-source-roofline.json")
        });
    let samples = env::var("CDF_MYSQL_SOURCE_ROOFLINE_SAMPLES")
        .ok()
        .map(|value| value.parse::<u32>())
        .transpose()?
        .unwrap_or(5);
    let rows = env::var("CDF_MYSQL_SOURCE_ROOFLINE_ROWS")
        .ok()
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(250_000);
    let report = cdf_benchmarks::run_mysql_source_roofline(&database_url, &output, samples, rows)?;
    println!(
        "MySQL source {}: selected ratio={:.3}",
        report.status,
        report.selected_roofline_ratio_ppm as f64 / 1_000_000.0,
    );
    for cell in &report.cells {
        println!(
            "  {} batch_rows={} ratio={:.3} status={}",
            cell.shape,
            cell.output_batch_rows,
            cell.roofline_ratio_ppm as f64 / 1_000_000.0,
            cell.status,
        );
    }
    println!("report={}", output.display());
    Ok(())
}
