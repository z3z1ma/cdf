use std::time::Duration;

use cdf_benchmarks::{BenchmarkSuite, cases_for, run_case};
use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};

fn baseline(c: &mut Criterion) {
    let suite = std::env::var("CDF_BENCH_SUITE")
        .ok()
        .and_then(|value| BenchmarkSuite::parse(&value).ok())
        .unwrap_or(BenchmarkSuite::Smoke);
    let mut group = c.benchmark_group(format!("cdf_baseline_{}", suite.as_str()));

    for case in cases_for(suite) {
        group.bench_function(case.label, |bench| {
            bench.iter_batched(
                || tempfile::tempdir().expect("create benchmark tempdir"),
                |temp| {
                    let outcome = run_case(case, temp.path()).expect("benchmark case succeeds");
                    black_box((outcome.rows, outcome.bytes));
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .configure_from_args()
        .sample_size(10)
        .measurement_time(Duration::from_secs(2));
    targets = baseline
}
criterion_main!(benches);
