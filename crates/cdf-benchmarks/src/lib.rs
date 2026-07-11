use std::error::Error;

mod catalog;
mod fixtures;
mod host;
mod lab;
mod macro_runner;
mod matrix;
mod profiling;
mod references;
mod resource;
mod runners;

pub use catalog::{FixtureCatalog, FixtureSpec, fixture_catalog, fixture_spec};
pub use cdf_package::canonical_json_bytes;
pub use fixtures::write_all_local_fixture_formats;
pub use host::{HostProbeConfig, SystemHostProvider};
pub use lab::*;
pub use macro_runner::*;
pub use matrix::{
    BenchmarkSuite, CaseDefinition, CaseOutcome, CoverageCell, MetricClass, benchmark_cases,
    cases_for, coverage_matrix,
};
pub use profiling::{ProfilePlan, ProfileTool, plan_profile};
pub use references::{
    ExternalFileFormat, ReferenceWorkload, discover_polars, polars_scan_command, run_reference,
};
pub use runners::run_case;

pub type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub(crate) fn bench_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.into(),
    ))
}
