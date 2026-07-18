use std::error::Error;

mod baseline;
mod catalog;
mod cdf_command;
mod comparison;
mod envelope;
mod fixtures;
mod host;
mod interop;
mod lab;
mod macro_runner;
mod matrix;
mod package_shape;
mod profiling;
mod references;
mod resource;
mod runners;

pub use baseline::{PreoptimizationBaselineConfig, run_preoptimization_baseline};
pub use catalog::{FixtureCatalog, FixtureSpec, fixture_catalog, fixture_spec};
pub use cdf_command::{CdfCommandWorkload, CdfWorkspaceMode, run_cdf_command_workload};
pub use cdf_package::canonical_json_bytes;
pub use comparison::*;
pub use envelope::*;
pub use fixtures::write_all_local_fixture_formats;
pub use host::{HostProbeConfig, SystemHostProvider};
pub use interop::*;
pub use lab::*;
pub use macro_runner::*;
pub use matrix::{
    BenchmarkSuite, CaseDefinition, CaseOutcome, CoverageCell, MetricClass, benchmark_cases,
    cases_for, coverage_matrix,
};
pub use package_shape::{PackageShapeSummary, summarize_package_shape};
pub use profiling::{ProfilePlan, ProfileTool, plan_profile};
pub use references::{
    ExternalFileFormat, ReferenceWorkload, discover_polars, polars_scan_command, run_reference,
};
pub use runners::{
    PreparedDestinationKind, PreparedFileDestinationRun, PreparedFileDestinationWorkload,
    PreparedFileFormat, PreparedFilePackageRun, PreparedFilePackageWorkload,
    StartupControlWorkload, run_case, run_prepared_file_to_destination,
    run_prepared_file_to_package, run_startup_control_workload,
};

pub type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub(crate) fn bench_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.into(),
    ))
}
