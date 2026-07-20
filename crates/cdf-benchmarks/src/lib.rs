mod baseline;
mod catalog;
mod comparison;
mod envelope;
mod fixtures;
mod interop;
mod lab;
mod matrix;
mod package_shape;
mod profiling;
mod references;
mod resource;
mod runners;

pub use baseline::{PreoptimizationBaselineConfig, run_preoptimization_baseline};
pub use catalog::{FixtureCatalog, FixtureSpec, fixture_catalog, fixture_spec};
pub use cdf_bench_core::*;
pub use comparison::*;
pub use envelope::*;
pub use fixtures::write_all_local_fixture_formats;
pub use interop::*;
pub use lab::*;
pub use matrix::{
    BenchmarkSuite, CaseDefinition, CaseOutcome, CoverageCell, MetricClass, benchmark_cases,
    cases_for, coverage_matrix,
};
pub use package_shape::{
    PackageReadSummary, PackageShapeSummary, read_package_batches, summarize_package_shape,
};
pub use profiling::{ProfilePlan, ProfileTool, plan_profile};
pub use references::{
    ExternalFileFormat, ReferenceWorkload, discover_polars, polars_scan_command, run_reference,
};
pub use runners::{
    PreparedDestinationKind, PreparedFileDestinationRun, PreparedFileDestinationWorkload,
    PreparedFileFormat, PreparedFilePackageWorkload, PreparedIcebergPackageWorkload,
    PreparedSourcePackageRun, StartupControlWorkload, run_case, run_prepared_file_to_destination,
    run_prepared_file_to_package, run_prepared_iceberg_to_package, run_startup_control_workload,
};
