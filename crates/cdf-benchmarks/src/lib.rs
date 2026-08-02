#![doc = "Opt-in benchmark suites, fixtures, reference comparisons, and report orchestration for CDF. Production data execution remains in the runtime and adapter crates."]

mod baseline;
mod catalog;
mod comparison;
mod duckdb_profile;
mod envelope;
mod fixtures;
mod interop;
mod lab;
mod matrix;
mod package_shape;
mod profiling;
#[allow(
    unsafe_code,
    reason = "measurement-only FFI exception governed by .10x/decisions/compiler-enforced-rust-safety-walls.md"
)]
mod references;
mod resource;
mod runners;
mod sqlite_source_roofline;
mod stress;

pub use baseline::{PreoptimizationBaselineConfig, run_preoptimization_baseline};
pub use catalog::{FixtureCatalog, FixtureSpec, fixture_catalog, fixture_spec};
pub use cdf_bench_core::{
    BenchResult, BenchmarkObservation, BenchmarkReport, BiasLabel, CachePreparation, Capability,
    CdfCommandWorkload, CdfWorkspaceMode, ChildCommand, ChildObservation, ChildObservationStatus,
    ComparabilityKey, DestinationPathEligibility, DestinationPathMeasurementIdentity, EffectiveCpu,
    HostCapabilityProvider, HostFingerprint, HostProbeConfig, IoMode, MacroRunRequest,
    MacroRunSpec, MeasurementProviderIdentity, MeasurementSample, MeasurementSummary,
    ObservationStatus, OsFingerprint, PhaseMetric, ReferenceIdentity, StorageClass,
    SystemHostProvider, ToolIdentity, WorkerMeasurement, bench_error, canonical_json_bytes,
    canonical_sha256, host_class, run_cdf_command_workload, run_macro_cell, summarize_samples,
    unavailable_reference_cell,
};
pub use comparison::{
    BaselineEntry, BaselineIndex, ComparisonCell, ComparisonReport, ComparisonVerdict,
    HIGH_VARIANCE_MAD_PERCENT, REGRESSION_THRESHOLD_PERCENT, compare_reports, comparison_fails,
    install_baseline,
};
pub use duckdb_profile::{
    DUCKDB_PROFILE_SUMMARY_VERSION, DuckDbOperatorProfile, DuckDbProfileSummary,
    read_duckdb_profile, summarize_duckdb_profile,
};
pub use envelope::{
    CloseoutEnvelope, CloseoutEnvelopeCell, CloseoutEnvelopeStatus, DestinationBulkCatalogEntry,
    DestinationEnvelopeTarget, EnvelopeSpec, EnvelopeTarget,
    destination_execution_descriptor_sha256, generate_closeout_envelope, generate_envelope,
};
pub use fixtures::write_all_local_fixture_formats;
pub use interop::{
    ArrowCZeroCopyProbe, INTEROP_REPORT_SCHEMA_VERSION, InteropBatchCurvePoint,
    InteropCancellationReport, InteropCancellationStatus, InteropCellStatus, InteropCopyProof,
    InteropEnvironment, InteropFixtureWorkload, InteropMeasurementReport, InteropModeReport,
    InteropNativeReference, InteropReferenceStatus, InteropSample, InteropWorkerMeasurement,
    classify_arrow_c_zero_copy_probe, run_interop_fixture_workload,
};
pub use lab::{
    BENCHMARK_REPORT_SCHEMA_VERSION, ByteCounterAuthority, DATASET_CATALOG_SCHEMA_VERSION,
    DatasetCatalog, DatasetProvenance, DatasetRecipe, DatasetSpec, GeneratorDelivery,
    ImportedTrendRecord, IncomparableTrendImport, MAX_GENERATOR_CHUNK_BYTES, RemoteFilesMirror,
    SyntheticJsonShape, TimedRegionPolicy, WorkloadSpec, dataset_catalog,
    import_incomparable_trend, report_fixture, validate_dataset_catalog, validate_report,
};
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
    PreparedFileFormat, PreparedFilePackageWorkload, PreparedIcebergCatalog,
    PreparedIcebergPackageWorkload, PreparedSourceIoStage, PreparedSourcePackageRun,
    StartupControlWorkload, run_case, run_prepared_file_to_destination,
    run_prepared_file_to_package, run_prepared_iceberg_to_package, run_startup_control_workload,
};
pub use sqlite_source_roofline::{
    SqliteSourceRooflineReport, run_sqlite_source_roofline, run_sqlite_source_roofline_worker,
};
pub use stress::{ConstantMemoryParquetRecipe, generate_constant_memory_parquet};
