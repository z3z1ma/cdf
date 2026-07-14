use crate::{BenchResult, bench_error};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchmarkSuite {
    Smoke,
    Full,
    Postgres,
}

impl BenchmarkSuite {
    pub fn parse(value: &str) -> BenchResult<Self> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "full" => Ok(Self::Full),
            "postgres" => Ok(Self::Postgres),
            other => Err(bench_error(format!(
                "unknown benchmark suite `{other}`; expected smoke, full, or postgres"
            ))),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Full => "full",
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricClass {
    ReleaseGate,
    TrendOnly,
    AdHoc,
}

impl MetricClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReleaseGate => "release_gate",
            Self::TrendOnly => "trend_only",
            Self::AdHoc => "ad_hoc",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaseDefinition {
    pub label: &'static str,
    pub suite: BenchmarkSuite,
    pub metric_class: MetricClass,
    pub(crate) kind: CaseKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CaseOutcome {
    pub label: &'static str,
    pub metric_class: MetricClass,
    pub rows: u64,
    pub bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CoverageCell {
    pub area: &'static str,
    pub status: &'static str,
    pub reason: &'static str,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CaseKind {
    NativeArrow {
        fixture: &'static str,
    },
    NativeDataFusion {
        fixture: &'static str,
    },
    NativeDuckDb {
        fixture: &'static str,
    },
    CdfEnginePackage {
        fixture: &'static str,
    },
    FileToPackage {
        fixture: &'static str,
        format: LocalFormat,
    },
    RestDecode {
        fixture: &'static str,
    },
    ArchiveIpcToParquet {
        fixture: &'static str,
    },
    PackageReplay {
        fixture: &'static str,
        destination: ReplayDestination,
    },
    StartupFileToDuckDb {
        fixture: &'static str,
    },
}

impl CaseKind {
    pub(crate) fn fixture(self) -> &'static str {
        match self {
            Self::NativeArrow { fixture }
            | Self::NativeDataFusion { fixture }
            | Self::NativeDuckDb { fixture }
            | Self::CdfEnginePackage { fixture }
            | Self::FileToPackage { fixture, .. }
            | Self::RestDecode { fixture }
            | Self::ArchiveIpcToParquet { fixture }
            | Self::PackageReplay { fixture, .. }
            | Self::StartupFileToDuckDb { fixture } => fixture,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LocalFormat {
    Csv,
    Json,
    Ndjson,
    Parquet,
}

impl LocalFormat {
    pub(crate) fn all() -> [Self; 4] {
        [Self::Csv, Self::Json, Self::Ndjson, Self::Parquet]
    }

    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Ndjson => "ndjson",
            Self::Parquet => "parquet",
        }
    }

    pub(crate) fn extension(self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Json => "json",
            Self::Ndjson => "ndjson",
            Self::Parquet => "parquet",
        }
    }

    pub(crate) fn format_id(self) -> &'static str {
        self.label()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReplayDestination {
    DuckDb,
    Parquet,
    Postgres,
}

const CASES: &[CaseDefinition] = &[
    CaseDefinition {
        label: "ad_hoc.native_arrow.filter_project.medium",
        suite: BenchmarkSuite::Smoke,
        metric_class: MetricClass::AdHoc,
        kind: CaseKind::NativeArrow { fixture: "medium" },
    },
    CaseDefinition {
        label: "trend.cdf_engine.package_filter_project.medium",
        suite: BenchmarkSuite::Smoke,
        metric_class: MetricClass::TrendOnly,
        kind: CaseKind::CdfEnginePackage { fixture: "medium" },
    },
    CaseDefinition {
        label: "trend.cdf_file_to_package.ndjson.medium",
        suite: BenchmarkSuite::Smoke,
        metric_class: MetricClass::TrendOnly,
        kind: CaseKind::FileToPackage {
            fixture: "medium",
            format: LocalFormat::Ndjson,
        },
    },
    CaseDefinition {
        label: "trend.cdf_rest_decode.local.medium",
        suite: BenchmarkSuite::Smoke,
        metric_class: MetricClass::TrendOnly,
        kind: CaseKind::RestDecode { fixture: "medium" },
    },
    CaseDefinition {
        label: "trend.cdf_archive.ipc_to_parquet.medium",
        suite: BenchmarkSuite::Smoke,
        metric_class: MetricClass::TrendOnly,
        kind: CaseKind::ArchiveIpcToParquet { fixture: "medium" },
    },
    CaseDefinition {
        label: "trend.cdf_package_replay.duckdb_package_receipt_checkpoint.medium",
        suite: BenchmarkSuite::Smoke,
        metric_class: MetricClass::TrendOnly,
        kind: CaseKind::PackageReplay {
            fixture: "medium",
            destination: ReplayDestination::DuckDb,
        },
    },
    CaseDefinition {
        label: "ad_hoc.native_datafusion.filter_project.medium",
        suite: BenchmarkSuite::Full,
        metric_class: MetricClass::AdHoc,
        kind: CaseKind::NativeDataFusion { fixture: "medium" },
    },
    CaseDefinition {
        label: "ad_hoc.native_duckdb.local_insert.medium",
        suite: BenchmarkSuite::Full,
        metric_class: MetricClass::AdHoc,
        kind: CaseKind::NativeDuckDb { fixture: "medium" },
    },
    CaseDefinition {
        label: "trend.cdf_file_to_package.csv.medium",
        suite: BenchmarkSuite::Full,
        metric_class: MetricClass::TrendOnly,
        kind: CaseKind::FileToPackage {
            fixture: "medium",
            format: LocalFormat::Csv,
        },
    },
    CaseDefinition {
        label: "trend.cdf_file_to_package.json.medium",
        suite: BenchmarkSuite::Full,
        metric_class: MetricClass::TrendOnly,
        kind: CaseKind::FileToPackage {
            fixture: "medium",
            format: LocalFormat::Json,
        },
    },
    CaseDefinition {
        label: "trend.cdf_file_to_package.parquet.medium",
        suite: BenchmarkSuite::Full,
        metric_class: MetricClass::TrendOnly,
        kind: CaseKind::FileToPackage {
            fixture: "medium",
            format: LocalFormat::Parquet,
        },
    },
    CaseDefinition {
        label: "trend.cdf_package_replay.parquet_package_receipt_checkpoint.medium",
        suite: BenchmarkSuite::Full,
        metric_class: MetricClass::TrendOnly,
        kind: CaseKind::PackageReplay {
            fixture: "medium",
            destination: ReplayDestination::Parquet,
        },
    },
    CaseDefinition {
        label: "trend.cdf_package_replay.postgres_package_receipt_checkpoint.medium",
        suite: BenchmarkSuite::Postgres,
        metric_class: MetricClass::TrendOnly,
        kind: CaseKind::PackageReplay {
            fixture: "medium",
            destination: ReplayDestination::Postgres,
        },
    },
    CaseDefinition {
        label: "trend.cdf_startup.file_to_duckdb.tiny",
        suite: BenchmarkSuite::Full,
        metric_class: MetricClass::TrendOnly,
        kind: CaseKind::StartupFileToDuckDb { fixture: "tiny" },
    },
    CaseDefinition {
        label: "trend.cdf_engine.package_filter_project.wide",
        suite: BenchmarkSuite::Full,
        metric_class: MetricClass::TrendOnly,
        kind: CaseKind::CdfEnginePackage { fixture: "wide" },
    },
    CaseDefinition {
        label: "ad_hoc.native_arrow.filter_project.wide",
        suite: BenchmarkSuite::Full,
        metric_class: MetricClass::AdHoc,
        kind: CaseKind::NativeArrow { fixture: "wide" },
    },
];

const COVERAGE: &[CoverageCell] = &[
    CoverageCell {
        area: "engine_vs_direct_arrow_datafusion",
        status: "implemented",
        reason: "CDF package path, direct Arrow, and direct DataFusion local filter/project cases are present.",
    },
    CoverageCell {
        area: "file_to_package_csv_json_ndjson_parquet",
        status: "implemented",
        reason: "Public FileResource formats are generated from committed specs and packaged.",
    },
    CoverageCell {
        area: "arrow_ipc_file_to_package",
        status: "deferred",
        reason: "P3 B3 owns the native Arrow IPC stream driver; the lab does not report a removed compatibility reader as the current CDF path.",
    },
    CoverageCell {
        area: "package_replay_duckdb_parquet",
        status: "implemented",
        reason: "Public project replay APIs execute local DuckDB and filesystem Parquet with receipt/checkpoint semantics.",
    },
    CoverageCell {
        area: "package_replay_postgres",
        status: "implemented_opt_in",
        reason: "The postgres suite uses CDF_BENCH_POSTGRES_URL with ResolvedProjectDestination::postgres and is excluded from normal smoke/full local runs.",
    },
    CoverageCell {
        area: "rest_decode_local_fixture",
        status: "implemented",
        reason: "RestResource decodes local JSON fixture responses through an in-memory transport.",
    },
    CoverageCell {
        area: "archive_ipc_to_parquet",
        status: "implemented",
        reason: "Package archive transcode uses the public archive_package_to_parquet API.",
    },
    CoverageCell {
        area: "startup_medium_wide",
        status: "implemented",
        reason: "Tiny startup, medium local paths, and wide CDF/Arrow pipelines are present.",
    },
    CoverageCell {
        area: "native_polars_style",
        status: "deferred",
        reason: "No existing Polars dependency is present; this harness avoids adding a heavy non-MVP comparison dependency.",
    },
];

pub fn benchmark_cases() -> &'static [CaseDefinition] {
    CASES
}

pub fn cases_for(suite: BenchmarkSuite) -> Vec<&'static CaseDefinition> {
    CASES
        .iter()
        .filter(|case| match suite {
            BenchmarkSuite::Smoke => case.suite == BenchmarkSuite::Smoke,
            BenchmarkSuite::Full => {
                case.suite == BenchmarkSuite::Smoke || case.suite == BenchmarkSuite::Full
            }
            BenchmarkSuite::Postgres => case.suite == BenchmarkSuite::Postgres,
        })
        .collect()
}

pub fn coverage_matrix() -> &'static [CoverageCell] {
    COVERAGE
}
