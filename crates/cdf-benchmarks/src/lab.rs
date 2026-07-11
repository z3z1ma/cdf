use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{BenchResult, bench_error};

pub const DATASET_CATALOG_SCHEMA_VERSION: u16 = 2;
pub const BENCHMARK_REPORT_SCHEMA_VERSION: u16 = 1;
pub const MAX_GENERATOR_CHUNK_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetCatalog {
    pub schema_version: u16,
    pub datasets: Vec<DatasetSpec>,
    pub workloads: Vec<WorkloadSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetSpec {
    pub id: String,
    pub description: String,
    pub schema_ref: String,
    pub expected_rows: Option<u64>,
    pub expected_logical_bytes: Option<u64>,
    pub expected_physical_bytes: Option<u64>,
    pub provenance: DatasetProvenance,
    pub recipe: DatasetRecipe,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatasetProvenance {
    pub source: String,
    pub version: String,
    pub license: String,
    pub license_url: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DatasetRecipe {
    BenchmarkFixture {
        fixture_catalog_version: u16,
        fixture_name: String,
        generator_version: String,
        rows: u64,
        batch_rows: u64,
        max_generated_bytes: u64,
    },
    RemoteFiles {
        base_url: String,
        object_template: String,
        immutable_identity: String,
    },
    Tpch {
        generator: String,
        generator_version: String,
        scale_factor: u32,
        seed: u64,
        formats: Vec<String>,
        chunk_bytes: u64,
        delivery: GeneratorDelivery,
    },
    SyntheticJson {
        generator_version: String,
        seed: u64,
        shape: SyntheticJsonShape,
        logical_bytes: u64,
        chunk_bytes: u64,
        delivery: GeneratorDelivery,
    },
    SyntheticStream {
        generator_version: String,
        seed: u64,
        logical_bytes: u64,
        record_bytes: u64,
        chunk_bytes: u64,
        delivery: GeneratorDelivery,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GeneratorDelivery {
    Streaming,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyntheticJsonShape {
    Wide,
    Nested,
    Dirty,
    SchemaVarying,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSpec {
    pub id: String,
    pub dataset_id: String,
    pub operation: String,
    pub timed_region: TimedRegionPolicy,
    pub logical_byte_counter: ByteCounterAuthority,
    pub physical_byte_counter: ByteCounterAuthority,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimedRegionPolicy {
    pub version: u16,
    pub includes: Vec<String>,
    pub excludes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ByteCounterAuthority {
    pub name: String,
    pub method: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostFingerprint {
    pub schema_version: u16,
    pub architecture: String,
    pub cpu_label: String,
    pub advertised_logical_cores: u32,
    pub advertised_physical_cores: Capability<u32>,
    pub advertised_memory_bytes: Capability<u64>,
    pub effective_cpu: Capability<EffectiveCpu>,
    pub effective_memory_bytes: Capability<u64>,
    pub storage: Capability<StorageClass>,
    pub os: OsFingerprint,
    pub rust_version: String,
    pub cdf_version: String,
    pub dependency_versions: BTreeMap<String, String>,
    pub benchmark_profile: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Capability<T> {
    Supported {
        value: T,
        method: String,
        provider_version: String,
    },
    Unavailable {
        reason: String,
        method: String,
        provider_version: String,
    },
    Failed {
        error: String,
        method: String,
        provider_version: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveCpu {
    pub logical_cores: u32,
    pub quota_millicores: Option<u32>,
    pub affinity_cores: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageClass {
    pub medium: String,
    pub filesystem: String,
    pub label: String,
    pub free_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OsFingerprint {
    pub family: String,
    pub version: String,
    pub kernel: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub schema_version: u16,
    pub host: HostFingerprint,
    pub observations: Vec<BenchmarkObservation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BenchmarkObservation {
    pub comparability: ComparabilityKey,
    pub status: ObservationStatus,
    pub samples: Vec<MeasurementSample>,
    pub summary: Option<MeasurementSummary>,
    pub reference: Option<ReferenceIdentity>,
    pub bias: Vec<BiasLabel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub measurement_provider: Option<MeasurementProviderIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeasurementProviderIdentity {
    pub method: String,
    pub version: String,
    pub observes_cpu_time: bool,
    pub observes_peak_rss: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComparabilityKey {
    pub dataset_id: String,
    pub workload_id: String,
    pub timed_region_version: u16,
    pub cdf_revision: String,
    pub dependency_tuple: String,
    pub host_class: String,
    pub os_toolchain: String,
    pub io_mode: IoMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IoMode {
    Warm,
    Cold,
    Uncontrolled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ObservationStatus {
    Observed,
    Failed { error: String },
    TimedOut { timeout_ms: u64 },
    Unavailable { reason: String },
    Inconclusive { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeasurementSample {
    pub wall_time_ns: u64,
    pub cpu_time_ns: Option<u64>,
    pub rows: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub peak_rss_bytes: Option<u64>,
    pub spill_bytes: u64,
    pub phases: Vec<PhaseMetric>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhaseMetric {
    pub phase: String,
    pub duration_ns: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeasurementSummary {
    pub sample_count: u32,
    pub median_wall_time_ns: u64,
    pub median_absolute_deviation_ns: u64,
    pub median_rows_per_second: u64,
    pub median_logical_bytes_per_second: u64,
    pub median_physical_bytes_per_second: u64,
    pub peak_rss_bytes: Option<u64>,
    pub spill_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceIdentity {
    pub kind: String,
    pub name: String,
    pub version: String,
    pub semantic_work: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BiasLabel {
    pub code: String,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyTrendRecord {
    pub observed_at_ms: u128,
    pub suite: String,
    pub label: String,
    pub metric_class: String,
    pub elapsed_ns: u128,
    pub rows: u64,
    pub bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyTrendImport {
    pub record: LegacyTrendRecord,
    pub status: ObservationStatus,
}

pub fn dataset_catalog() -> BenchResult<DatasetCatalog> {
    let catalog: DatasetCatalog =
        serde_json::from_str(include_str!("../fixtures/p3-datasets.json"))?;
    validate_dataset_catalog(&catalog)?;
    Ok(catalog)
}

pub fn report_fixture() -> BenchResult<BenchmarkReport> {
    let report: BenchmarkReport =
        serde_json::from_str(include_str!("../fixtures/p3-report-fixture.json"))?;
    validate_report(&report)?;
    Ok(report)
}

pub fn import_legacy_trend(bytes: &[u8]) -> BenchResult<LegacyTrendImport> {
    let record: LegacyTrendRecord = serde_json::from_slice(bytes)?;
    Ok(LegacyTrendImport {
        record,
        status: ObservationStatus::Inconclusive {
            reason: "legacy trend records lack host, revision, timed-region, cache-mode, distribution, RSS, and comparability authority"
                .to_owned(),
        },
    })
}

pub fn canonical_sha256<T: Serialize>(value: &T) -> BenchResult<String> {
    let bytes = cdf_package::canonical_json_bytes(value)?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

pub fn host_class(host: &HostFingerprint) -> BenchResult<String> {
    let authority = (
        &host.architecture,
        &host.cpu_label,
        host.advertised_logical_cores,
        &host.advertised_physical_cores,
        &host.advertised_memory_bytes,
        &host.effective_cpu,
        &host.effective_memory_bytes,
        &host.storage,
        &host.os,
    );
    let digest = canonical_sha256(&authority)?;
    Ok(format!("host-class-{}", &digest[7..23]))
}

pub fn summarize_samples(samples: &[MeasurementSample]) -> BenchResult<MeasurementSummary> {
    if samples.is_empty() || samples.iter().any(|sample| sample.wall_time_ns == 0) {
        return Err(bench_error(
            "measurement summary requires samples with positive wall time",
        ));
    }
    let wall = samples
        .iter()
        .map(|sample| sample.wall_time_ns)
        .collect::<Vec<_>>();
    let median_wall_time_ns = median(wall.clone());
    let median_absolute_deviation_ns = median(
        wall.into_iter()
            .map(|value| value.abs_diff(median_wall_time_ns))
            .collect(),
    );
    let rate = |value: u64, duration_ns: u64| -> u64 {
        u64::try_from(u128::from(value).saturating_mul(1_000_000_000) / u128::from(duration_ns))
            .unwrap_or(u64::MAX)
    };
    Ok(MeasurementSummary {
        sample_count: u32::try_from(samples.len())
            .map_err(|error| bench_error(format!("sample count overflow: {error}")))?,
        median_wall_time_ns,
        median_absolute_deviation_ns,
        median_rows_per_second: median(
            samples
                .iter()
                .map(|sample| rate(sample.rows, sample.wall_time_ns))
                .collect(),
        ),
        median_logical_bytes_per_second: median(
            samples
                .iter()
                .map(|sample| rate(sample.logical_bytes, sample.wall_time_ns))
                .collect(),
        ),
        median_physical_bytes_per_second: median(
            samples
                .iter()
                .map(|sample| rate(sample.physical_bytes, sample.wall_time_ns))
                .collect(),
        ),
        peak_rss_bytes: samples
            .iter()
            .filter_map(|sample| sample.peak_rss_bytes)
            .max(),
        spill_bytes: samples
            .iter()
            .map(|sample| sample.spill_bytes)
            .max()
            .unwrap_or(0),
    })
}

fn median(mut values: Vec<u64>) -> u64 {
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        values[middle - 1].saturating_add(values[middle]) / 2
    } else {
        values[middle]
    }
}

pub fn validate_dataset_catalog(catalog: &DatasetCatalog) -> BenchResult<()> {
    if catalog.schema_version != DATASET_CATALOG_SCHEMA_VERSION {
        return Err(bench_error(format!(
            "unsupported P3 dataset catalog schema version {}",
            catalog.schema_version
        )));
    }
    let mut dataset_ids = BTreeSet::new();
    for dataset in &catalog.datasets {
        require_text(&dataset.id, "dataset id")?;
        require_text(&dataset.schema_ref, "dataset schema_ref")?;
        require_text(&dataset.provenance.source, "dataset provenance source")?;
        require_text(&dataset.provenance.version, "dataset provenance version")?;
        require_text(&dataset.provenance.license, "dataset provenance license")?;
        if !dataset_ids.insert(dataset.id.as_str()) {
            return Err(bench_error(format!(
                "duplicate dataset id `{}`",
                dataset.id
            )));
        }
        validate_recipe(&dataset.id, &dataset.recipe)?;
    }
    let mut workload_ids = BTreeSet::new();
    for workload in &catalog.workloads {
        require_text(&workload.id, "workload id")?;
        if !workload_ids.insert(workload.id.as_str()) {
            return Err(bench_error(format!(
                "duplicate workload id `{}`",
                workload.id
            )));
        }
        if !dataset_ids.contains(workload.dataset_id.as_str()) {
            return Err(bench_error(format!(
                "workload `{}` references unknown dataset `{}`",
                workload.id, workload.dataset_id
            )));
        }
        if workload.timed_region.version == 0 {
            return Err(bench_error(format!(
                "workload `{}` timed-region version must be positive",
                workload.id
            )));
        }
        if workload.timed_region.includes.is_empty() {
            return Err(bench_error(format!(
                "workload `{}` must name timed-region inclusions",
                workload.id
            )));
        }
    }
    Ok(())
}

pub fn validate_report(report: &BenchmarkReport) -> BenchResult<()> {
    if report.schema_version != BENCHMARK_REPORT_SCHEMA_VERSION {
        return Err(bench_error(format!(
            "unsupported benchmark report schema version {}",
            report.schema_version
        )));
    }
    validate_host(&report.host)?;
    let mut keys = BTreeSet::new();
    for observation in &report.observations {
        let key = &observation.comparability;
        for (value, label) in [
            (&key.dataset_id, "dataset_id"),
            (&key.workload_id, "workload_id"),
            (&key.cdf_revision, "cdf_revision"),
            (&key.dependency_tuple, "dependency_tuple"),
            (&key.host_class, "host_class"),
            (&key.os_toolchain, "os_toolchain"),
        ] {
            require_text(value, label)?;
        }
        reject_sensitive_identity(&key.host_class, "host_class")?;
        reject_sensitive_identity(&key.os_toolchain, "os_toolchain")?;
        if key.timed_region_version == 0 {
            return Err(bench_error("timed_region_version must be positive"));
        }
        let serialized_key = serde_json::to_string(key)?;
        if !keys.insert(serialized_key) {
            return Err(bench_error("duplicate benchmark comparability key"));
        }
        match &observation.status {
            ObservationStatus::Observed => {
                let summary = observation
                    .summary
                    .as_ref()
                    .ok_or_else(|| bench_error("observed benchmark cell must contain a summary"))?;
                if observation.samples.is_empty()
                    || summary.sample_count as usize != observation.samples.len()
                {
                    return Err(bench_error(
                        "observed benchmark sample_count must match non-empty samples",
                    ));
                }
                if observation
                    .samples
                    .iter()
                    .any(|sample| sample.wall_time_ns == 0)
                {
                    return Err(bench_error(
                        "observed benchmark samples require positive wall_time_ns",
                    ));
                }
                if observation.samples.windows(2).any(|samples| {
                    samples[0].rows != samples[1].rows
                        || samples[0].logical_bytes != samples[1].logical_bytes
                        || samples[0].physical_bytes != samples[1].physical_bytes
                }) {
                    return Err(bench_error(
                        "observed benchmark samples must repeat identical rows and byte authorities",
                    ));
                }
                if summary != &summarize_samples(&observation.samples)? {
                    return Err(bench_error(
                        "observed benchmark summary must be derived exactly from retained samples",
                    ));
                }
            }
            ObservationStatus::Failed { error } => {
                require_text(error, "failure error")?;
                reject_sensitive_identity(error, "failure error")?;
            }
            ObservationStatus::TimedOut { timeout_ms } if *timeout_ms == 0 => {
                return Err(bench_error("timed-out cell requires positive timeout_ms"));
            }
            ObservationStatus::Unavailable { reason }
            | ObservationStatus::Inconclusive { reason } => {
                require_text(reason, "non-observed reason")?;
                reject_sensitive_identity(reason, "non-observed reason")?;
            }
            ObservationStatus::TimedOut { .. } => {}
        }
        if !matches!(observation.status, ObservationStatus::Observed)
            && (!observation.samples.is_empty() || observation.summary.is_some())
        {
            return Err(bench_error(
                "non-observed benchmark cells cannot contain samples or summary",
            ));
        }
        if let Some(reference) = &observation.reference {
            for (value, label) in [
                (&reference.kind, "reference kind"),
                (&reference.name, "reference name"),
                (&reference.version, "reference version"),
                (&reference.semantic_work, "reference semantic_work"),
            ] {
                require_text(value, label)?;
                reject_sensitive_identity(value, label)?;
            }
            if observation.bias.is_empty() {
                return Err(bench_error(
                    "reference observations require explicit semantic-work bias labels",
                ));
            }
        }
        for bias in &observation.bias {
            require_text(&bias.code, "bias code")?;
            require_text(&bias.description, "bias description")?;
            reject_sensitive_identity(&bias.code, "bias code")?;
            reject_sensitive_identity(&bias.description, "bias description")?;
        }
        if let Some(provider) = &observation.measurement_provider {
            require_text(&provider.method, "measurement provider method")?;
            require_text(&provider.version, "measurement provider version")?;
            reject_sensitive_identity(&provider.method, "measurement provider method")?;
            reject_sensitive_identity(&provider.version, "measurement provider version")?;
        }
    }
    Ok(())
}

fn validate_recipe(dataset_id: &str, recipe: &DatasetRecipe) -> BenchResult<()> {
    match recipe {
        DatasetRecipe::BenchmarkFixture {
            fixture_catalog_version,
            fixture_name,
            generator_version,
            rows,
            batch_rows,
            max_generated_bytes,
        } => {
            require_text(fixture_name, "benchmark fixture name")?;
            require_text(generator_version, "benchmark fixture generator version")?;
            if *fixture_catalog_version == 0
                || *rows == 0
                || *batch_rows == 0
                || batch_rows > rows
                || *max_generated_bytes == 0
                || *max_generated_bytes > MAX_GENERATOR_CHUNK_BYTES
            {
                return Err(bench_error(format!(
                    "dataset `{dataset_id}` benchmark fixture requires positive bounded catalog rows batches and bytes"
                )));
            }
            let catalog = crate::fixture_catalog()?;
            let fixture = crate::fixture_spec(fixture_name)?;
            if *fixture_catalog_version != catalog.schema_version
                || *generator_version != crate::fixtures::LEGACY_FIXTURE_GENERATOR_VERSION
                || *rows != fixture.rows as u64
                || *batch_rows != fixture.batch_size as u64
            {
                return Err(bench_error(format!(
                    "dataset `{dataset_id}` benchmark fixture recipe does not match its generator authority"
                )));
            }
        }
        DatasetRecipe::RemoteFiles {
            base_url,
            object_template,
            immutable_identity,
        } => {
            require_text(base_url, "remote base_url")?;
            require_text(object_template, "remote object_template")?;
            require_text(immutable_identity, "remote immutable_identity")?;
        }
        DatasetRecipe::Tpch {
            scale_factor,
            formats,
            chunk_bytes,
            ..
        } => {
            if *scale_factor == 0 || formats.is_empty() {
                return Err(bench_error(format!(
                    "dataset `{dataset_id}` TPC-H recipe requires scale and formats"
                )));
            }
            validate_chunk(dataset_id, *chunk_bytes)?;
        }
        DatasetRecipe::SyntheticJson {
            logical_bytes,
            chunk_bytes,
            ..
        }
        | DatasetRecipe::SyntheticStream {
            logical_bytes,
            chunk_bytes,
            ..
        } => {
            if *logical_bytes == 0 {
                return Err(bench_error(format!(
                    "dataset `{dataset_id}` requires positive logical_bytes"
                )));
            }
            validate_chunk(dataset_id, *chunk_bytes)?;
        }
    }
    Ok(())
}

fn validate_chunk(dataset_id: &str, chunk_bytes: u64) -> BenchResult<()> {
    if chunk_bytes == 0 || chunk_bytes > MAX_GENERATOR_CHUNK_BYTES {
        return Err(bench_error(format!(
            "dataset `{dataset_id}` generator chunk_bytes must be between 1 and {MAX_GENERATOR_CHUNK_BYTES}"
        )));
    }
    Ok(())
}

fn validate_host(host: &HostFingerprint) -> BenchResult<()> {
    if host.schema_version != 1 || host.advertised_logical_cores == 0 {
        return Err(bench_error(
            "invalid host fingerprint version or core count",
        ));
    }
    for (value, label) in [
        (&host.architecture, "host architecture"),
        (&host.cpu_label, "host cpu_label"),
        (&host.os.family, "host os family"),
        (&host.os.version, "host os version"),
        (&host.rust_version, "host rust_version"),
        (&host.cdf_version, "host cdf_version"),
        (&host.benchmark_profile, "host benchmark_profile"),
    ] {
        require_text(value, label)?;
        reject_sensitive_identity(value, label)?;
    }
    if let Some(kernel) = &host.os.kernel {
        reject_sensitive_identity(kernel, "host os kernel")?;
    }
    validate_capability_metadata(&host.advertised_physical_cores)?;
    validate_capability_metadata(&host.advertised_memory_bytes)?;
    validate_capability_metadata(&host.effective_cpu)?;
    validate_capability_metadata(&host.effective_memory_bytes)?;
    validate_capability_metadata(&host.storage)?;
    if let Capability::Supported { value, .. } = &host.storage {
        for (field, label) in [
            (&value.medium, "storage medium"),
            (&value.filesystem, "storage filesystem"),
            (&value.label, "storage label"),
        ] {
            require_text(field, label)?;
            reject_sensitive_identity(field, label)?;
        }
    }
    for (name, version) in &host.dependency_versions {
        require_text(name, "dependency name")?;
        require_text(version, "dependency version")?;
        reject_sensitive_identity(name, "dependency name")?;
        reject_sensitive_identity(version, "dependency version")?;
    }
    Ok(())
}

fn validate_capability_metadata<T>(capability: &Capability<T>) -> BenchResult<()> {
    let (method, provider_version, detail) = match capability {
        Capability::Supported {
            method,
            provider_version,
            ..
        } => (method, provider_version, None),
        Capability::Unavailable {
            reason,
            method,
            provider_version,
        } => (method, provider_version, Some(reason)),
        Capability::Failed {
            error,
            method,
            provider_version,
        } => (method, provider_version, Some(error)),
    };
    for (value, label) in [
        (method, "capability method"),
        (provider_version, "capability provider_version"),
    ] {
        require_text(value, label)?;
        reject_sensitive_identity(value, label)?;
    }
    if let Some(detail) = detail {
        require_text(detail, "capability detail")?;
        reject_sensitive_identity(detail, "capability detail")?;
    }
    Ok(())
}

fn require_text(value: &str, label: &str) -> BenchResult<()> {
    if value.trim().is_empty() {
        return Err(bench_error(format!("{label} must not be empty")));
    }
    Ok(())
}

fn reject_sensitive_identity(value: &str, label: &str) -> BenchResult<()> {
    if value.contains('/') || value.contains('\\') || value.contains('@') {
        return Err(bench_error(format!(
            "{label} must be sanitized and cannot contain paths or user/host identity"
        )));
    }
    Ok(())
}
