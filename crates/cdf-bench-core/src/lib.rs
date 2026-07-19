use std::{collections::BTreeMap, error::Error};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

mod cdf_command;
mod host;
mod macro_runner;

pub use cdf_command::{CdfCommandWorkload, CdfWorkspaceMode, run_cdf_command_workload};
pub use host::{HostProbeConfig, SystemHostProvider};
pub use macro_runner::{
    CachePreparation, ChildCommand, ChildObservation, ChildObservationStatus,
    HostCapabilityProvider, MacroRunRequest, MacroRunSpec, ToolIdentity, WorkerMeasurement,
    run_macro_cell, unavailable_reference_cell,
};

pub type BenchResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

pub fn bench_error(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message.into(),
    ))
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_path: Option<DestinationPathMeasurementIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationPathMeasurementIdentity {
    pub destination_id: String,
    pub path_id: String,
    pub evidence_version: String,
    pub eligibility: DestinationPathEligibility,
    pub schema_fixture: String,
    pub evidence_record: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationPathEligibility {
    Eligible,
    Ineligible,
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
    Ineligible { reason: String },
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cgroup_memory: Option<cdf_memory::CgroupV2MemoryReport>,
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

pub fn canonical_sha256<T: Serialize>(value: &T) -> BenchResult<String> {
    let bytes = canonical_json_bytes(value)?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

pub fn canonical_json_bytes<T: Serialize + ?Sized>(value: &T) -> BenchResult<Vec<u8>> {
    let value = serde_json::to_value(value)?;
    let mut output = Vec::new();
    write_canonical_value(&value, &mut output)?;
    Ok(output)
}

fn write_canonical_value(value: &Value, output: &mut Vec<u8>) -> BenchResult<()> {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            output.extend_from_slice(serde_json::to_string(value)?.as_bytes());
        }
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_value(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(map) => {
            output.push(b'{');
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(key, _)| *key);
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                output.extend_from_slice(serde_json::to_string(key)?.as_bytes());
                output.push(b':');
                write_canonical_value(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
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
