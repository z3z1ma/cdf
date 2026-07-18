use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use serde::{Deserialize, Serialize};

use crate::{
    BenchResult, BenchmarkObservation, BiasLabel, Capability, ComparabilityKey, HostFingerprint,
    IoMode, MeasurementProviderIdentity, MeasurementSample, ObservationStatus, PhaseMetric,
    ReferenceIdentity, bench_error, host_class, summarize_samples,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildCommand {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub current_dir: Option<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerMeasurement {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timed_wall_time_ns: Option<u64>,
    pub rows: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub spill_bytes: u64,
    pub phases: Vec<PhaseMetric>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChildObservation {
    pub wall_time_ns: u64,
    pub cpu_time_ns: Option<u64>,
    pub peak_rss_bytes: Option<u64>,
    pub stdout: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChildObservationStatus {
    Completed(ChildObservation),
    Failed {
        exit_code: Option<i32>,
        stderr: String,
    },
    TimedOut,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachePreparation {
    pub mode: IoMode,
    pub method: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolIdentity {
    pub name: String,
    pub version: String,
    pub executable: String,
}

pub trait HostCapabilityProvider: Send + Sync {
    fn fingerprint(&self) -> BenchResult<HostFingerprint>;

    fn prepare_io_mode(&self, mode: IoMode, allow_privileged: bool)
    -> Capability<CachePreparation>;

    fn observe_child(
        &self,
        command: &ChildCommand,
        timeout: Duration,
    ) -> BenchResult<ChildObservationStatus>;

    fn discover_tool(&self, name: &str) -> Capability<ToolIdentity>;

    fn process_observer_identity(&self) -> MeasurementProviderIdentity;

    fn cgroup_memory_report(&self) -> Capability<cdf_memory::CgroupV2MemoryReport> {
        Capability::Unavailable {
            reason: "cgroup memory authority is unavailable from this host provider".to_owned(),
            method: "provider-cgroup-memory".to_owned(),
            provider_version: "provider-default-v1".to_owned(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MacroRunRequest {
    pub comparability: ComparabilityKey,
    pub expected_host_class: Option<String>,
    pub sample_count: u32,
    pub timeout: Duration,
    pub allow_privileged_cache_control: bool,
    pub command: ChildCommand,
    pub reference: Option<ReferenceIdentity>,
    pub bias: Vec<BiasLabel>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MacroRunSpec {
    pub comparability: ComparabilityKey,
    pub expected_host_class: Option<String>,
    pub sample_count: u32,
    pub timeout_ms: u64,
    pub allow_privileged_cache_control: bool,
    pub command: ChildCommand,
    pub reference: Option<ReferenceIdentity>,
    pub bias: Vec<BiasLabel>,
}

impl MacroRunSpec {
    pub fn execute(
        &self,
        provider: &dyn HostCapabilityProvider,
    ) -> BenchResult<BenchmarkObservation> {
        run_macro_cell(
            provider,
            &MacroRunRequest {
                comparability: self.comparability.clone(),
                expected_host_class: self.expected_host_class.clone(),
                sample_count: self.sample_count,
                timeout: Duration::from_millis(self.timeout_ms),
                allow_privileged_cache_control: self.allow_privileged_cache_control,
                command: self.command.clone(),
                reference: self.reference.clone(),
                bias: self.bias.clone(),
            },
        )
    }
}

pub fn run_macro_cell(
    provider: &dyn HostCapabilityProvider,
    request: &MacroRunRequest,
) -> BenchResult<BenchmarkObservation> {
    if request.sample_count == 0 || request.timeout.is_zero() {
        return Err(bench_error(
            "macro run requires positive sample count and timeout",
        ));
    }
    if request.reference.is_some() && request.bias.is_empty() {
        return Err(bench_error(
            "reference macro cells require at least one explicit semantic-work bias label",
        ));
    }
    validate_child_command(&request.command)?;
    let actual_host_class = host_class(&provider.fingerprint()?)?;
    let measurement_provider = Some(provider.process_observer_identity());
    if request.comparability.host_class != actual_host_class
        || request
            .expected_host_class
            .as_ref()
            .is_some_and(|expected| expected != &actual_host_class)
    {
        return Ok(non_observed(
            request,
            ObservationStatus::Inconclusive {
                reason: "host fingerprint changed from the requested comparability class"
                    .to_owned(),
            },
            measurement_provider,
        ));
    }

    if request.comparability.io_mode != IoMode::Cold
        && let Some(status) = cache_preparation_status(provider.prepare_io_mode(
            request.comparability.io_mode,
            request.allow_privileged_cache_control,
        ))
    {
        return Ok(non_observed(request, status, measurement_provider));
    }
    if request.comparability.io_mode == IoMode::Warm {
        match provider.observe_child(&request.command, request.timeout)? {
            ChildObservationStatus::Completed(_) => {}
            status => {
                return Ok(non_observed(
                    request,
                    child_failure_status(status, request),
                    measurement_provider,
                ));
            }
        }
    }

    let mut samples = Vec::with_capacity(request.sample_count as usize);
    for _ in 0..request.sample_count {
        if request.comparability.io_mode == IoMode::Cold
            && let Some(status) = cache_preparation_status(
                provider.prepare_io_mode(IoMode::Cold, request.allow_privileged_cache_control),
            )
        {
            return Ok(non_observed(request, status, measurement_provider));
        }
        let observed = provider.observe_child(&request.command, request.timeout)?;
        let child = match observed {
            ChildObservationStatus::Completed(child) => child,
            status => {
                return Ok(non_observed(
                    request,
                    child_failure_status(status, request),
                    measurement_provider,
                ));
            }
        };
        let payload: WorkerMeasurement =
            serde_json::from_slice(&child.stdout).map_err(|error| {
                bench_error(format!(
                    "isolated benchmark child emitted invalid measurement JSON: {error}"
                ))
            })?;
        samples.push(MeasurementSample {
            wall_time_ns: payload
                .timed_wall_time_ns
                .filter(|duration| *duration > 0)
                .unwrap_or(child.wall_time_ns),
            cpu_time_ns: child.cpu_time_ns,
            rows: payload.rows,
            logical_bytes: payload.logical_bytes,
            physical_bytes: payload.physical_bytes,
            peak_rss_bytes: child.peak_rss_bytes,
            cgroup_memory: cgroup_report(provider.cgroup_memory_report()),
            spill_bytes: payload.spill_bytes,
            phases: payload.phases,
        });
    }

    let summary = summarize_samples(&samples)?;
    Ok(BenchmarkObservation {
        comparability: request.comparability.clone(),
        status: ObservationStatus::Observed,
        samples,
        summary: Some(summary),
        reference: request.reference.clone(),
        bias: request.bias.clone(),
        measurement_provider,
        destination_path: None,
    })
}

fn cgroup_report(
    capability: Capability<cdf_memory::CgroupV2MemoryReport>,
) -> Option<cdf_memory::CgroupV2MemoryReport> {
    match capability {
        Capability::Supported { value, .. } => Some(value),
        Capability::Unavailable { .. } | Capability::Failed { .. } => None,
    }
}

fn validate_child_command(command: &ChildCommand) -> BenchResult<()> {
    if command.program.as_os_str().is_empty() {
        return Err(bench_error(
            "isolated child command program must not be empty",
        ));
    }
    for (key, value) in &command.environment {
        let normalized = key.to_ascii_lowercase();
        if [
            "secret",
            "token",
            "password",
            "credential",
            "authorization",
            "api_key",
            "apikey",
            "connection_string",
            "dsn",
        ]
        .iter()
        .any(|needle| normalized.contains(needle))
            || value.contains("secret://")
            || (value.contains("://") && value.contains('@'))
        {
            return Err(bench_error(
                "macro command environment cannot embed credential-bearing values; inherit credentials from the isolated process environment",
            ));
        }
    }
    Ok(())
}

fn cache_preparation_status(capability: Capability<CachePreparation>) -> Option<ObservationStatus> {
    match capability {
        Capability::Supported { .. } => None,
        Capability::Unavailable { reason, .. } => Some(ObservationStatus::Unavailable { reason }),
        Capability::Failed { error, .. } => Some(ObservationStatus::Failed { error }),
    }
}

fn child_failure_status(
    status: ChildObservationStatus,
    request: &MacroRunRequest,
) -> ObservationStatus {
    match status {
        ChildObservationStatus::Completed(_) => {
            unreachable!("completed child has no failure status")
        }
        ChildObservationStatus::Failed { exit_code, stderr } => {
            let mut error = format!(
                "isolated benchmark child exited unsuccessfully with code {}",
                exit_code
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "unknown".to_owned())
            );
            let stderr = stderr.trim();
            if !stderr.is_empty() {
                error.push_str("; stderr: ");
                error.push_str(stderr);
            }
            ObservationStatus::Failed { error }
        }
        ChildObservationStatus::TimedOut => ObservationStatus::TimedOut {
            timeout_ms: u64::try_from(request.timeout.as_millis()).unwrap_or(u64::MAX),
        },
    }
}

pub fn unavailable_reference_cell(
    comparability: ComparabilityKey,
    reference: ReferenceIdentity,
    bias: Vec<BiasLabel>,
    capability: Capability<ToolIdentity>,
) -> BenchResult<BenchmarkObservation> {
    if bias.is_empty() {
        return Err(bench_error(
            "unavailable reference cells require an explicit semantic-work bias label",
        ));
    }
    let status = match capability {
        Capability::Supported { .. } => ObservationStatus::Inconclusive {
            reason: "reference tool is available but this cell was not executed".to_owned(),
        },
        Capability::Unavailable { reason, .. } => ObservationStatus::Unavailable { reason },
        Capability::Failed { error, .. } => ObservationStatus::Failed { error },
    };
    Ok(BenchmarkObservation {
        comparability,
        status,
        samples: Vec::new(),
        summary: None,
        reference: Some(reference),
        bias,
        measurement_provider: None,
        destination_path: None,
    })
}

fn non_observed(
    request: &MacroRunRequest,
    status: ObservationStatus,
    measurement_provider: Option<MeasurementProviderIdentity>,
) -> BenchmarkObservation {
    BenchmarkObservation {
        comparability: request.comparability.clone(),
        status,
        samples: Vec::new(),
        summary: None,
        reference: request.reference.clone(),
        bias: request.bias.clone(),
        measurement_provider,
        destination_path: None,
    }
}
