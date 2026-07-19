use std::collections::{BTreeMap, BTreeSet, VecDeque};

use cdf_kernel::{CdfError, PartitionPlan, PartitionRetrySafety, Result, ScanPlan};
use serde::{Deserialize, Serialize};

use crate::{
    CompiledSourceExecutionPlan, CompiledSourceRetry, DecodeUnitPlan, DestinationIngressMode,
    DestinationRuntimeCapabilities, DestinationWriterModel, ExecutionHostCapabilities,
    ExecutionServices, SourceExecutionCapabilities, SourceExecutorClass, artifact_hash,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CanonicalPartitionOrdinal(u32);

impl CanonicalPartitionOrdinal {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CanonicalUnitOrdinal(u32);

impl CanonicalUnitOrdinal {
    pub fn new(value: u32) -> Self {
        Self(value)
    }

    pub fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledPartition {
    pub ordinal: CanonicalPartitionOrdinal,
    pub partition: PartitionPlan,
    pub immutable_identity_hash: String,
    pub minimum_working_set_bytes: u64,
    pub maximum_working_set_bytes: u64,
    pub executor_class: SourceExecutorClass,
    pub retry: Option<CompiledSourceRetry>,
    pub rate_limit: Option<crate::SourceRateLimit>,
    pub quota_authority: Option<String>,
    pub speculative_safe: bool,
    pub canonical_order: bool,
    pub bounded_source: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalPartitionSchedule {
    pub plan_id: String,
    pub partitions: Vec<ScheduledPartition>,
}

impl CanonicalPartitionSchedule {
    pub fn compile(source: &CompiledSourceExecutionPlan, scan: &ScanPlan) -> Result<Self> {
        source.validate()?;
        if source.resource_id != scan.request.resource_id {
            return Err(CdfError::contract(
                "source plan and scan plan resource ids do not match",
            ));
        }
        let mut partition_ids = BTreeSet::new();
        let minimum_working_set_bytes = source
            .execution_capabilities
            .minimum_poll_bytes
            .checked_add(source.execution_capabilities.minimum_decode_bytes)
            .ok_or_else(|| CdfError::contract("source minimum working set overflowed u64"))?;
        let maximum_working_set_bytes = source
            .execution_capabilities
            .maximum_poll_bytes
            .checked_add(source.execution_capabilities.maximum_decode_bytes)
            .ok_or_else(|| CdfError::contract("source maximum working set overflowed u64"))?;
        let partitions = scan
            .partitions
            .iter()
            .enumerate()
            .map(|(ordinal, partition)| {
                if !partition_ids.insert(partition.partition_id.as_str()) {
                    return Err(CdfError::contract(format!(
                        "scan plan contains duplicate partition id `{}`",
                        partition.partition_id
                    )));
                }
                let ordinal = u32::try_from(ordinal).map_err(|_| {
                    CdfError::contract("scan plan partition count exceeds u32 ordinals")
                })?;
                let immutable_identity_hash = artifact_hash(&serde_json::json!({
                    "driver": source.driver,
                    "physical_plan_hash": source.physical_plan_hash,
                    "partition": partition,
                }))?;
                Ok(ScheduledPartition {
                    ordinal: CanonicalPartitionOrdinal::new(ordinal),
                    partition: partition.clone(),
                    immutable_identity_hash,
                    minimum_working_set_bytes,
                    maximum_working_set_bytes,
                    executor_class: source.execution_capabilities.executor_class,
                    retry: compile_partition_retry(
                        &source.execution_capabilities,
                        partition.retry_safety,
                    )?,
                    rate_limit: source.execution_capabilities.rate_limit,
                    quota_authority: source.execution_capabilities.quota_authority.clone(),
                    speculative_safe: source.execution_capabilities.speculative_safe,
                    canonical_order: source.execution_capabilities.canonical_order,
                    bounded_source: source.execution_capabilities.bounded,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            plan_id: scan.plan_id.to_string(),
            partitions,
        })
    }

    pub fn validate_against_scan(
        &self,
        scan: &ScanPlan,
        source: &CompiledSourceExecutionPlan,
    ) -> Result<()> {
        source.validate()?;
        if self.plan_id != scan.plan_id.as_str() || self.partitions.len() != scan.partitions.len() {
            return Err(CdfError::data(
                "partition schedule does not match its scan plan identity or partition count",
            ));
        }
        for (ordinal, (scheduled, partition)) in
            self.partitions.iter().zip(&scan.partitions).enumerate()
        {
            if usize::try_from(scheduled.ordinal.get()).ok() != Some(ordinal)
                || &scheduled.partition != partition
                || scheduled.minimum_working_set_bytes == 0
                || scheduled.maximum_working_set_bytes < scheduled.minimum_working_set_bytes
            {
                return Err(CdfError::data(
                    "partition schedule contains stale ordinal, partition, or working-set evidence",
                ));
            }
            let expected_identity = artifact_hash(&serde_json::json!({
                "driver": source.driver,
                "physical_plan_hash": source.physical_plan_hash,
                "partition": partition,
            }))?;
            let expected_retry =
                compile_partition_retry(&source.execution_capabilities, partition.retry_safety)?;
            if scheduled.immutable_identity_hash != expected_identity
                || scheduled.minimum_working_set_bytes
                    != source
                        .execution_capabilities
                        .minimum_poll_bytes
                        .checked_add(source.execution_capabilities.minimum_decode_bytes)
                        .ok_or_else(|| {
                            CdfError::contract("source minimum working set overflowed u64")
                        })?
                || scheduled.maximum_working_set_bytes
                    != source
                        .execution_capabilities
                        .maximum_poll_bytes
                        .checked_add(source.execution_capabilities.maximum_decode_bytes)
                        .ok_or_else(|| {
                            CdfError::contract("source maximum working set overflowed u64")
                        })?
                || scheduled.executor_class != source.execution_capabilities.executor_class
                || scheduled.retry != expected_retry
                || scheduled.rate_limit != source.execution_capabilities.rate_limit
                || scheduled.quota_authority != source.execution_capabilities.quota_authority
                || scheduled.speculative_safe != source.execution_capabilities.speculative_safe
                || scheduled.canonical_order != source.execution_capabilities.canonical_order
                || scheduled.bounded_source != source.execution_capabilities.bounded
            {
                return Err(CdfError::data(
                    "partition schedule widens or differs from its compiled source execution plan",
                ));
            }
            validate_partition_retry_binding(partition.retry_safety, scheduled.retry.as_ref())?;
        }
        Ok(())
    }
}

fn validate_partition_retry_binding(
    partition_safety: PartitionRetrySafety,
    retry: Option<&CompiledSourceRetry>,
) -> Result<()> {
    let Some(retry) = retry else {
        return if partition_safety == PartitionRetrySafety::Forbidden {
            Ok(())
        } else {
            Err(CdfError::data(
                "retry-safe partition schedule omitted its compiled retry policy",
            ))
        };
    };
    retry.validate()?;
    let expected_attestation = match partition_safety {
        PartitionRetrySafety::Forbidden => {
            return Err(CdfError::data(
                "retry-forbidden partition schedule contains a compiled retry policy",
            ));
        }
        PartitionRetrySafety::ImmutableContent => {
            crate::SourceAttestationStrength::ImmutableContent
        }
        PartitionRetrySafety::Snapshot => crate::SourceAttestationStrength::Snapshot,
    };
    if retry.granularity != crate::SourceRetryGranularity::Partition
        || retry.attestation != expected_attestation
    {
        return Err(CdfError::data(
            "partition schedule retry granularity or attestation exceeds its planned safety proof",
        ));
    }
    Ok(())
}

fn compile_partition_retry(
    capabilities: &SourceExecutionCapabilities,
    partition_safety: PartitionRetrySafety,
) -> Result<Option<CompiledSourceRetry>> {
    if partition_safety == PartitionRetrySafety::Forbidden {
        capabilities.validate()?;
        return Ok(None);
    }
    if capabilities.retry_granularity != crate::SourceRetryGranularity::Partition {
        return Err(CdfError::contract(
            "retry-safe partition requires partition-granularity source retry capability",
        ));
    }
    let expected_attestation = match partition_safety {
        PartitionRetrySafety::Forbidden => unreachable!("forbidden retry returned above"),
        PartitionRetrySafety::ImmutableContent => {
            crate::SourceAttestationStrength::ImmutableContent
        }
        PartitionRetrySafety::Snapshot => crate::SourceAttestationStrength::Snapshot,
    };
    if capabilities.attestation != expected_attestation {
        return Err(CdfError::contract(
            "partition retry identity proof does not match the source attestation capability",
        ));
    }
    CompiledSourceRetry::from_capabilities(capabilities)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionCeilings {
    pub configured_jobs: Option<u16>,
    pub container_cpu_slots: u16,
    pub managed_memory_bytes: u64,
    pub transport_connections: Option<u16>,
    pub destination_writers: Option<u16>,
    pub staged_destination_in_flight: Option<u16>,
    pub lane_concurrency: Option<u16>,
    pub scope_concurrency: Option<u16>,
}

impl AdmissionCeilings {
    pub fn validate(&self) -> Result<()> {
        if self.container_cpu_slots == 0
            || self.managed_memory_bytes == 0
            || self.configured_jobs == Some(0)
            || self.transport_connections == Some(0)
            || self.destination_writers == Some(0)
            || self.staged_destination_in_flight == Some(0)
            || self.lane_concurrency == Some(0)
            || self.scope_concurrency == Some(0)
        {
            return Err(CdfError::contract(
                "scheduler ceilings must be nonzero when declared",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveJobsResolution {
    pub jobs: u16,
    pub memory_jobs: u16,
    pub cpu_jobs: u16,
    pub limiting_factors: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSchedulerResolution {
    pub effective_jobs: EffectiveJobsResolution,
    pub container_cpu_slots: u16,
    pub managed_memory_available_bytes: u64,
    pub source_maximum_concurrency: u16,
    pub source_useful_concurrency: u16,
    pub source_lane_concurrency: Option<u16>,
    pub source_rate_limit: Option<crate::SourceRateLimit>,
    pub source_quota_authority: Option<String>,
    pub source_bounded: bool,
    pub transport_connection_limit: Option<u16>,
    pub destination_writer_concurrency: u16,
    pub destination_in_flight_segments: Option<u16>,
}

impl RuntimeSchedulerResolution {
    /// Revalidates the source-owned half of a recorded scheduler join immediately before work.
    /// Destination limits may narrow this result, but no caller may widen or substitute source
    /// concurrency, lane, quota, rate, or boundedness authority after planning.
    pub fn validate_for_source(
        &self,
        partition_count: usize,
        source: &SourceExecutionCapabilities,
    ) -> Result<()> {
        source.validate()?;
        let compiled_lane = source
            .blocking_lane
            .as_ref()
            .map(|lane| lane.maximum_concurrency);
        if self.source_maximum_concurrency != source.maximum_concurrency
            || self.source_useful_concurrency != source.useful_concurrency
            || match (compiled_lane, self.source_lane_concurrency) {
                (None, None) => false,
                (Some(compiled), Some(bound)) => bound == 0 || bound > compiled,
                _ => true,
            }
            || self.source_rate_limit != source.rate_limit
            || self.source_quota_authority != source.quota_authority
            || self.source_bounded != source.bounded
        {
            return Err(CdfError::data(
                "runtime scheduler source authority differs from the compiled source plan",
            ));
        }
        let jobs = self.effective_jobs.jobs;
        if partition_count == 0 {
            if jobs != 0 {
                return Err(CdfError::data(
                    "runtime scheduler admitted jobs for an empty source plan",
                ));
            }
            return Ok(());
        }
        let partition_ceiling = u16::try_from(partition_count).unwrap_or(u16::MAX);
        let lane_ceiling = self.source_lane_concurrency.unwrap_or(u16::MAX);
        if jobs == 0
            || jobs > partition_ceiling
            || jobs > source.maximum_concurrency
            || jobs > source.useful_concurrency
            || jobs > lane_ceiling
            || jobs > self.container_cpu_slots
            || jobs > self.effective_jobs.memory_jobs
            || jobs > self.effective_jobs.cpu_jobs
        {
            return Err(CdfError::data(
                "runtime scheduler job admission exceeds compiled source or host ceilings",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodeUnitConcurrencyResolution {
    pub jobs: u16,
    pub memory_jobs: u16,
    pub cpu_jobs: u16,
    pub estimated_bytes_per_job: u64,
    pub limiting_factors: Vec<String>,
}

pub fn resolve_decode_unit_concurrency(
    units: &[DecodeUnitPlan],
    host: &ExecutionHostCapabilities,
    cpu: &crate::CpuTaskSpec,
    managed_memory_available_bytes: u64,
    useful_concurrency: u16,
    target_batch_bytes: u64,
    buffered_batches_per_unit: u16,
) -> Result<DecodeUnitConcurrencyResolution> {
    host.validate()?;
    cpu.validate()?;
    if units.is_empty() {
        return Ok(DecodeUnitConcurrencyResolution {
            jobs: 0,
            memory_jobs: 0,
            cpu_jobs: 0,
            estimated_bytes_per_job: 0,
            limiting_factors: vec!["no_units".to_owned()],
        });
    }
    if managed_memory_available_bytes == 0
        || useful_concurrency == 0
        || target_batch_bytes == 0
        || buffered_batches_per_unit == 0
    {
        return Err(CdfError::contract(
            "decode-unit concurrency requires nonzero memory, useful concurrency, batch bytes, and buffered-batch bound",
        ));
    }
    for unit in units {
        unit.validate()?;
    }
    let maximum_unit_bytes = units
        .iter()
        .map(|unit| unit.estimated_working_set_bytes)
        .max()
        .unwrap_or(0);
    let buffered_batch_bytes = target_batch_bytes
        .checked_mul(u64::from(buffered_batches_per_unit))
        .ok_or_else(|| CdfError::contract("decode-unit buffered batch bytes overflowed"))?;
    let estimated_bytes_per_job = maximum_unit_bytes
        .checked_add(buffered_batch_bytes)
        .ok_or_else(|| CdfError::contract("decode-unit working set overflowed"))?;
    let memory_jobs = u16::try_from(
        (managed_memory_available_bytes / estimated_bytes_per_job).min(u64::from(u16::MAX)),
    )
    .unwrap_or(u16::MAX);
    if memory_jobs == 0 {
        return Err(CdfError::data(format!(
            "one decode unit requires an estimated {estimated_bytes_per_job} bytes including bounded handoff, but only {managed_memory_available_bytes} managed bytes are available; raise the memory budget, reduce the codec batch target, or reduce decode concurrency"
        )));
    }
    let cpu_jobs = host.logical_cpu_slots / cpu.claimed_cpu_slots();
    if cpu_jobs == 0 {
        return Err(CdfError::data(format!(
            "one decode unit claims {} CPU slots but the host provides {}",
            cpu.claimed_cpu_slots(),
            host.logical_cpu_slots
        )));
    }
    let unit_jobs = u16::try_from(units.len()).unwrap_or(u16::MAX);
    let candidates = [
        ("unit_count", unit_jobs),
        ("source_useful", useful_concurrency),
        ("container_cpu", cpu_jobs),
        ("managed_memory", memory_jobs),
    ];
    let jobs = candidates
        .iter()
        .map(|(_, value)| *value)
        .min()
        .unwrap_or(1);
    let limiting_factors = candidates
        .iter()
        .filter(|(_, value)| *value == jobs)
        .map(|(name, _)| (*name).to_owned())
        .collect();
    Ok(DecodeUnitConcurrencyResolution {
        jobs,
        memory_jobs,
        cpu_jobs,
        estimated_bytes_per_job,
        limiting_factors,
    })
}

pub fn resolve_runtime_scheduler(
    partition_count: usize,
    source: &SourceExecutionCapabilities,
    destination: &DestinationRuntimeCapabilities,
    execution: &ExecutionServices,
    configured_jobs: Option<u16>,
) -> Result<RuntimeSchedulerResolution> {
    source.validate()?;
    destination.validate()?;
    let host = execution.capabilities();
    let memory = execution.memory().snapshot();
    let available_memory = memory.budget_bytes.saturating_sub(memory.current_bytes);
    let destination_writer_concurrency = match destination.writer_model {
        DestinationWriterModel::SingleWriter => 1,
        DestinationWriterModel::ConcurrentSegments => {
            destination.max_in_flight_segments.unwrap_or(u16::MAX)
        }
    };
    let destination_in_flight_segments = (destination.ingress_mode
        == DestinationIngressMode::StagedDurableSegments)
        .then_some(destination.max_in_flight_segments)
        .flatten();
    let default_staged_destination_pressure = (configured_jobs.is_none()
        && destination.ingress_mode == DestinationIngressMode::StagedDurableSegments)
        .then_some(destination.max_in_flight_segments)
        .flatten();
    // Staged destinations are a bounded downstream pressure authority, not a hidden unbounded
    // queue. By default, join their declared in-flight window into source admission so a fast
    // local source cannot overdrive a single-writer staged destination into low-progress waits.
    // An explicit --jobs/configured_jobs value remains the operator knob for deliberate
    // overdrive experiments; explicit configuration is still bounded by source, CPU, and memory.
    let lane_concurrency = source
        .blocking_lane
        .as_ref()
        .map(|compiled| {
            let installed = host
                .blocking_lanes
                .iter()
                .find(|lane| lane.lane_id == compiled.lane_id);
            let resolved = match (compiled.binding, installed) {
                (crate::BlockingLaneBinding::Static, None) => compiled,
                (crate::BlockingLaneBinding::Static, Some(bound))
                | (crate::BlockingLaneBinding::RuntimeResolvedRequired, Some(bound)) => bound,
                (crate::BlockingLaneBinding::RuntimeResolvedRequired, None) => {
                    return Err(CdfError::contract(format!(
                        "blocking lane `{}` requires source resolution against the execution host before scheduler admission",
                        compiled.lane_id
                    )));
                }
                (crate::BlockingLaneBinding::RuntimeResolved, _) => {
                    return Err(CdfError::contract(
                        "compiled scheduler input cannot contain an already runtime-resolved blocking lane",
                    ));
                }
            };
            resolved.validate_tightening_of(compiled)?;
            Ok::<u16, CdfError>(resolved.maximum_concurrency)
        })
        .transpose()?;
    let ceilings = AdmissionCeilings {
        configured_jobs,
        container_cpu_slots: host.logical_cpu_slots,
        managed_memory_bytes: available_memory,
        transport_connections: None,
        destination_writers: None,
        staged_destination_in_flight: default_staged_destination_pressure,
        lane_concurrency,
        scope_concurrency: None,
    };
    Ok(RuntimeSchedulerResolution {
        effective_jobs: resolve_effective_jobs(partition_count, source, &ceilings)?,
        container_cpu_slots: host.logical_cpu_slots,
        managed_memory_available_bytes: available_memory,
        source_maximum_concurrency: source.maximum_concurrency,
        source_useful_concurrency: source.useful_concurrency,
        source_lane_concurrency: ceilings.lane_concurrency,
        source_rate_limit: source.rate_limit,
        source_quota_authority: source.quota_authority.clone(),
        source_bounded: source.bounded,
        transport_connection_limit: ceilings.transport_connections,
        destination_writer_concurrency,
        destination_in_flight_segments,
    })
}

pub fn effective_container_cpu_slots(
    available_parallelism: u16,
    cgroup_cpu_max: Option<&str>,
) -> Result<u16> {
    if available_parallelism == 0 {
        return Err(CdfError::contract("available parallelism must be nonzero"));
    }
    let Some(cpu_max) = cgroup_cpu_max
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(available_parallelism);
    };
    let mut parts = cpu_max.split_whitespace();
    let quota = parts
        .next()
        .ok_or_else(|| CdfError::contract("cgroup cpu.max omitted quota"))?;
    let period = parts
        .next()
        .ok_or_else(|| CdfError::contract("cgroup cpu.max omitted period"))?;
    if parts.next().is_some() {
        return Err(CdfError::contract(
            "cgroup cpu.max must contain quota and period",
        ));
    }
    if quota == "max" {
        return Ok(available_parallelism);
    }
    let quota = quota
        .parse::<u64>()
        .map_err(|error| CdfError::contract(format!("invalid cgroup CPU quota: {error}")))?;
    let period = period
        .parse::<u64>()
        .map_err(|error| CdfError::contract(format!("invalid cgroup CPU period: {error}")))?;
    if quota == 0 || period == 0 {
        return Err(CdfError::contract(
            "cgroup CPU quota and period must be nonzero",
        ));
    }
    let quota_slots = quota.div_ceil(period).min(u64::from(u16::MAX)) as u16;
    Ok(available_parallelism.min(quota_slots.max(1)))
}

pub fn resolve_effective_jobs(
    partition_count: usize,
    source: &SourceExecutionCapabilities,
    ceilings: &AdmissionCeilings,
) -> Result<EffectiveJobsResolution> {
    source.validate()?;
    ceilings.validate()?;
    if partition_count == 0 {
        return Ok(EffectiveJobsResolution {
            jobs: 0,
            memory_jobs: 0,
            cpu_jobs: 0,
            limiting_factors: vec!["no_partitions".to_owned()],
        });
    }
    // Admission is a safety bound, not a throughput estimate. Every admitted source may retain
    // its compiled maximum while the canonical head is stalled, so sizing from the minimum can
    // admit a frontier that cannot make forward progress under the memory ledger.
    let working_set = source
        .maximum_poll_bytes
        .checked_add(source.maximum_decode_bytes)
        .ok_or_else(|| CdfError::contract("source maximum working set overflowed u64"))?;
    let memory_jobs =
        u16::try_from((ceilings.managed_memory_bytes / working_set).min(u64::from(u16::MAX)))
            .unwrap_or(u16::MAX);
    if memory_jobs == 0 {
        return Err(CdfError::data(format!(
            "one source partition requires {working_set} bytes but the managed scheduler budget is {} bytes; raise the memory budget or reduce the source working set",
            ceilings.managed_memory_bytes
        )));
    }
    let cpu_cost = source
        .blocking_lane
        .as_ref()
        .map(crate::BlockingLaneSpec::claimed_cpu_slots)
        .unwrap_or(1);
    let cpu_jobs = ceilings.container_cpu_slots / cpu_cost;
    if cpu_jobs == 0 {
        return Err(CdfError::data(
            "one source partition requires more CPU slots than the effective container provides",
        ));
    }
    let partition_jobs = u16::try_from(partition_count).unwrap_or(u16::MAX);
    let candidates = [
        ("partition_count", partition_jobs),
        (
            "configured_jobs",
            ceilings.configured_jobs.unwrap_or(u16::MAX),
        ),
        ("source_maximum", source.maximum_concurrency),
        ("source_useful", source.useful_concurrency),
        ("container_cpu", cpu_jobs),
        ("managed_memory", memory_jobs),
        (
            "transport_connections",
            ceilings.transport_connections.unwrap_or(u16::MAX),
        ),
        (
            "destination_writers",
            ceilings.destination_writers.unwrap_or(u16::MAX),
        ),
        (
            "staged_destination_in_flight",
            ceilings.staged_destination_in_flight.unwrap_or(u16::MAX),
        ),
        (
            "blocking_lane",
            ceilings.lane_concurrency.unwrap_or(u16::MAX),
        ),
        (
            "checkpoint_scope",
            ceilings.scope_concurrency.unwrap_or(u16::MAX),
        ),
    ];
    let jobs = candidates
        .iter()
        .map(|(_, value)| *value)
        .min()
        .unwrap_or(1);
    let limiting_factors = candidates
        .iter()
        .filter(|(_, value)| *value == jobs)
        .map(|(name, _)| (*name).to_owned())
        .collect();
    Ok(EffectiveJobsResolution {
        jobs,
        memory_jobs,
        cpu_jobs,
        limiting_factors,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionRequest {
    pub resource: String,
    pub ordinal: CanonicalPartitionOrdinal,
    pub memory_bytes: u64,
    pub cpu_slots: u16,
    pub io_permits: u16,
    pub connection_permits: u16,
    pub quota_authority: Option<String>,
    pub scope_lease: Option<String>,
}

impl AdmissionRequest {
    pub fn validate(&self) -> Result<()> {
        if self.resource.is_empty()
            || self.memory_bytes == 0
            || self.cpu_slots == 0
            || self.io_permits == 0
            || self.connection_permits == 0
        {
            return Err(CdfError::contract(
                "admission requests require resource identity and nonzero resource costs",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionLimits {
    pub jobs: u16,
    pub memory_bytes: u64,
    pub cpu_slots: u16,
    pub io_permits: u16,
    pub connection_permits: u16,
    pub quota_limits: BTreeMap<String, u16>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionPermit {
    id: u64,
    pub request: AdmissionRequest,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdmissionSnapshot {
    pub queued: usize,
    pub active: usize,
    pub memory_bytes: u64,
    pub cpu_slots: u16,
    pub io_permits: u16,
    pub connection_permits: u16,
    pub cancelled: bool,
}

pub struct FairAdmissionController {
    limits: AdmissionLimits,
    queues: BTreeMap<String, VecDeque<AdmissionRequest>>,
    rotation: VecDeque<String>,
    active: BTreeMap<u64, AdmissionRequest>,
    active_scopes: BTreeSet<String>,
    active_quotas: BTreeMap<String, u16>,
    snapshot: AdmissionSnapshot,
    next_id: u64,
    cancelled: bool,
}

impl FairAdmissionController {
    pub fn new(limits: AdmissionLimits) -> Result<Self> {
        if limits.jobs == 0
            || limits.memory_bytes == 0
            || limits.cpu_slots == 0
            || limits.io_permits == 0
            || limits.connection_permits == 0
            || limits.quota_limits.values().any(|value| *value == 0)
        {
            return Err(CdfError::contract("admission limits must be nonzero"));
        }
        Ok(Self {
            limits,
            queues: BTreeMap::new(),
            rotation: VecDeque::new(),
            active: BTreeMap::new(),
            active_scopes: BTreeSet::new(),
            active_quotas: BTreeMap::new(),
            snapshot: AdmissionSnapshot::default(),
            next_id: 1,
            cancelled: false,
        })
    }

    pub fn enqueue(&mut self, request: AdmissionRequest) -> Result<()> {
        if self.cancelled {
            return Err(CdfError::internal(
                "scheduler admission is cancelled; no new work may be queued",
            ));
        }
        request.validate()?;
        if request.memory_bytes > self.limits.memory_bytes
            || request.cpu_slots > self.limits.cpu_slots
            || request.io_permits > self.limits.io_permits
            || request.connection_permits > self.limits.connection_permits
        {
            return Err(CdfError::data(format!(
                "partition {} for resource `{}` exceeds a scheduler capacity ceiling",
                request.ordinal.get(),
                request.resource
            )));
        }
        let resource = request.resource.clone();
        let queue = self.queues.entry(resource.clone()).or_default();
        if queue.is_empty() && !self.rotation.contains(&resource) {
            self.rotation.push_back(resource);
        }
        queue.push_back(request);
        self.snapshot.queued += 1;
        Ok(())
    }

    pub fn try_admit_next(&mut self) -> Option<AdmissionPermit> {
        if self.cancelled || self.snapshot.active >= usize::from(self.limits.jobs) {
            return None;
        }
        let candidates = self.rotation.len();
        for _ in 0..candidates {
            let resource = self.rotation.pop_front()?;
            let eligible = self
                .queues
                .get(&resource)
                .and_then(VecDeque::front)
                .is_some_and(|request| self.eligible(request));
            if !eligible {
                self.rotation.push_back(resource);
                continue;
            }
            let request = self.queues.get_mut(&resource)?.pop_front()?;
            if self
                .queues
                .get(&resource)
                .is_some_and(|queue| queue.is_empty())
            {
                self.queues.remove(&resource);
            } else {
                self.rotation.push_back(resource);
            }
            self.snapshot.queued -= 1;
            self.snapshot.active += 1;
            self.snapshot.memory_bytes += request.memory_bytes;
            self.snapshot.cpu_slots += request.cpu_slots;
            self.snapshot.io_permits += request.io_permits;
            self.snapshot.connection_permits += request.connection_permits;
            if let Some(scope) = &request.scope_lease {
                self.active_scopes.insert(scope.clone());
            }
            if let Some(quota) = &request.quota_authority {
                *self.active_quotas.entry(quota.clone()).or_default() += 1;
            }
            let id = self.next_id;
            self.next_id = self.next_id.saturating_add(1);
            self.active.insert(id, request.clone());
            return Some(AdmissionPermit { id, request });
        }
        None
    }

    pub fn release(&mut self, permit: AdmissionPermit) -> Result<()> {
        let request = self.active.get(&permit.id).ok_or_else(|| {
            CdfError::internal("scheduler admission permit was released more than once")
        })?;
        if *request != permit.request {
            return Err(CdfError::internal(
                "scheduler admission permit payload did not match active authority",
            ));
        }
        let request = self
            .active
            .remove(&permit.id)
            .expect("permit authority was checked immediately before removal");
        self.snapshot.active -= 1;
        self.snapshot.memory_bytes -= request.memory_bytes;
        self.snapshot.cpu_slots -= request.cpu_slots;
        self.snapshot.io_permits -= request.io_permits;
        self.snapshot.connection_permits -= request.connection_permits;
        if let Some(scope) = request.scope_lease {
            self.active_scopes.remove(&scope);
        }
        if let Some(quota) = request.quota_authority {
            let count = self.active_quotas.get_mut(&quota).ok_or_else(|| {
                CdfError::internal("scheduler quota usage was missing during release")
            })?;
            *count -= 1;
            if *count == 0 {
                self.active_quotas.remove(&quota);
            }
        }
        Ok(())
    }

    pub fn snapshot(&self) -> AdmissionSnapshot {
        self.snapshot.clone()
    }

    pub fn cancel(&mut self) -> Vec<AdmissionRequest> {
        self.cancelled = true;
        self.snapshot.cancelled = true;
        let mut cancelled = self
            .queues
            .values_mut()
            .flat_map(|queue| queue.drain(..))
            .collect::<Vec<_>>();
        cancelled.sort_by(|left, right| {
            left.resource
                .cmp(&right.resource)
                .then(left.ordinal.cmp(&right.ordinal))
        });
        self.queues.clear();
        self.rotation.clear();
        self.snapshot.queued = 0;
        cancelled
    }

    fn eligible(&self, request: &AdmissionRequest) -> bool {
        self.snapshot.active < usize::from(self.limits.jobs)
            && self
                .snapshot
                .memory_bytes
                .saturating_add(request.memory_bytes)
                <= self.limits.memory_bytes
            && self.snapshot.cpu_slots.saturating_add(request.cpu_slots) <= self.limits.cpu_slots
            && self.snapshot.io_permits.saturating_add(request.io_permits) <= self.limits.io_permits
            && self
                .snapshot
                .connection_permits
                .saturating_add(request.connection_permits)
                <= self.limits.connection_permits
            && request
                .scope_lease
                .as_ref()
                .is_none_or(|scope| !self.active_scopes.contains(scope))
            && request.quota_authority.as_ref().is_none_or(|quota| {
                self.active_quotas.get(quota).copied().unwrap_or(0)
                    < self
                        .limits
                        .quota_limits
                        .get(quota)
                        .copied()
                        .unwrap_or(u16::MAX)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(resource: &str, ordinal: u32, scope: Option<&str>) -> AdmissionRequest {
        AdmissionRequest {
            resource: resource.to_owned(),
            ordinal: CanonicalPartitionOrdinal::new(ordinal),
            memory_bytes: 10,
            cpu_slots: 1,
            io_permits: 1,
            connection_permits: 1,
            quota_authority: Some("shared-origin".to_owned()),
            scope_lease: scope.map(str::to_owned),
        }
    }

    #[test]
    fn effective_jobs_joins_every_ceiling_and_fails_small_memory() {
        let source = SourceExecutionCapabilities {
            minimum_poll_bytes: 10,
            maximum_poll_bytes: 100,
            minimum_decode_bytes: 10,
            maximum_decode_bytes: 100,
            maximum_concurrency: 12,
            useful_concurrency: 8,
            executor_class: SourceExecutorClass::Cpu,
            blocking_lane: None,
            pausable: true,
            spillable: true,
            idempotent_reads: true,
            reopenable: true,
            resumable: true,
            speculative_safe: true,
            retry_granularity: crate::SourceRetryGranularity::Partition,
            retryable_errors: vec![cdf_kernel::ErrorKind::Transient],
            retry_policy: Some(crate::SourceRetryPolicy::default()),
            attestation: crate::SourceAttestationStrength::ImmutableContent,
            rate_limit: Some(crate::SourceRateLimit {
                operations: 100,
                interval_ms: 1_000,
            }),
            quota_authority: Some("shared-origin".to_owned()),
            canonical_order: true,
            bounded: true,
            batch_memory: crate::SourceBatchMemoryContract::Preaccounted,
            telemetry_version: "v1".to_owned(),
        };
        let ceilings = AdmissionCeilings {
            configured_jobs: Some(7),
            container_cpu_slots: 16,
            managed_memory_bytes: 1_000,
            transport_connections: Some(5),
            destination_writers: Some(4),
            staged_destination_in_flight: None,
            lane_concurrency: None,
            scope_concurrency: Some(3),
        };
        let resolution = resolve_effective_jobs(100, &source, &ceilings).unwrap();
        assert_eq!(resolution.jobs, 3);
        assert_eq!(resolution.limiting_factors, vec!["checkpoint_scope"]);

        let mut too_small = ceilings;
        too_small.managed_memory_bytes = 199;
        assert!(resolve_effective_jobs(1, &source, &too_small).is_err());

        let mut unaccounted_retry = source.clone();
        unaccounted_retry.batch_memory = crate::SourceBatchMemoryContract::FrontierReserved;
        assert!(
            unaccounted_retry
                .validate()
                .unwrap_err()
                .message
                .contains("must preaccount")
        );

        let runtime = RuntimeSchedulerResolution {
            effective_jobs: EffectiveJobsResolution {
                jobs: 3,
                memory_jobs: 5,
                cpu_jobs: 16,
                limiting_factors: vec!["checkpoint_scope".to_owned()],
            },
            container_cpu_slots: 16,
            managed_memory_available_bytes: 1_000,
            source_maximum_concurrency: source.maximum_concurrency,
            source_useful_concurrency: source.useful_concurrency,
            source_lane_concurrency: None,
            source_rate_limit: source.rate_limit,
            source_quota_authority: source.quota_authority.clone(),
            source_bounded: source.bounded,
            transport_connection_limit: Some(5),
            destination_writer_concurrency: 4,
            destination_in_flight_segments: None,
        };
        runtime.validate_for_source(100, &source).unwrap();
        let mut stale = runtime.clone();
        stale.source_quota_authority = Some("other-origin".to_owned());
        assert!(stale.validate_for_source(100, &source).is_err());
        let mut stale = runtime.clone();
        stale.source_rate_limit = Some(crate::SourceRateLimit {
            operations: 101,
            interval_ms: 1_000,
        });
        assert!(stale.validate_for_source(100, &source).is_err());
        let mut stale = runtime.clone();
        stale.source_bounded = false;
        assert!(stale.validate_for_source(100, &source).is_err());

        let mut unit_only = source;
        unit_only.retry_granularity = crate::SourceRetryGranularity::Unit;
        assert!(
            compile_partition_retry(&unit_only, PartitionRetrySafety::Forbidden)
                .unwrap()
                .is_none()
        );
        assert!(
            compile_partition_retry(&unit_only, PartitionRetrySafety::ImmutableContent).is_err()
        );
    }

    #[test]
    fn effective_jobs_join_staged_destination_pressure_by_default_only() {
        let source = SourceExecutionCapabilities {
            minimum_poll_bytes: 10,
            maximum_poll_bytes: 100,
            minimum_decode_bytes: 10,
            maximum_decode_bytes: 100,
            maximum_concurrency: 64,
            useful_concurrency: 64,
            executor_class: SourceExecutorClass::Cpu,
            blocking_lane: None,
            pausable: true,
            spillable: true,
            idempotent_reads: true,
            reopenable: true,
            resumable: true,
            speculative_safe: true,
            retry_granularity: crate::SourceRetryGranularity::Partition,
            retryable_errors: vec![cdf_kernel::ErrorKind::Transient],
            retry_policy: Some(crate::SourceRetryPolicy::default()),
            attestation: crate::SourceAttestationStrength::ImmutableContent,
            rate_limit: None,
            quota_authority: None,
            canonical_order: true,
            bounded: true,
            batch_memory: crate::SourceBatchMemoryContract::Preaccounted,
            telemetry_version: "v1".to_owned(),
        };
        let defaulted = resolve_effective_jobs(
            12,
            &source,
            &AdmissionCeilings {
                configured_jobs: None,
                container_cpu_slots: 16,
                managed_memory_bytes: 10_000,
                transport_connections: None,
                destination_writers: None,
                staged_destination_in_flight: Some(2),
                lane_concurrency: None,
                scope_concurrency: None,
            },
        )
        .unwrap();
        assert_eq!(defaulted.jobs, 2);
        assert_eq!(
            defaulted.limiting_factors,
            vec!["staged_destination_in_flight"]
        );

        let explicit = resolve_effective_jobs(
            12,
            &source,
            &AdmissionCeilings {
                configured_jobs: Some(12),
                container_cpu_slots: 16,
                managed_memory_bytes: 10_000,
                transport_connections: None,
                destination_writers: None,
                staged_destination_in_flight: None,
                lane_concurrency: None,
                scope_concurrency: None,
            },
        )
        .unwrap();
        assert_eq!(explicit.jobs, 12);
        assert!(
            !explicit
                .limiting_factors
                .iter()
                .any(|factor| factor == "staged_destination_in_flight")
        );
    }

    #[test]
    fn decode_unit_concurrency_joins_unit_cpu_io_source_and_memory_bounds() {
        let units = (0..20)
            .map(|ordinal| DecodeUnitPlan {
                unit_id: format!("row-group-{ordinal}"),
                ordinal,
                extent: None,
                estimated_working_set_bytes: 16,
                independently_retryable: true,
            })
            .collect::<Vec<_>>();
        let host = ExecutionHostCapabilities {
            logical_cpu_slots: 12,
            io_workers: 4,
            blocking_lanes: Vec::new(),
        };
        let cpu = crate::CpuTaskSpec {
            task_kind: "format.test.decode".to_owned(),
            cpu_slot_cost: 1,
            native_internal_parallelism: 2,
        };
        let resolution =
            resolve_decode_unit_concurrency(&units, &host, &cpu, 160, 8, 16, 2).unwrap();
        assert_eq!(resolution.estimated_bytes_per_job, 48);
        assert_eq!(resolution.memory_jobs, 3);
        assert_eq!(resolution.cpu_jobs, 6);
        assert_eq!(resolution.jobs, 3);
        assert_eq!(resolution.limiting_factors, vec!["managed_memory"]);
        let cpu_limited =
            resolve_decode_unit_concurrency(&units, &host, &cpu, 4_800, 8, 16, 2).unwrap();
        assert_eq!(cpu_limited.jobs, 6);
        assert_eq!(cpu_limited.limiting_factors, vec!["container_cpu"]);
        assert!(resolve_decode_unit_concurrency(&units, &host, &cpu, 47, 8, 16, 2).is_err());
    }

    #[test]
    fn container_cpu_authority_honors_fractional_and_unlimited_cgroups() {
        assert_eq!(effective_container_cpu_slots(16, None).unwrap(), 16);
        assert_eq!(
            effective_container_cpu_slots(16, Some("max 100000")).unwrap(),
            16
        );
        assert_eq!(
            effective_container_cpu_slots(16, Some("150000 100000")).unwrap(),
            2
        );
        assert_eq!(
            effective_container_cpu_slots(16, Some("50000 100000")).unwrap(),
            1
        );
        assert!(effective_container_cpu_slots(16, Some("0 100000")).is_err());
    }

    #[test]
    fn fair_admission_enforces_all_resources_quotas_and_scope_leases() {
        let mut controller = FairAdmissionController::new(AdmissionLimits {
            jobs: 3,
            memory_bytes: 30,
            cpu_slots: 3,
            io_permits: 3,
            connection_permits: 3,
            quota_limits: BTreeMap::from([("shared-origin".to_owned(), 2)]),
        })
        .unwrap();
        controller
            .enqueue(request("large", 0, Some("scope-a")))
            .unwrap();
        controller
            .enqueue(request("large", 1, Some("scope-b")))
            .unwrap();
        controller
            .enqueue(request("small", 0, Some("scope-c")))
            .unwrap();
        controller
            .enqueue(request("small", 1, Some("scope-a")))
            .unwrap();

        let first = controller.try_admit_next().unwrap();
        let second = controller.try_admit_next().unwrap();
        assert_eq!(first.request.resource, "large");
        assert_eq!(second.request.resource, "small");
        assert_ne!(first.request.scope_lease, second.request.scope_lease);
        assert!(controller.try_admit_next().is_none());
        let snapshot = controller.snapshot();
        assert_eq!(snapshot.active, 2);
        assert_eq!(snapshot.memory_bytes, 20);

        controller.release(first).unwrap();
        let third = controller.try_admit_next().unwrap();
        assert_eq!(third.request.resource, "large");
        controller.release(second).unwrap();
        controller.release(third).unwrap();
        assert_eq!(controller.snapshot().active, 0);
    }

    #[test]
    fn blocked_head_does_not_starve_an_independent_resource() {
        let mut controller = FairAdmissionController::new(AdmissionLimits {
            jobs: 2,
            memory_bytes: 20,
            cpu_slots: 2,
            io_permits: 2,
            connection_permits: 2,
            quota_limits: BTreeMap::new(),
        })
        .unwrap();
        controller.enqueue(request("a", 0, Some("same"))).unwrap();
        let active = controller.try_admit_next().unwrap();
        controller.enqueue(request("a", 1, Some("same"))).unwrap();
        controller.enqueue(request("b", 0, Some("other"))).unwrap();
        let independent = controller.try_admit_next().unwrap();
        assert_eq!(independent.request.resource, "b");
        controller.release(active).unwrap();
        controller.release(independent).unwrap();
    }

    #[test]
    fn cancellation_is_canonical_and_prevents_new_admission() {
        let mut controller = FairAdmissionController::new(AdmissionLimits {
            jobs: 2,
            memory_bytes: 20,
            cpu_slots: 2,
            io_permits: 2,
            connection_permits: 2,
            quota_limits: BTreeMap::new(),
        })
        .unwrap();
        controller.enqueue(request("z", 2, None)).unwrap();
        controller.enqueue(request("a", 3, None)).unwrap();
        controller.enqueue(request("a", 1, None)).unwrap();

        let cancelled = controller.cancel();
        assert_eq!(
            cancelled
                .iter()
                .map(|request| (request.resource.as_str(), request.ordinal.get()))
                .collect::<Vec<_>>(),
            vec![("a", 1), ("a", 3), ("z", 2)]
        );
        assert!(controller.snapshot().cancelled);
        assert_eq!(controller.snapshot().queued, 0);
        assert!(controller.try_admit_next().is_none());
        assert!(controller.enqueue(request("new", 0, None)).is_err());
    }

    #[test]
    fn cancellation_preserves_active_permits_until_join_release() {
        let mut controller = FairAdmissionController::new(AdmissionLimits {
            jobs: 1,
            memory_bytes: 10,
            cpu_slots: 1,
            io_permits: 1,
            connection_permits: 1,
            quota_limits: BTreeMap::new(),
        })
        .unwrap();
        controller.enqueue(request("a", 0, None)).unwrap();
        controller.enqueue(request("a", 1, None)).unwrap();
        let active = controller.try_admit_next().unwrap();

        assert_eq!(controller.cancel().len(), 1);
        assert_eq!(controller.snapshot().active, 1);
        controller.release(active).unwrap();
        assert_eq!(controller.snapshot().active, 0);
        assert!(controller.snapshot().cancelled);
    }

    #[test]
    fn invalid_release_cannot_leak_active_capacity() {
        let mut controller = FairAdmissionController::new(AdmissionLimits {
            jobs: 1,
            memory_bytes: 10,
            cpu_slots: 1,
            io_permits: 1,
            connection_permits: 1,
            quota_limits: BTreeMap::new(),
        })
        .unwrap();
        controller.enqueue(request("a", 0, None)).unwrap();
        let permit = controller.try_admit_next().unwrap();
        let mut corrupted = permit.clone();
        corrupted.request.memory_bytes = 9;

        assert!(controller.release(corrupted).is_err());
        assert_eq!(controller.snapshot().active, 1);
        controller.release(permit).unwrap();
        assert_eq!(controller.snapshot(), AdmissionSnapshot::default());
    }
}
