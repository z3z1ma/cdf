use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use cdf_kernel::{CdfError, ErrorKind, Result};
use serde::{Deserialize, Serialize};

use crate::{
    ExecutionServices, RunCancellation, SourceAttestationStrength, SourceExecutionCapabilities,
    SourceRetryGranularity, SourceRetryPolicy,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompiledSourceRetry {
    pub granularity: SourceRetryGranularity,
    pub retryable_errors: Vec<ErrorKind>,
    pub policy: SourceRetryPolicy,
    pub attestation: SourceAttestationStrength,
    pub resumable: bool,
}

impl CompiledSourceRetry {
    pub fn from_capabilities(capabilities: &SourceExecutionCapabilities) -> Result<Option<Self>> {
        capabilities.validate()?;
        if capabilities.retry_granularity == SourceRetryGranularity::None {
            return Ok(None);
        }
        Ok(Some(Self {
            granularity: capabilities.retry_granularity,
            retryable_errors: capabilities.retryable_errors.clone(),
            policy: capabilities.retry_policy.clone().ok_or_else(|| {
                CdfError::internal("validated retryable source omitted its retry policy")
            })?,
            attestation: capabilities.attestation,
            resumable: capabilities.resumable,
        }))
    }

    pub fn validate(&self) -> Result<()> {
        if self.granularity == SourceRetryGranularity::None || self.retryable_errors.is_empty() {
            return Err(CdfError::contract(
                "compiled source retry requires a concrete granularity and typed errors",
            ));
        }
        if !matches!(
            self.attestation,
            SourceAttestationStrength::ImmutableContent | SourceAttestationStrength::Snapshot
        ) {
            return Err(CdfError::contract(
                "compiled source retry requires immutable-content or snapshot reattestation authority",
            ));
        }
        if self
            .retryable_errors
            .iter()
            .any(|kind| !matches!(kind, ErrorKind::Transient | ErrorKind::RateLimited))
        {
            return Err(CdfError::contract(
                "compiled source retry may contain only transient or rate-limited errors",
            ));
        }
        self.policy.validate()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceRetryExhaustion {
    Ineligible,
    AttemptLimit,
    ElapsedDeadline,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRetryHistoryEntry {
    pub failed_attempt: u16,
    pub cause: ErrorKind,
    pub selected_delay_ms: Option<u64>,
    pub exhaustion: Option<SourceRetryExhaustion>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRetryEvidence {
    plan_id: String,
    schedule_identity_hash: String,
    partition_ordinal: u32,
    partition_id: String,
    immutable_identity_hash: String,
    partition_binding_hash: String,
    compiled_retry: CompiledSourceRetry,
    history: Vec<SourceRetryHistoryEntry>,
}

impl SourceRetryEvidence {
    pub fn validate(&self) -> Result<()> {
        self.compiled_retry.validate()?;
        let history_shape_is_valid = self.history.iter().enumerate().all(|(index, entry)| {
            let is_last = index + 1 == self.history.len();
            let selected_or_exhausted =
                entry.selected_delay_ms.is_some() || entry.exhaustion.is_some();
            let retryable_cause =
                matches!(entry.cause, ErrorKind::Transient | ErrorKind::RateLimited);
            let terminal_ineligible_cause = is_last
                && entry.exhaustion == Some(SourceRetryExhaustion::Ineligible)
                && entry.selected_delay_ms.is_none();
            selected_or_exhausted
                && (is_last || entry.exhaustion.is_none())
                && (retryable_cause || terminal_ineligible_cause)
        });
        let selected_delay_sum = self.history.iter().try_fold(0_u64, |total, entry| {
            total.checked_add(entry.selected_delay_ms.unwrap_or(0))
        });
        let policy = &self.compiled_retry.policy;
        let history_matches_policy = self.history.iter().all(|entry| {
            let cause_is_retryable = self.compiled_retry.retryable_errors.contains(&entry.cause);
            let terminal_ineligible =
                entry.exhaustion == Some(SourceRetryExhaustion::Ineligible) && !cause_is_retryable;
            entry.failed_attempt <= policy.max_total_attempts
                && (cause_is_retryable || terminal_ineligible)
                && entry
                    .selected_delay_ms
                    .is_none_or(|delay| delay < policy.max_elapsed_ms)
                && match entry.exhaustion {
                    Some(SourceRetryExhaustion::AttemptLimit) => {
                        cause_is_retryable && entry.failed_attempt == policy.max_total_attempts
                    }
                    Some(SourceRetryExhaustion::ElapsedDeadline) => cause_is_retryable,
                    Some(SourceRetryExhaustion::Ineligible) => terminal_ineligible,
                    None => cause_is_retryable && entry.failed_attempt < policy.max_total_attempts,
                }
        });
        if self.plan_id.is_empty()
            || self.plan_id.chars().any(char::is_control)
            || self.schedule_identity_hash.is_empty()
            || self.partition_id.is_empty()
            || self.partition_id.chars().any(char::is_control)
            || self.immutable_identity_hash.is_empty()
            || self.partition_binding_hash.is_empty()
            || self.history.is_empty()
            || self.history[0].failed_attempt != 1
            || self
                .history
                .windows(2)
                .any(|pair| pair[0].failed_attempt.saturating_add(1) != pair[1].failed_attempt)
            || !history_shape_is_valid
            || !history_matches_policy
            || selected_delay_sum.is_none_or(|total| total >= policy.max_elapsed_ms)
        {
            return Err(CdfError::data(
                "source retry evidence requires a safe partition id and contiguous typed failure history with delays or terminal ineligible/exhaustion evidence",
            ));
        }
        Ok(())
    }

    pub fn validate_against_schedule(
        &self,
        schedule: &crate::CanonicalPartitionSchedule,
    ) -> Result<()> {
        self.validate()?;
        if self.plan_id != schedule.plan_id {
            return Err(CdfError::data(
                "source retry evidence plan identity does not match its partition schedule",
            ));
        }
        if !schedule.contains_runtime_binding(self.partition_ordinal, &self.schedule_identity_hash)
            || schedule.admission.retry.as_ref() != Some(&self.compiled_retry)
        {
            return Err(CdfError::data(
                "source retry evidence does not match its compiled partition retry binding",
            ));
        }
        if let Some(partitions) = schedule.inline_partitions() {
            let scheduled = partitions
                .get(usize::try_from(self.partition_ordinal).map_err(|_| {
                    CdfError::data("source retry evidence partition ordinal exceeds usize")
                })?)
                .ok_or_else(|| {
                    CdfError::data("source retry evidence references an absent partition ordinal")
                })?;
            if scheduled.ordinal.get() != self.partition_ordinal
                || scheduled.partition.partition_id.as_str() != self.partition_id
                || scheduled.immutable_identity_hash != self.immutable_identity_hash
            {
                return Err(CdfError::data(
                    "source retry evidence does not match its inline partition authority",
                ));
            }
        }
        Ok(())
    }

    pub fn plan_id(&self) -> &str {
        &self.plan_id
    }

    pub fn partition_ordinal(&self) -> u32 {
        self.partition_ordinal
    }

    pub fn partition_id(&self) -> &str {
        &self.partition_id
    }

    pub fn immutable_identity_hash(&self) -> &str {
        &self.immutable_identity_hash
    }

    pub fn partition_binding_hash(&self) -> &str {
        &self.partition_binding_hash
    }

    pub fn compiled_retry(&self) -> &CompiledSourceRetry {
        &self.compiled_retry
    }

    pub fn history(&self) -> &[SourceRetryHistoryEntry] {
        &self.history
    }
}

/// Shared runtime-only evidence sink that survives both successful and failed engine returns.
#[derive(Clone, Debug, Default)]
pub struct SourceRetryJournal(Arc<Mutex<BTreeMap<u32, SourceRetryEvidence>>>);

/// Read-only evidence handle retained by an embedding caller while the engine owns mutation.
#[derive(Clone, Debug, Default)]
pub struct SourceRetryEvidenceView(SourceRetryJournal);

impl SourceRetryEvidenceView {
    pub fn snapshot(&self) -> Result<Vec<SourceRetryEvidence>> {
        self.0.snapshot()
    }
}

impl SourceRetryJournal {
    pub fn evidence_view(&self) -> SourceRetryEvidenceView {
        SourceRetryEvidenceView(self.clone())
    }

    pub fn record(
        &self,
        plan_id: &str,
        partition: &crate::ScheduledPartition,
        history: &[SourceRetryHistoryEntry],
    ) -> Result<()> {
        if history.is_empty() {
            return Ok(());
        }
        let evidence = SourceRetryEvidence {
            plan_id: plan_id.to_owned(),
            schedule_identity_hash: partition.schedule_identity_hash.clone(),
            partition_ordinal: partition.ordinal.get(),
            partition_id: partition.partition.partition_id.to_string(),
            immutable_identity_hash: partition.immutable_identity_hash.clone(),
            partition_binding_hash: crate::artifact_hash(partition)?,
            compiled_retry: partition.retry.clone().ok_or_else(|| {
                CdfError::internal("retry journal received a partition without compiled retry")
            })?,
            history: history.to_vec(),
        };
        evidence.validate()?;
        let mut entries = self
            .0
            .lock()
            .map_err(|_| CdfError::internal("source retry journal lock poisoned"))?;
        if let Some(existing) = entries.get(&evidence.partition_ordinal)
            && (existing.plan_id != evidence.plan_id
                || existing.schedule_identity_hash != evidence.schedule_identity_hash
                || existing.partition_id != evidence.partition_id
                || existing.immutable_identity_hash != evidence.immutable_identity_hash
                || existing.partition_binding_hash != evidence.partition_binding_hash
                || existing.compiled_retry != evidence.compiled_retry)
        {
            return Err(CdfError::internal(
                "source retry journal ordinal changed its compiled partition binding",
            ));
        }
        if let Some(existing) = entries.get(&evidence.partition_ordinal)
            && !evidence.history.starts_with(&existing.history)
        {
            return Err(CdfError::internal(
                "source retry journal history may only extend its existing exact prefix",
            ));
        }
        entries.insert(evidence.partition_ordinal, evidence);
        Ok(())
    }

    pub fn snapshot(&self) -> Result<Vec<SourceRetryEvidence>> {
        Ok(self
            .0
            .lock()
            .map_err(|_| CdfError::internal("source retry journal lock poisoned"))?
            .values()
            .cloned()
            .collect())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceRetryDecision {
    Retry { next_attempt: u16, delay_ms: u64 },
    GiveUp { reason: SourceRetryExhaustion },
}

/// Scheduler-owned budget and delay state for one planned retry unit.
///
/// The caller retains atomic attempt and reattestation authority. This state
/// owns only typed eligibility, elapsed/attempt budgets, jitter, delay, and
/// runtime history; none of those values enter package identity.
pub struct SourceRetryState {
    compiled: CompiledSourceRetry,
    policy: SourceRetryPolicy,
    execution: ExecutionServices,
    started_at: Duration,
    attempt: u16,
    history: Vec<SourceRetryHistoryEntry>,
}

impl SourceRetryState {
    pub fn new(
        compiled: &CompiledSourceRetry,
        narrowed_policy: Option<SourceRetryPolicy>,
        execution: ExecutionServices,
    ) -> Result<Self> {
        compiled.validate()?;
        let source_policy = &compiled.policy;
        let policy = match narrowed_policy {
            Some(policy) => {
                policy.validate()?;
                if policy.max_total_attempts > source_policy.max_total_attempts
                    || policy.max_elapsed_ms > source_policy.max_elapsed_ms
                    || policy.base_delay_ms != source_policy.base_delay_ms
                    || policy.max_delay_ms != source_policy.max_delay_ms
                {
                    return Err(CdfError::contract(
                        "compiled retry policy may only lower source attempt/deadline ceilings",
                    ));
                }
                policy
            }
            None => source_policy.clone(),
        };
        let started_at = execution.monotonic_now();
        Ok(Self {
            compiled: compiled.clone(),
            policy,
            execution,
            started_at,
            attempt: 1,
            history: Vec::new(),
        })
    }

    pub fn current_attempt(&self) -> u16 {
        self.attempt
    }

    pub fn history(&self) -> &[SourceRetryHistoryEntry] {
        &self.history
    }

    pub fn decide_after_failure(&mut self, error: &CdfError) -> Result<SourceRetryDecision> {
        if !matches!(error.kind, ErrorKind::Transient | ErrorKind::RateLimited)
            || !self.compiled.retryable_errors.contains(&error.kind)
        {
            return Ok(self.give_up(error, SourceRetryExhaustion::Ineligible));
        }
        if self.attempt >= self.policy.max_total_attempts {
            return Ok(self.give_up(error, SourceRetryExhaustion::AttemptLimit));
        }
        let elapsed = self.elapsed()?;
        let deadline = Duration::from_millis(self.policy.max_elapsed_ms);
        let remaining = deadline.saturating_sub(elapsed);
        if remaining.is_zero() {
            return Ok(self.give_up(error, SourceRetryExhaustion::ElapsedDeadline));
        }

        let failure_index = u32::from(self.attempt.saturating_sub(1)).min(63);
        let exponential_cap = self
            .policy
            .base_delay_ms
            .saturating_mul(1_u64 << failure_index)
            .min(self.policy.max_delay_ms);
        let jitter = uniform_inclusive(&self.execution, exponential_cap);
        let delay_ms = jitter.max(error.retry_after_ms.unwrap_or(0));
        if Duration::from_millis(delay_ms) >= remaining {
            return Ok(self.give_up(error, SourceRetryExhaustion::ElapsedDeadline));
        }
        let next_attempt = self.attempt.saturating_add(1);
        self.history.push(SourceRetryHistoryEntry {
            failed_attempt: self.attempt,
            cause: error.kind.clone(),
            selected_delay_ms: Some(delay_ms),
            exhaustion: None,
        });
        Ok(SourceRetryDecision::Retry {
            next_attempt,
            delay_ms,
        })
    }

    pub async fn wait_for_retry(
        &mut self,
        decision: SourceRetryDecision,
        cancellation: RunCancellation,
    ) -> Result<bool> {
        let SourceRetryDecision::Retry {
            next_attempt,
            delay_ms,
        } = decision
        else {
            return Ok(false);
        };
        if next_attempt != self.attempt.saturating_add(1) {
            return Err(CdfError::internal(
                "retry decision does not advance the current attempt exactly once",
            ));
        }
        self.execution
            .delay(Duration::from_millis(delay_ms), cancellation)
            .await?;
        if self.elapsed()? >= Duration::from_millis(self.policy.max_elapsed_ms) {
            if let Some(entry) = self.history.last_mut() {
                entry.exhaustion = Some(SourceRetryExhaustion::ElapsedDeadline);
            }
            return Ok(false);
        }
        self.attempt = next_attempt;
        Ok(true)
    }

    fn elapsed(&self) -> Result<Duration> {
        self.execution
            .monotonic_now()
            .checked_sub(self.started_at)
            .ok_or_else(|| CdfError::internal("execution monotonic clock moved backwards"))
    }

    fn give_up(&mut self, error: &CdfError, reason: SourceRetryExhaustion) -> SourceRetryDecision {
        self.history.push(SourceRetryHistoryEntry {
            failed_attempt: self.attempt,
            cause: error.kind.clone(),
            selected_delay_ms: None,
            exhaustion: Some(reason),
        });
        SourceRetryDecision::GiveUp { reason }
    }
}

fn uniform_inclusive(execution: &ExecutionServices, inclusive_max: u64) -> u64 {
    if inclusive_max == 0 {
        return 0;
    }
    if inclusive_max == u64::MAX {
        return execution.entropy_u64();
    }
    let bound = inclusive_max + 1;
    let uniform_zone = u64::MAX - (u64::MAX % bound);
    loop {
        let entropy = execution.entropy_u64();
        if entropy < uniform_zone {
            return entropy % bound;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    use cdf_kernel::{
        CompiledScanIntent, PartitionId, PartitionPlan, PartitionRetrySafety, ScopeKey,
    };
    use cdf_memory::{DeterministicMemoryCoordinator, MemoryCoordinator};

    use super::*;
    use crate::{
        BlockingLaneSpec, BlockingValueTask, ExecutionHost, ExecutionHostCapabilities,
        ExecutionTaskScope, FixedSpillBudget, IoValue, IoValueTask, SourceAttestationStrength,
        SourceExecutorClass, SpillBudgetCoordinator,
    };

    struct RetryHost {
        now_ms: Arc<AtomicU64>,
        entropy: Mutex<VecDeque<u64>>,
        delays: Arc<Mutex<Vec<u64>>>,
        memory: Arc<dyn MemoryCoordinator>,
        spill: Arc<dyn SpillBudgetCoordinator>,
    }

    impl RetryHost {
        fn new(entropy: impl IntoIterator<Item = u64>) -> Result<Self> {
            Ok(Self {
                now_ms: Arc::new(AtomicU64::new(0)),
                entropy: Mutex::new(entropy.into_iter().collect()),
                delays: Arc::new(Mutex::new(Vec::new())),
                memory: Arc::new(DeterministicMemoryCoordinator::new(
                    1024 * 1024,
                    Default::default(),
                )?),
                spill: Arc::new(FixedSpillBudget::new(1024 * 1024)?),
            })
        }
    }

    impl ExecutionHost for RetryHost {
        fn capabilities(&self) -> ExecutionHostCapabilities {
            ExecutionHostCapabilities {
                logical_cpu_slots: 1,
                io_workers: 1,
                blocking_lanes: Vec::new(),
            }
        }

        fn memory(&self) -> Arc<dyn MemoryCoordinator> {
            Arc::clone(&self.memory)
        }

        fn spill(&self) -> Arc<dyn SpillBudgetCoordinator> {
            Arc::clone(&self.spill)
        }

        fn open_scope(&self, _run_id: &str) -> Result<Box<dyn ExecutionTaskScope>> {
            panic!("retry policy test does not open task scopes")
        }

        fn run_io_blocking(&self, _task: IoValueTask) -> Result<IoValue> {
            panic!("retry policy test does not execute I/O")
        }

        fn delay(
            &self,
            duration: Duration,
            cancellation: RunCancellation,
        ) -> cdf_kernel::BoxFuture<'static, Result<()>> {
            let now_ms = Arc::clone(&self.now_ms);
            let delays = Arc::clone(&self.delays);
            Box::pin(async move {
                cancellation.check()?;
                let delay_ms = u64::try_from(duration.as_millis())
                    .map_err(|_| CdfError::internal("test delay exceeds u64"))?;
                delays.lock().unwrap().push(delay_ms);
                now_ms.fetch_add(delay_ms, Ordering::SeqCst);
                cancellation.check()
            })
        }

        fn monotonic_now(&self) -> Duration {
            Duration::from_millis(self.now_ms.load(Ordering::SeqCst))
        }

        fn unix_now(&self) -> Duration {
            self.monotonic_now()
        }

        fn entropy_u64(&self) -> u64 {
            self.entropy.lock().unwrap().pop_front().unwrap_or(0)
        }

        fn ensure_blocking_lanes(&self, _lanes: &[BlockingLaneSpec]) -> Result<()> {
            Ok(())
        }

        fn run_blocking_value(&self, _lane: &str, _task: BlockingValueTask) -> Result<IoValue> {
            panic!("retry policy test does not execute blocking work")
        }
    }

    fn capabilities() -> SourceExecutionCapabilities {
        SourceExecutionCapabilities {
            minimum_poll_bytes: 1,
            maximum_poll_bytes: 1,
            minimum_decode_bytes: 1,
            maximum_decode_bytes: 1,
            maximum_concurrency: 1,
            useful_concurrency: 1,
            executor_class: SourceExecutorClass::Io,
            blocking_lane: None,
            pausable: true,
            spillable: false,
            idempotent_reads: true,
            reopenable: true,
            resumable: false,
            speculative_safe: false,
            retry_granularity: SourceRetryGranularity::Partition,
            retryable_errors: vec![ErrorKind::Transient, ErrorKind::RateLimited],
            retry_policy: Some(SourceRetryPolicy::default()),
            attestation: SourceAttestationStrength::ImmutableContent,
            rate_limit: None,
            quota_authority: None,
            canonical_order: true,
            bounded: true,
            batch_memory: crate::SourceBatchMemoryContract::Preaccounted,
            telemetry_version: "v1".to_owned(),
        }
    }

    fn compiled_retry() -> CompiledSourceRetry {
        CompiledSourceRetry::from_capabilities(&capabilities())
            .unwrap()
            .unwrap()
    }

    fn scheduled_partition() -> crate::ScheduledPartition {
        let partition_id = PartitionId::new("partition-0").unwrap();
        crate::ScheduledPartition {
            ordinal: crate::CanonicalPartitionOrdinal::new(0),
            partition: PartitionPlan {
                partition_id: partition_id.clone(),
                scope: ScopeKey::Partition { partition_id },
                planned_position: None,
                start_position: None,
                scan_intent: CompiledScanIntent::full_scan(),
                retry_safety: PartitionRetrySafety::ImmutableContent,
                metadata: BTreeMap::new(),
            },
            immutable_identity_hash: "sha256:partition-0".to_owned(),
            schedule_identity_hash: "sha256:schedule-0".to_owned(),
            minimum_working_set_bytes: 2,
            maximum_working_set_bytes: 2,
            executor_class: SourceExecutorClass::Io,
            retry: Some(compiled_retry()),
            rate_limit: None,
            quota_authority: None,
            speculative_safe: false,
            canonical_order: true,
            bounded_source: true,
        }
    }

    #[test]
    fn retry_evidence_is_compiled_policy_and_schedule_bound() {
        let scheduled = scheduled_partition();
        let schedule = crate::CanonicalPartitionSchedule {
            plan_id: "plan-0".to_owned(),
            schedule_identity_hash: scheduled.schedule_identity_hash.clone(),
            admission: crate::PartitionAdmissionTemplate {
                minimum_working_set_bytes: scheduled.minimum_working_set_bytes,
                maximum_working_set_bytes: scheduled.maximum_working_set_bytes,
                executor_class: scheduled.executor_class,
                retry: scheduled.retry.clone(),
                rate_limit: scheduled.rate_limit,
                quota_authority: scheduled.quota_authority.clone(),
                speculative_safe: scheduled.speculative_safe,
                canonical_order: scheduled.canonical_order,
                bounded_source: scheduled.bounded_source,
            },
            authority: crate::PartitionScheduleAuthority::Inline {
                partitions: vec![crate::CanonicalPartitionBinding {
                    ordinal: scheduled.ordinal,
                    partition: scheduled.partition.clone(),
                    immutable_identity_hash: scheduled.immutable_identity_hash.clone(),
                }],
            },
        };
        let journal = SourceRetryJournal::default();
        journal
            .record(
                &schedule.plan_id,
                &scheduled,
                &[SourceRetryHistoryEntry {
                    failed_attempt: 1,
                    cause: ErrorKind::Transient,
                    selected_delay_ms: Some(25),
                    exhaustion: None,
                }],
            )
            .unwrap();
        let evidence = journal.snapshot().unwrap().pop().unwrap();
        evidence.validate_against_schedule(&schedule).unwrap();

        assert!(
            journal
                .record(
                    &schedule.plan_id,
                    &scheduled,
                    &[SourceRetryHistoryEntry {
                        failed_attempt: 1,
                        cause: ErrorKind::RateLimited,
                        selected_delay_ms: Some(25),
                        exhaustion: None,
                    }],
                )
                .is_err(),
            "a valid but divergent history must not replace observed evidence"
        );

        let mut widened = schedule.clone();
        widened
            .admission
            .retry
            .as_mut()
            .unwrap()
            .policy
            .max_total_attempts = 4;
        assert!(evidence.validate_against_schedule(&widened).is_err());

        let excessive = (1..=4)
            .map(|failed_attempt| SourceRetryHistoryEntry {
                failed_attempt,
                cause: ErrorKind::Transient,
                selected_delay_ms: (failed_attempt < 4).then_some(1),
                exhaustion: (failed_attempt == 4).then_some(SourceRetryExhaustion::AttemptLimit),
            })
            .collect::<Vec<_>>();
        assert!(
            SourceRetryJournal::default()
                .record(&schedule.plan_id, &scheduled, &excessive)
                .is_err()
        );
    }

    #[test]
    fn exact_default_budget_uses_full_jitter_retry_after_and_attempt_limit() {
        let host = Arc::new(RetryHost::new([50, 150]).unwrap());
        let delays = Arc::clone(&host.delays);
        let host_contract: Arc<dyn ExecutionHost> = host;
        let execution = ExecutionServices::new(host_contract).unwrap();
        let mut state = SourceRetryState::new(&compiled_retry(), None, execution).unwrap();

        let first = state
            .decide_after_failure(&CdfError::rate_limited("slow", Some(400)))
            .unwrap();
        assert_eq!(
            first,
            SourceRetryDecision::Retry {
                next_attempt: 2,
                delay_ms: 400,
            }
        );
        assert!(
            futures_executor::block_on(state.wait_for_retry(first, RunCancellation::default()))
                .unwrap()
        );

        let second = state
            .decide_after_failure(&CdfError::transient("again"))
            .unwrap();
        assert_eq!(
            second,
            SourceRetryDecision::Retry {
                next_attempt: 3,
                delay_ms: 150,
            }
        );
        assert!(
            futures_executor::block_on(state.wait_for_retry(second, RunCancellation::default()))
                .unwrap()
        );

        assert_eq!(
            state
                .decide_after_failure(&CdfError::transient("terminal"))
                .unwrap(),
            SourceRetryDecision::GiveUp {
                reason: SourceRetryExhaustion::AttemptLimit,
            }
        );
        assert_eq!(*delays.lock().unwrap(), vec![400, 150]);
        assert_eq!(state.current_attempt(), 3);
        assert_eq!(state.history().len(), 3);
    }

    #[test]
    fn deadline_and_typed_eligibility_fail_closed() {
        let host = Arc::new(RetryHost::new([0]).unwrap());
        let host_contract: Arc<dyn ExecutionHost> = host.clone();
        let execution = ExecutionServices::new(host_contract).unwrap();
        let mut state = SourceRetryState::new(&compiled_retry(), None, execution).unwrap();

        assert_eq!(
            state
                .decide_after_failure(&CdfError::data("bad row"))
                .unwrap(),
            SourceRetryDecision::GiveUp {
                reason: SourceRetryExhaustion::Ineligible,
            }
        );
        assert_eq!(
            state.history()[0].exhaustion,
            Some(SourceRetryExhaustion::Ineligible)
        );

        host.now_ms.store(29_950, Ordering::SeqCst);
        assert_eq!(
            state
                .decide_after_failure(&CdfError::rate_limited("wait", Some(100)))
                .unwrap(),
            SourceRetryDecision::GiveUp {
                reason: SourceRetryExhaustion::ElapsedDeadline,
            }
        );
    }

    #[test]
    fn cancellation_during_retry_delay_prevents_the_next_attempt() {
        let host = Arc::new(RetryHost::new([50]).unwrap());
        let delays = Arc::clone(&host.delays);
        let host_contract: Arc<dyn ExecutionHost> = host;
        let execution = ExecutionServices::new(host_contract).unwrap();
        let mut state = SourceRetryState::new(&compiled_retry(), None, execution).unwrap();
        let decision = state
            .decide_after_failure(&CdfError::transient("cancel me"))
            .unwrap();
        let cancellation = RunCancellation::default();
        cancellation.cancel();

        let error =
            futures_executor::block_on(state.wait_for_retry(decision, cancellation)).unwrap_err();

        assert!(error.message.contains("cancelled"));
        assert_eq!(state.current_attempt(), 1);
        assert!(delays.lock().unwrap().is_empty());
    }

    #[test]
    fn compiled_policy_can_only_narrow_source_ceiling() {
        let source = SourceRetryPolicy::default();
        assert_eq!(
            source.narrow(Some(2), Some(5_000)).unwrap(),
            SourceRetryPolicy {
                max_total_attempts: 2,
                max_elapsed_ms: 5_000,
                ..source.clone()
            }
        );

        let host: Arc<dyn ExecutionHost> = Arc::new(RetryHost::new([]).unwrap());
        let widened = SourceRetryPolicy {
            max_total_attempts: 4,
            ..source
        };
        assert!(
            SourceRetryState::new(
                &compiled_retry(),
                Some(widened),
                ExecutionServices::new(host).unwrap(),
            )
            .is_err()
        );

        let mut metadata_only = capabilities();
        metadata_only.attestation = SourceAttestationStrength::Metadata;
        assert!(metadata_only.validate().is_err());
    }
}
