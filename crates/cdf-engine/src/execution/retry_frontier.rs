//! Source retry and canonical-frontier ownership.

use cdf_kernel::{CdfError, Result};

use crate::{EngineExecutionInvocation, EnginePlan};

pub(super) fn source_frontier_batch_bound(plan: &EnginePlan, partition_count: u64) -> Result<u64> {
    let schedule = plan.partition_schedule.as_ref().ok_or_else(|| {
        CdfError::contract("package execution requires a compiled partition schedule")
    })?;
    if partition_count != 0 && schedule.partition_count() != partition_count {
        return Err(CdfError::contract(
            "source frontier schedule does not cover every executable partition",
        ));
    }
    if schedule.admission.maximum_working_set_bytes == 0 {
        return Err(CdfError::contract(
            "source frontier partition requires a nonzero working-set bound",
        ));
    }
    let maximum_batch_bytes = plan
        .compiled_source_execution
        .as_ref()
        .ok_or_else(|| {
            CdfError::contract("package execution requires a compiled source execution plan")
        })?
        .execution_capabilities()
        .maximum_emitted_batch_bytes;
    // The schedule owns admission for transport plus decode. The frontier retains only the
    // decoded batch crossing the source edge, so charging the schedule total here would count
    // transport memory twice.
    Ok(maximum_batch_bytes)
}

pub(crate) fn partition_open_jobs(plan: &EnginePlan, options: &EngineExecutionInvocation) -> usize {
    let partition_count = plan.partition_schedule.as_ref().map_or_else(
        || plan.scan.partition_count().unwrap_or(u64::MAX),
        cdf_runtime::CanonicalPartitionSchedule::partition_count,
    );
    if partition_count <= 1 {
        return usize::try_from(partition_count.max(1)).unwrap_or(1);
    }
    let (Some(_graph), Some(_schedule), Some(scheduler)) = (
        plan.operator_graph.as_ref(),
        plan.partition_schedule.as_ref(),
        options.scheduler.as_ref(),
    ) else {
        return 1;
    };
    // Exact limits own input-attempt authority, so later partitions cannot be speculatively
    // opened until the planner and source expose a bounded side-effect-free budget.
    if plan.scan.request.limit.is_some() {
        return 1;
    }
    let speculative_safe = plan
        .compiled_source_execution
        .as_ref()
        .is_some_and(|source| source.execution_capabilities().speculative_safe);
    if !speculative_safe {
        return 1;
    }
    let admitted_jobs = usize::from(scheduler.effective_jobs.jobs.max(1));
    usize::try_from(partition_count).map_or(admitted_jobs, |count| admitted_jobs.min(count))
}

pub(super) fn decide_partition_retry(
    state: &mut cdf_runtime::SourceRetryState,
    error: &CdfError,
    plan_id: &str,
    partition: &cdf_runtime::ScheduledPartition,
    journal: &cdf_runtime::SourceRetryJournal,
) -> Result<cdf_runtime::SourceRetryDecision> {
    let decision = state.decide_after_failure(error)?;
    journal.record(plan_id, partition, state.history())?;
    Ok(decision)
}

pub(super) async fn await_partition_retry(
    state: &mut cdf_runtime::SourceRetryState,
    decision: cdf_runtime::SourceRetryDecision,
    error: &CdfError,
    cancellation: cdf_runtime::RunCancellation,
    plan_id: &str,
    partition: &cdf_runtime::ScheduledPartition,
    journal: &cdf_runtime::SourceRetryJournal,
) -> Result<()> {
    if matches!(
        decision,
        cdf_runtime::SourceRetryDecision::GiveUp {
            reason: cdf_runtime::SourceRetryExhaustion::Ineligible
        }
    ) {
        return Err(error.clone());
    }
    let retry = state.wait_for_retry(decision, cancellation).await;
    journal.record(plan_id, partition, state.history())?;
    let retry = retry?;
    if !retry {
        return Err(retry_exhausted_error(error, state.history()));
    }
    Ok(())
}

pub(super) async fn schedule_partition_retry(
    state: &mut cdf_runtime::SourceRetryState,
    error: &CdfError,
    cancellation: cdf_runtime::RunCancellation,
    plan_id: &str,
    partition: &cdf_runtime::ScheduledPartition,
    journal: &cdf_runtime::SourceRetryJournal,
) -> Result<()> {
    let decision = decide_partition_retry(state, error, plan_id, partition, journal)?;
    await_partition_retry(
        state,
        decision,
        error,
        cancellation,
        plan_id,
        partition,
        journal,
    )
    .await
}

pub(super) fn with_cleanup_failure(
    mut primary: CdfError,
    context: &str,
    cleanup: CdfError,
) -> CdfError {
    primary.message = format!(
        "{}; {context} also failed: {}",
        primary.message, cleanup.message
    );
    primary
}

fn retry_exhausted_error(
    error: &CdfError,
    history: &[cdf_runtime::SourceRetryHistoryEntry],
) -> CdfError {
    let attempts = history
        .last()
        .map_or(1, |entry| entry.failed_attempt.max(1));
    CdfError::new(
        error.kind.clone(),
        format!(
            "{}; source retry stopped after {attempts} attempt(s) ({})",
            error.message,
            history.last().and_then(|entry| entry.exhaustion).map_or(
                "retry unavailable",
                |reason| match reason {
                    cdf_runtime::SourceRetryExhaustion::Ineligible => "error is not retryable",
                    cdf_runtime::SourceRetryExhaustion::AttemptLimit => "attempt limit exhausted",
                    cdf_runtime::SourceRetryExhaustion::ElapsedDeadline => {
                        "elapsed deadline exhausted"
                    }
                }
            )
        ),
    )
}
