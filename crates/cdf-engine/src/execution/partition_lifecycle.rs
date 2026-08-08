//! Partition-scoped execution lifecycle ownership.

use cdf_kernel::{CdfError, ExecutablePartition, PartitionAttestation, PartitionPlan, Result};

use crate::{EngineExecutionInvocation, EnginePlan};

pub(super) fn validate_execution_invocation<R>(
    plan: &EnginePlan,
    resource: &R,
    drain_controller: Option<&cdf_runtime::DrainEpochController>,
    options: &EngineExecutionInvocation,
) -> Result<u64>
where
    R: cdf_kernel::ResourceStream + ?Sized,
{
    plan.validate_execution_extent_for_execution()?;
    plan.validate_resolved_transaction_limit(
        options
            .services
            .as_ref()
            .map(|services| services.spill().snapshot().budget_bytes),
    )?;
    match (&plan.execution_extent, drain_controller.is_some()) {
        (cdf_kernel::ExecutionExtent::Bounded { .. }, false)
        | (cdf_kernel::ExecutionExtent::Drain { .. }, true) => {}
        (cdf_kernel::ExecutionExtent::Bounded { .. }, true) => {
            return Err(CdfError::contract(
                "bounded execution cannot use the drain epoch controller",
            ));
        }
        (cdf_kernel::ExecutionExtent::Drain { .. }, false) => {
            return Err(CdfError::contract(
                "drain execution requires the finite epoch controller and settlement gate",
            ));
        }
        (cdf_kernel::ExecutionExtent::Resident { .. }, _) => {
            return Err(CdfError::contract(
                "resident execution is not enabled; use a finite drain termination",
            ));
        }
    }
    if let Some(controller) = drain_controller {
        controller.validate_ready_for_epoch()?;
    }
    plan.validate_compiled_expression_plan()?;
    plan.validate_partition_schedule()?;
    plan.validate_compiled_source_resource(resource)?;
    let planned_partition_count = plan.scan.partition_count()?;
    if matches!(
        plan.write_disposition,
        cdf_kernel::WriteDisposition::CdcApply
    ) && planned_partition_count > 1
    {
        return Err(CdfError::contract(
            "cdc_apply currently requires one ordered source partition so settlement and key order cannot be interleaved by jobs",
        ));
    }
    if let Some(scheduler) = &options.scheduler {
        let source = plan.compiled_source_execution.as_ref().ok_or_else(|| {
            CdfError::contract("package execution requires a compiled source execution plan")
        })?;
        scheduler.validate_for_source(planned_partition_count, source.execution_capabilities())?;
    }
    crate::planning::validate_program(&plan.validation_program)?;
    cdf_kernel::validate_scan_partition_observation_identities(&plan.scan)?;
    cdf_kernel::validate_compiled_scan_intents(&plan.scan)?;
    let schema_authority = plan.schema_authority();
    if schema_authority.version != 1 {
        return Err(CdfError::data(format!(
            "unsupported engine schema-authority version {}",
            schema_authority.version
        )));
    }
    Ok(planned_partition_count)
}

#[derive(Clone)]
pub(super) struct PartitionOpenEvidence {
    pub(super) duration_ns: u64,
    pub(super) retry_pre_attestation: Option<PartitionAttestation>,
}

#[derive(Clone)]
pub(super) struct PartitionOpenMetadata {
    pub(super) ordinal: u64,
    pub(super) partition: ExecutablePartition,
    pub(super) evidence: PartitionOpenEvidence,
}

pub(super) type OpenedPartition = (
    PartitionOpenMetadata,
    Option<cdf_kernel::OpenedPartitionStream>,
);

#[derive(Clone)]
pub(super) struct PartitionOpenRuntime {
    pub(super) services: Option<cdf_runtime::ExecutionServices>,
    pub(super) cancellation: cdf_runtime::RunCancellation,
    pub(super) retry_journal: cdf_runtime::SourceRetryJournal,
    pub(super) retry_progress: Option<std::sync::Arc<crate::SourceRetryProgressObserver>>,
}

pub(super) enum ExecutablePartitionPlans {
    Inline(Vec<PartitionPlan>),
    External(Box<dyn cdf_kernel::PlannedPartitionReader>),
}

impl ExecutablePartitionPlans {
    pub(super) fn next(&mut self, ordinal: u64) -> Result<ExecutablePartition> {
        match self {
            Self::Inline(partitions) => partitions
                .get(usize::try_from(ordinal).map_err(|_| {
                    CdfError::data("inline partition ordinal exceeds host address space")
                })?)
                .cloned()
                .map(ExecutablePartition::inline)
                .ok_or_else(|| {
                    CdfError::internal("source frontier requested an absent partition ordinal")
                }),
            Self::External(reader) => reader.next_partition(ordinal)?.ok_or_else(|| {
                CdfError::data(
                    "external planned task set ended before its recorded partition count",
                )
            }),
        }
    }
}
