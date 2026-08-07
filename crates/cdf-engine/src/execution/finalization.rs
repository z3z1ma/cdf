//! Package publication and final output assembly.

use cdf_contract::{AdmissionPolicy, VerdictSummary};
use cdf_kernel::{CdfError, Result, SourceTransferReport, TerminalSchemaObservationQuarantine};
use cdf_package::PackageBuilder;
use cdf_package_contract::PackageStatus;

use super::measurements::{PhaseMeasurements, elapsed_ns};
use crate::{
    EngineDrainEpoch, EngineExecutionEvidence, EnginePackageDraft, EngineRunOutput,
    EngineRunOutputWithSegmentPositions, EngineSegmentPosition, ExecutionProfile, LineageSummary,
};

pub type PackagePreFinalizeHook<'a> =
    dyn Fn(&PackageBuilder, EnginePackageDraft<'_>) -> Result<()> + 'a;
pub type StreamingFinalizeHook<'a> = dyn FnMut() -> Result<()> + 'a;

pub(super) enum PackageExecutionOutcome {
    Package(Box<EngineRunOutputWithSegmentPositions>),
    DrainFinishedNoOp {
        source_frontier: cdf_runtime::SourceFrontierReport,
    },
}

impl PackageExecutionOutcome {
    pub(super) fn into_package(self) -> Result<EngineRunOutputWithSegmentPositions> {
        match self {
            Self::Package(output) => Ok(*output),
            Self::DrainFinishedNoOp { .. } => Err(CdfError::internal(
                "bounded package execution produced a drain no-op",
            )),
        }
    }
}

pub(super) struct PackageFinalization<'pre, 'stream> {
    pub(super) builder: PackageBuilder,
    pub(super) pre_finalize: Option<&'pre PackagePreFinalizeHook<'pre>>,
    pub(super) stream_finalize: Option<&'stream mut StreamingFinalizeHook<'stream>>,
    pub(super) profile: ExecutionProfile,
    pub(super) lineage: LineageSummary,
    pub(super) admission: AdmissionPolicy,
    pub(super) verdict_summary: VerdictSummary,
    pub(super) terminal_schema_quarantines: Vec<TerminalSchemaObservationQuarantine>,
    pub(super) segment_positions: Vec<EngineSegmentPosition>,
    pub(super) phase_measurements: PhaseMeasurements,
    pub(super) source_frontier: cdf_runtime::SourceFrontierReport,
    pub(super) source_transfer: SourceTransferReport,
    pub(super) drain_epoch_closure: Option<cdf_runtime::DrainEpochClosure>,
    pub(super) consumed_partition_count: u64,
    pub(super) drain_partition_resume: Option<Box<crate::DrainPartitionResume>>,
    pub(super) consumed_late_data_carryover: Vec<cdf_kernel::LateDataCarryoverRef>,
    pub(super) late_data_carryover: Vec<cdf_kernel::LateDataCarryoverRef>,
    pub(super) partition_watermarks: Vec<cdf_kernel::PartitionWatermarkState>,
    pub(super) execution_evidence: EngineExecutionEvidence,
}

impl PackageFinalization<'_, '_> {
    pub(super) fn finish(mut self) -> Result<PackageExecutionOutcome> {
        if self.verdict_summary.accepted_with_residual_rows > self.verdict_summary.accepted_rows {
            return Err(CdfError::internal(
                "accepted-with-residual row count exceeds accepted row count",
            ));
        }
        if let Some(stream_finalize) = self.stream_finalize.as_deref_mut() {
            stream_finalize()?;
        }
        self.builder.update_status(PackageStatus::Validated)?;
        if let Some(pre_finalize) = self.pre_finalize {
            pre_finalize(
                &self.builder,
                EnginePackageDraft {
                    profile: &self.profile,
                    lineage: &self.lineage,
                    segment_positions: &self.segment_positions,
                    drain_frontier: self
                        .drain_epoch_closure
                        .as_ref()
                        .map(|closure| &closure.frontier),
                    consumed_late_data_carryover: &self.consumed_late_data_carryover,
                    late_data_carryover: &self.late_data_carryover,
                    partition_watermarks: &self.partition_watermarks,
                    execution_evidence: &self.execution_evidence,
                },
            )?;
        }
        let finalize_started = self.phase_measurements.start();
        let (manifest, verification) = self.builder.finish_verified()?;
        self.phase_measurements.add(
            cdf_kernel::RunPhase::PackageFinalize,
            elapsed_ns(finalize_started, "package finalize")?,
            self.profile.output_bytes,
            manifest.identity.file_bytes,
        );

        Ok(PackageExecutionOutcome::Package(Box::new(
            EngineRunOutputWithSegmentPositions {
                output: EngineRunOutput {
                    manifest,
                    verification,
                    profile: self.profile,
                    lineage: self.lineage,
                    admission: self.admission,
                    verdict_summary: self.verdict_summary,
                    terminal_schema_quarantines: self.terminal_schema_quarantines,
                },
                segment_positions: self.segment_positions,
                phase_metrics: self.phase_measurements.into_metrics(),
                source_frontier: self.source_frontier,
                source_transfer: self.source_transfer,
                drain_epoch: self.drain_epoch_closure.map(|closure| EngineDrainEpoch {
                    closure,
                    consumed_partition_count: self.consumed_partition_count,
                    resume_partition: self.drain_partition_resume,
                    consumed_late_data_carryover: self.consumed_late_data_carryover,
                    late_data_carryover: self.late_data_carryover,
                    partition_watermarks: self.partition_watermarks,
                }),
                execution_evidence: self.execution_evidence,
            },
        )))
    }
}
