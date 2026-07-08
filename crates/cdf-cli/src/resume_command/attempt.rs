use std::path::{Path, PathBuf};

use cdf_kernel::{CdfError, Checkpoint, Receipt, RunEventSink, RunId, StateDelta};
use cdf_package::{PackageReader, PackageStatus};
use cdf_project::{
    PackageArtifactRecoveryRequest, PackageArtifactReplayRequest, recover_package_from_artifacts,
    replay_package_from_artifacts,
};
use cdf_state_sqlite::{
    RunEventAppend, RunEventKind, RunLedgerSnapshot, SqliteCheckpointStore, SqliteRunLedger,
};

use crate::{context::ProjectContext, output::CliError};

use super::{
    destination::SelectedDestination,
    events::{
        base_package_event, fill_package_event_fields, receipt_source_name, resume_event_details,
        run_succeeded,
    },
    model::{
        CommonReplayReport, ResumePackageFacts, ResumeReplayReport, StatusRepairProof,
        checkpoint_status, package_path_from_events, prove_status_repair_head,
        resolve_project_path, select_receipt,
    },
    report::{
        ResumeCheckpointPointer, ResumeDestinationPointer, ResumePackagePointer,
        ResumeReceiptPointer, ResumeRecoveryReport, ResumeReport,
    },
};

pub(super) struct ResumeAttempt<'a> {
    context: &'a ProjectContext,
    run_ledger: &'a SqliteRunLedger,
    snapshot: &'a RunLedgerSnapshot,
    event_sink: Option<&'a dyn RunEventSink>,
    run_id: RunId,
    package_path: Option<PathBuf>,
    package: Option<ResumePackageFacts>,
    package_error: Option<String>,
    store: SqliteCheckpointStore,
}

impl<'a> ResumeAttempt<'a> {
    pub(super) fn new(
        context: &'a ProjectContext,
        run_ledger: &'a SqliteRunLedger,
        snapshot: &'a RunLedgerSnapshot,
        event_sink: Option<&'a dyn RunEventSink>,
    ) -> Result<Self, CliError> {
        let package_path = package_path_from_events(&snapshot.events)
            .map(|path| resolve_project_path(&context.root, Path::new(&path)));
        let (package, package_error) = match package_path.as_ref() {
            Some(path) if path.exists() => match ResumePackageFacts::load(path) {
                Ok(package) => (Some(package), None),
                Err(error) => (None, Some(error.message)),
            },
            Some(_) | None => (None, None),
        };
        let store = context.state_store()?;
        Ok(Self {
            context,
            run_ledger,
            snapshot,
            event_sink,
            run_id: snapshot.run.run_id.clone(),
            package_path,
            package,
            package_error,
            store,
        })
    }

    pub(super) fn execute(&self) -> Result<ResumeReport, CliError> {
        if run_succeeded(&self.snapshot.events) {
            let mut report = self.report(
                "terminal_success",
                "no_op",
                false,
                false,
                "success",
                "terminal successful run; no recovery action is required",
            );
            self.append_run_resumed(&report)?;
            report.ledger_event_count_after = self.snapshot_after()?.events.len();
            return Ok(report);
        }

        let Some(package) = &self.package else {
            return Ok(self.report_missing_package_facts());
        };
        if !package.path.exists() {
            let report = self.fail_closed(
                "missing_package_artifact",
                "inspect_missing_artifacts",
                format!(
                    "package path {} recorded for the run does not exist; restore the package artifact or rerun from the last committed checkpoint",
                    package.path.display()
                ),
            );
            let _ = self.append_run_failed(&report);
            return Ok(report);
        }

        let replay_inputs = match package.reader.replay_inputs() {
            Ok(inputs) => inputs,
            Err(error) => {
                let report = self.fail_closed(
                    "inconsistent_package_artifact",
                    "inspect_missing_artifacts",
                    format!(
                        "package {} is not replayable from durable artifacts: {error}",
                        package.path.display()
                    ),
                );
                let _ = self.append_run_failed(&report);
                return Ok(report);
            }
        };
        let checkpoint_status = checkpoint_status(&self.store, &replay_inputs.state_delta)?;
        let receipt = select_receipt(package, &self.snapshot.events);

        match (receipt, checkpoint_status.committed) {
            (None, true) => {
                let report = self.fail_closed(
                    "committed_checkpoint_missing_receipt_artifact",
                    "inspect_missing_artifacts",
                    "checkpoint is committed but no durable package receipt is available; restore receipts.json before claiming recovery",
                );
                let _ = self.append_run_failed(&report);
                Ok(report)
            }
            (None, false) => self.replay_finalized_package(package, &replay_inputs),
            (Some(receipt), false) => {
                if !checkpoint_status.proposed {
                    let report = self.fail_closed(
                        "receipt_without_proposed_checkpoint",
                        "inspect_missing_artifacts",
                        format!(
                            "receipt {} is durable, but checkpoint {} is not proposed in the selected state store; restore checkpoint state before opening the commit gate",
                            receipt.receipt_id, replay_inputs.state_delta.checkpoint_id
                        ),
                    );
                    let _ = self.append_run_failed(&report);
                    return Ok(report);
                }
                self.recover_durable_receipt(package, receipt)
            }
            (Some(_), true)
                if package.status == PackageStatus::Checkpointed
                    || package.status == PackageStatus::Archived =>
            {
                let mut report = self.report(
                    "already_recovered",
                    "no_op",
                    false,
                    false,
                    "success",
                    "checkpoint is committed and package status is terminal; no recovery mutation was required",
                );
                self.append_run_resumed(&report)?;
                report.ledger_event_count_after = self.snapshot_after()?.events.len();
                Ok(report)
            }
            (Some(receipt), true) => {
                self.update_stale_package_status(package, &replay_inputs.state_delta, receipt)
            }
        }
    }

    fn report_missing_package_facts(&self) -> ResumeReport {
        if let Some(path) = &self.package_path
            && !path.exists()
        {
            let report = self.fail_closed(
                "missing_package_artifact",
                "inspect_missing_artifacts",
                format!(
                    "package path {} recorded for the run does not exist; restore the package artifact or rerun from the last committed checkpoint",
                    path.display()
                ),
            );
            let _ = self.append_run_failed(&report);
            return report;
        }
        if let Some(error) = &self.package_error {
            let report = self.fail_closed(
                "inconsistent_package_artifact",
                "inspect_missing_artifacts",
                format!("package artifact recorded for the run is unreadable: {error}"),
            );
            let _ = self.append_run_failed(&report);
            return report;
        }
        let report = self.fail_closed(
            "no_finalized_package",
            "rerun_extraction_from_last_committed_checkpoint",
            "no finalized package path is recorded for this run; resume will not invent no-argument discovery or source rerun inputs",
        );
        let _ = self.append_run_failed(&report);
        report
    }

    fn replay_finalized_package(
        &self,
        package: &ResumePackageFacts,
        inputs: &cdf_package::PackageReplayInputs,
    ) -> Result<ResumeReport, CliError> {
        let mut selected =
            match self.selected_destination_or_report(&inputs.destination_commit.target)? {
                Ok(destination) => destination,
                Err(report) => return Ok(report),
            };
        let destination = selected.take()?;
        let report = replay_package_from_artifacts(PackageArtifactReplayRequest {
            package_dir: package.path.clone(),
            destination,
            checkpoint_store: &self.store,
            after_receipt_verified: None,
        })
        .map(ResumeReplayReport::Generic)
        .map_err(|error| selected.redact_error(error))?;
        self.success_after_replay(package, &inputs.state_delta, report)
    }

    fn recover_durable_receipt(
        &self,
        package: &ResumePackageFacts,
        receipt: Receipt,
    ) -> Result<ResumeReport, CliError> {
        let inputs = package.reader.replay_inputs()?;
        let mut selected =
            match self.selected_destination_or_report(&inputs.destination_commit.target)? {
                Ok(destination) => destination,
                Err(report) => return Ok(report),
            };
        let destination = selected.take()?;
        let report = recover_package_from_artifacts(PackageArtifactRecoveryRequest {
            package_dir: package.path.clone(),
            checkpoint_store: &self.store,
            destination,
            receipt,
            after_receipt_verified: None,
        })
        .map(ResumeReplayReport::Generic)
        .map_err(|error| selected.redact_error(error))?;
        self.success_after_receipt_recovery(package, report)
    }

    fn selected_destination_or_report(
        &self,
        target: &cdf_kernel::TargetName,
    ) -> Result<Result<SelectedDestination, ResumeReport>, CliError> {
        match SelectedDestination::from_context(self.context, "resume", target) {
            Ok(destination) => Ok(Ok(destination)),
            Err(error) if error.not_supported => {
                let report = self.fail_closed(
                    "unsupported_destination",
                    "inspect_destination",
                    error.message,
                );
                let _ = self.append_run_failed(&report);
                Ok(Err(report))
            }
            Err(error) => Err(error),
        }
    }

    fn update_stale_package_status(
        &self,
        package: &ResumePackageFacts,
        delta: &StateDelta,
        receipt: Receipt,
    ) -> Result<ResumeReport, CliError> {
        let Some(package_replay_inputs) = &package.replay_inputs else {
            let report = self.fail_closed(
                "checkpoint_committed_package_replay_inputs_missing",
                "inspect_missing_artifacts",
                "package status repair requires replay inputs before mutation; restore package state/destination preimages before updating status",
            );
            let _ = self.append_run_failed(&report);
            return Ok(report);
        };
        if package_replay_inputs.state_delta != *delta {
            let report = self.fail_closed(
                "checkpoint_committed_package_delta_mismatch",
                "inspect_missing_artifacts",
                "package status repair requires package replay inputs to match the selected recovery delta exactly",
            );
            let _ = self.append_run_failed(&report);
            return Ok(report);
        }
        let head = match prove_status_repair_head(&self.store, delta, &receipt)? {
            StatusRepairProof::Exact(head) => *head,
            StatusRepairProof::Missing => {
                let report = self.fail_closed(
                    "checkpoint_committed_head_missing",
                    "inspect_missing_artifacts",
                    "package status repair requires a committed current head for the package scope before mutation",
                );
                let _ = self.append_run_failed(&report);
                return Ok(report);
            }
            StatusRepairProof::NotExact => {
                let report = self.fail_closed(
                    "checkpoint_committed_head_not_exact",
                    "inspect_missing_artifacts",
                    "package status repair requires the current committed head delta and receipt to exactly match the package replay delta and selected durable receipt",
                );
                let _ = self.append_run_failed(&report);
                return Ok(report);
            }
        };

        let mut reader = PackageReader::open(&package.path)?;
        let status = reader
            .update_status(PackageStatus::Checkpointed)?
            .lifecycle
            .status
            .clone();
        let mut report = self.report(
            "checkpoint_committed_with_stale_package_status",
            "update_package_status",
            false,
            true,
            "success",
            "checkpoint was already committed; updated package status only",
        );
        report.mutated = true;
        report.package.status = Some(status.as_str().to_owned());
        report.package.receipt_count = PackageReader::open(&package.path)?.receipts()?.len();
        report.receipt = ResumeReceiptPointer::from_receipt(Some(&receipt));
        report.checkpoint = ResumeCheckpointPointer::from_checkpoint(Some(&head));
        self.append_package_status_updated(package, &status)?;
        self.append_run_resumed(&report)?;
        report.ledger_event_count_after = self.snapshot_after()?.events.len();
        Ok(report)
    }

    fn success_after_replay(
        &self,
        package: &ResumePackageFacts,
        delta: &StateDelta,
        replay: ResumeReplayReport,
    ) -> Result<ResumeReport, CliError> {
        let common = replay.common();
        let source = common.receipt_source.clone();
        let mut report = self.report(
            "package_finalized_without_receipt",
            "replay_package",
            false,
            true,
            "success",
            "replayed finalized package without contacting the source, then committed checkpoint",
        );
        report.mutated = true;
        report.package.status = Some(common.package_status.as_str().to_owned());
        report.package.receipt_count = PackageReader::open(&package.path)?.receipts()?.len();
        report.checkpoint = ResumeCheckpointPointer::from_checkpoint(Some(common.checkpoint));
        report.receipt = ResumeReceiptPointer::from_receipt(Some(common.receipt));
        self.append_replay_events(package, delta, &common)?;
        self.append_run_resumed(&report)?;
        report.receipt.source = Some(receipt_source_name(
            &source,
            common.receipt.destination.as_ref(),
        ));
        report.ledger_event_count_after = self.snapshot_after()?.events.len();
        Ok(report)
    }

    fn success_after_receipt_recovery(
        &self,
        package: &ResumePackageFacts,
        replay: ResumeReplayReport,
    ) -> Result<ResumeReport, CliError> {
        let common = replay.common();
        let mut report = self.report(
            "receipt_recorded_without_checkpoint_commit",
            "verify_receipt_then_commit_checkpoint",
            false,
            true,
            "success",
            "verified durable receipt and committed checkpoint without contacting the source",
        );
        report.mutated = true;
        report.package.status = Some(common.package_status.as_str().to_owned());
        report.package.receipt_count = PackageReader::open(&package.path)?.receipts()?.len();
        report.checkpoint = ResumeCheckpointPointer::from_checkpoint(Some(common.checkpoint));
        report.receipt = ResumeReceiptPointer::from_receipt(Some(common.receipt));
        report.receipt.source = Some("supplied_durable_receipt".to_owned());
        self.append_checkpoint_committed(package, common.checkpoint, common.receipt)?;
        self.append_package_status_updated(package, common.package_status)?;
        self.append_run_resumed(&report)?;
        report.ledger_event_count_after = self.snapshot_after()?.events.len();
        Ok(report)
    }

    fn report(
        &self,
        state: impl Into<String>,
        action: impl Into<String>,
        source_contact: bool,
        mutation_required: bool,
        result: impl Into<String>,
        guidance: impl Into<String>,
    ) -> ResumeReport {
        let package = self
            .package
            .as_ref()
            .map(ResumePackagePointer::from_facts)
            .unwrap_or_else(|| ResumePackagePointer {
                path: self
                    .package_path
                    .as_ref()
                    .map(|path| path.display().to_string()),
                ..ResumePackagePointer::default()
            });
        let checkpoint = self
            .package
            .as_ref()
            .and_then(|package| package.replay_inputs.as_ref())
            .and_then(|inputs| checkpoint_status(&self.store, &inputs.state_delta).ok())
            .map(|status| status.pointer)
            .unwrap_or_default();
        let receipt = self
            .package
            .as_ref()
            .and_then(|package| select_receipt(package, &self.snapshot.events))
            .as_ref()
            .map(|receipt| ResumeReceiptPointer::from_receipt(Some(receipt)))
            .unwrap_or_default();
        ResumeReport {
            command: "resume",
            run_id: self.run_id.to_string(),
            state: state.into(),
            action: action.into(),
            source_contact,
            mutation_required,
            mutated: false,
            package,
            checkpoint,
            receipt,
            destination: ResumeDestinationPointer::from_context(self.context),
            recovery: ResumeRecoveryReport {
                result: result.into(),
                guidance: guidance.into(),
            },
            ledger_event_count_before: self.snapshot.events.len(),
            ledger_event_count_after: self.snapshot.events.len(),
        }
    }

    pub(super) fn fail_closed(
        &self,
        state: impl Into<String>,
        action: impl Into<String>,
        guidance: impl Into<String>,
    ) -> ResumeReport {
        self.report(state, action, false, false, "failed_closed", guidance)
    }

    fn append_replay_events(
        &self,
        package: &ResumePackageFacts,
        delta: &StateDelta,
        common: &CommonReplayReport<'_>,
    ) -> Result<(), CliError> {
        let mut receipt_event = base_package_event(
            package,
            RunEventKind::DestinationReceiptRecorded,
            Some(delta),
            Some(common.receipt),
        );
        receipt_event.destination_id = Some(common.receipt.destination.clone());
        self.append_event(receipt_event)?;
        self.append_checkpoint_committed(package, common.checkpoint, common.receipt)?;
        self.append_package_status_updated(package, common.package_status)?;
        Ok(())
    }

    fn append_checkpoint_committed(
        &self,
        package: &ResumePackageFacts,
        checkpoint: &Checkpoint,
        receipt: &Receipt,
    ) -> Result<(), CliError> {
        let mut event = base_package_event(
            package,
            RunEventKind::CheckpointCommitted,
            Some(&checkpoint.delta),
            Some(receipt),
        );
        event.destination_id = Some(receipt.destination.clone());
        self.append_event(event)?;
        Ok(())
    }

    fn append_package_status_updated(
        &self,
        package: &ResumePackageFacts,
        status: &PackageStatus,
    ) -> Result<(), CliError> {
        let mut event = base_package_event(
            package,
            RunEventKind::PackageStatusUpdated,
            package
                .replay_inputs
                .as_ref()
                .map(|inputs| &inputs.state_delta),
            None,
        );
        event.details = cdf_state_sqlite::RunEventDetails::new([(
            "package_status",
            cdf_state_sqlite::RunEventValue::String(status.as_str().to_owned()),
        )]);
        self.append_event(event)?;
        Ok(())
    }

    fn append_run_resumed(&self, report: &ResumeReport) -> Result<(), CliError> {
        let mut event = RunEventAppend::new(RunEventKind::RunResumed);
        if let Some(package) = &self.package {
            fill_package_event_fields(
                &mut event,
                package,
                package
                    .replay_inputs
                    .as_ref()
                    .map(|inputs| &inputs.state_delta),
                select_receipt(package, &self.snapshot.events).as_ref(),
            );
        }
        event.details = resume_event_details(report);
        self.append_event(event)?;
        Ok(())
    }

    pub(super) fn append_run_failed(&self, report: &ResumeReport) -> Result<(), CliError> {
        let mut event = RunEventAppend::new(RunEventKind::RunFailed);
        if let Some(package) = &self.package {
            fill_package_event_fields(
                &mut event,
                package,
                package
                    .replay_inputs
                    .as_ref()
                    .map(|inputs| &inputs.state_delta),
                select_receipt(package, &self.snapshot.events).as_ref(),
            );
        }
        event.details = resume_event_details(report);
        self.append_event(event)?;
        Ok(())
    }

    fn append_event(&self, event: RunEventAppend) -> Result<(), CliError> {
        let stored = self.run_ledger.append_event(&self.run_id, event)?;
        if let Some(sink) = self.event_sink {
            let _ = sink.try_emit(&stored);
        }
        Ok(())
    }

    fn snapshot_after(&self) -> Result<RunLedgerSnapshot, CliError> {
        self.run_ledger
            .snapshot(&self.run_id)?
            .ok_or_else(|| CdfError::internal("resumed run disappeared from run ledger").into())
    }
}
