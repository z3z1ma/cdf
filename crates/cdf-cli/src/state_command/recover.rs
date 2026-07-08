use std::path::PathBuf;

use cdf_kernel::{CdfError, CheckpointStore, Receipt, StateDelta};
use cdf_project::{PackageReplayReport, ProjectReceiptSource};
use serde::Serialize;

use crate::{
    args::{Cli, StateRecoverArgs},
    commands::output,
    output::{CliError, CommandOutput},
    replay_command::{
        PackageReplayDestinationArgs, build_replay_destination, load_package_replay_context,
    },
    reports::{RunCheckpointReport, RunDestinationReport, RunReceiptReport},
    run_command::ensure_parent_directory,
};

pub(super) fn recover(cli: &Cli, args: StateRecoverArgs) -> Result<CommandOutput, CliError> {
    let package = load_package_replay_context(cli, &args.package_dir)?;
    let selected_receipt = select_recovery_receipt(&package.reader, args.receipt_id.as_deref())?;
    let selection = selected_receipt.selection;
    let selected_receipt_id = selected_receipt.receipt.receipt_id.to_string();
    let mut destination = build_replay_destination(
        &package.project,
        PackageReplayDestinationArgs {
            destination_uri: &args.destination_uri,
            target: args.target.as_deref(),
            merge_dedup: args.merge_dedup.as_deref(),
        },
        &package.inputs,
    )?;
    let destination_report = destination.report().clone();
    let state_store_path = package.project.state_store_path()?;
    ensure_parent_directory(&state_store_path)?;
    let store = package.project.state_store()?;
    let proposed_by_cli = ensure_recovery_checkpoint_proposed(&store, &package.inputs.state_delta)?;
    let report =
        match destination.recover(args.package_dir.clone(), &store, selected_receipt.receipt) {
            Ok(report) => report,
            Err(error) => {
                if proposed_by_cli {
                    let _ = store.abandon(&package.inputs.state_delta.checkpoint_id);
                }
                return Err(error);
            }
        };
    let destination_report =
        destination_report.with_receipt_destination(report.receipt.destination.to_string());
    let cli_report = StateRecoverCliReport::from_report(
        package.package_id,
        args.package_dir,
        package.inputs.state_delta.package_hash.to_string(),
        selected_receipt_id,
        selection,
        destination_report,
        &report,
    );

    output("state recover", cli_report.human_message(), cli_report)
}

fn ensure_recovery_checkpoint_proposed(
    store: &impl CheckpointStore,
    delta: &StateDelta,
) -> Result<bool, CliError> {
    let history = store.history(&delta.pipeline_id, &delta.resource_id, &delta.scope)?;
    if history
        .iter()
        .any(|checkpoint| checkpoint.delta.checkpoint_id == delta.checkpoint_id)
    {
        return Ok(false);
    }
    store.propose(delta.clone())?;
    Ok(true)
}

fn select_recovery_receipt(
    reader: &cdf_package::PackageReader,
    receipt_id: Option<&str>,
) -> Result<SelectedReceipt, CliError> {
    let receipts = reader.receipts()?;
    match receipt_id {
        Some(receipt_id) => {
            let receipt_id = cdf_kernel::ReceiptId::new(receipt_id)?;
            let receipt = receipts
                .into_iter()
                .find(|receipt| receipt.receipt_id == receipt_id)
                .ok_or_else(|| {
                    CdfError::contract(format!(
                        "state recover receipt {} is not present in package receipts",
                        receipt_id
                    ))
                })?;
            Ok(SelectedReceipt {
                receipt,
                selection: RecoveryReceiptSelection::Explicit,
            })
        }
        None => match receipts.len() {
            0 => Err(CdfError::contract(
                "state recover requires exactly one durable package receipt; found zero",
            )
            .into()),
            1 => Ok(SelectedReceipt {
                receipt: receipts.into_iter().next().expect("len checked"),
                selection: RecoveryReceiptSelection::SingleDurableReceipt,
            }),
            count => Err(CdfError::contract(format!(
                "state recover found {count} durable package receipts; pass --receipt to choose one"
            ))
            .into()),
        },
    }
}

struct SelectedReceipt {
    receipt: Receipt,
    selection: RecoveryReceiptSelection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RecoveryReceiptSelection {
    Explicit,
    SingleDurableReceipt,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct StateRecoverCliReport {
    command: &'static str,
    package_id: String,
    package_dir: String,
    package_hash: String,
    selected_receipt_id: String,
    receipt_selection: RecoveryReceiptSelection,
    destination: RunDestinationReport,
    checkpoint_id: String,
    checkpoint: RunCheckpointReport,
    package_status: String,
    receipt_id: String,
    receipt: RunReceiptReport,
    receipt_source: &'static str,
    writes: StateRecoverWriteEffects,
    evidence_limits: Vec<&'static str>,
}

impl StateRecoverCliReport {
    fn from_report(
        package_id: String,
        package_dir: PathBuf,
        package_hash: String,
        selected_receipt_id: String,
        receipt_selection: RecoveryReceiptSelection,
        destination: RunDestinationReport,
        report: &PackageReplayReport,
    ) -> Self {
        Self {
            command: "state recover",
            package_id,
            package_dir: package_dir.display().to_string(),
            package_hash,
            selected_receipt_id,
            receipt_selection,
            destination,
            checkpoint_id: report.checkpoint.delta.checkpoint_id.to_string(),
            checkpoint: RunCheckpointReport::from_checkpoint(&report.checkpoint),
            package_status: report.package_status.as_str().to_owned(),
            receipt_id: report.receipt.receipt_id.to_string(),
            receipt: RunReceiptReport::from_receipt(&report.receipt),
            receipt_source: receipt_source_name(&report.receipt_source),
            writes: StateRecoverWriteEffects {
                package_status: true,
                destination_rows: false,
                checkpoint: true,
            },
            evidence_limits: vec![
                "package-receipt recovery verifies destination receipt and checkpoint coverage only",
                "package-receipt recovery does not reconstruct quarantine lineage",
                "package-receipt recovery does not reconstruct arbitrary missing run-ledger history",
            ],
        }
    }

    fn human_message(&self) -> String {
        format!(
            "recovered checkpoint {} from package receipt {}; destination rows were not written; evidence limits reported",
            self.checkpoint_id, self.receipt_id
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct StateRecoverWriteEffects {
    package_status: bool,
    destination_rows: bool,
    checkpoint: bool,
}

fn receipt_source_name(source: &ProjectReceiptSource) -> &'static str {
    match source {
        ProjectReceiptSource::DestinationCommit { .. } => "destination_commit",
        ProjectReceiptSource::DestinationCommitReceiptOnly { .. } => {
            "destination_commit_receipt_only"
        }
        ProjectReceiptSource::SuppliedDurableReceipt => "supplied_durable_receipt",
    }
}
