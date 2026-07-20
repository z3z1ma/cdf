use std::path::PathBuf;

use cdf_kernel::{CdfError, CheckpointStore, Receipt, StateDelta};
use cdf_project::{PackageReplayReport, ProjectReceiptSource};
use serde::Serialize;

use crate::{
    args::{Cli, StateRecoverArgs},
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
        redaction::redact_uri_userinfo,
    },
    replay_command::{
        PackageReplayDestinationArgs, build_replay_destination, load_package_replay_context,
    },
    reports::{RunCheckpointReport, RunDestinationReport, RunReceiptReport},
    run_command::ensure_parent_directory,
};

pub(super) fn recover(
    cli: &Cli,
    args: StateRecoverArgs,
    execution: &cdf_runtime::ExecutionServices,
    destinations: &cdf_runtime::DestinationRegistry,
) -> Result<CommandOutput, CliError> {
    let package = load_package_replay_context(cli, &args.package_dir)?;
    let selected_receipt = select_recovery_receipt(&package.reader, args.receipt_id.as_deref())?;
    let selection = selected_receipt.selection;
    let selected_receipt_id = selected_receipt.receipt.receipt_id.to_string();
    let mut destination = build_replay_destination(
        destinations,
        &package.project,
        PackageReplayDestinationArgs {
            destination_uri: Some(&args.destination_uri),
            target: args.target.as_deref(),
            merge_dedup: args.merge_dedup.as_deref(),
        },
        &package.inputs,
        execution,
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

    CommandOutput::rendered("state recover", cli_report.render_document(), cli_report)
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
    match receipt_id {
        Some(receipt_id) => {
            let receipt_id = cdf_kernel::ReceiptId::new(receipt_id)?;
            let mut receipt = None;
            reader.for_each_receipt(&mut |candidate| {
                if receipt.is_none() && candidate.receipt_id == receipt_id {
                    receipt = Some(candidate);
                }
                Ok(())
            })?;
            let receipt = receipt.ok_or_else(|| {
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
        None => {
            let mut first = None;
            let count = reader.for_each_receipt(&mut |receipt| {
                if first.is_none() {
                    first = Some(receipt);
                }
                Ok(())
            })?;
            match (first, count) {
                (None, 0) => Err(CdfError::contract(
                    "state recover requires exactly one durable package receipt; found zero",
                )
                .into()),
                (Some(receipt), 1) => Ok(SelectedReceipt {
                    receipt,
                    selection: RecoveryReceiptSelection::SingleDurableReceipt,
                }),
                _ => Err(CdfError::contract(format!(
                    "state recover found {count} durable package receipts; pass --receipt to choose one"
                ))
                .into()),
            }
        }
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

    fn render_document(&self) -> RenderDocument {
        let limit_table = self
            .evidence_limits
            .iter()
            .fold(Table::new(["evidence limit"]), |table, limit| {
                table.row([(*limit).to_owned()])
            });

        RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                StatusKind::Success,
                format!("recovered checkpoint {}", self.checkpoint_id),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Recovery")
                    .row("package", self.package_id.clone())
                    .row("package dir", safe_display_value(&self.package_dir))
                    .row("package hash", self.package_hash.clone())
                    .row("selected receipt", self.selected_receipt_id.clone())
                    .row(
                        "receipt selection",
                        receipt_selection_name(self.receipt_selection),
                    )
                    .row("receipt source", self.receipt_source)
                    .row(
                        "next command",
                        format!(
                            "cdf inspect package {}",
                            safe_display_value(&self.package_dir)
                        ),
                    ),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Checkpoint")
                    .row("checkpoint", self.checkpoint_id.clone())
                    .row("package status", self.package_status.clone())
                    .row("receipt", self.receipt_id.clone())
                    .row(
                        "mutation performed",
                        "checkpoint committed from durable receipt",
                    ),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Writes")
                    .row("package status", yes_no(self.writes.package_status))
                    .row("destination rows", yes_no(self.writes.destination_rows))
                    .row("checkpoint", yes_no(self.writes.checkpoint))
                    .row(
                        "destination",
                        "verified receipt only; destination rows were not written",
                    ),
            )
            .blank_line()
            .push(limit_table)
            .blank_line()
            .push(NextCommand::new(format!(
                "cdf inspect package {}",
                safe_display_value(&self.package_dir)
            )))
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

fn receipt_selection_name(selection: RecoveryReceiptSelection) -> &'static str {
    match selection {
        RecoveryReceiptSelection::Explicit => "explicit",
        RecoveryReceiptSelection::SingleDurableReceipt => "single_durable_receipt",
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn safe_display_value(value: &str) -> String {
    redact_uri_userinfo(value)
}
