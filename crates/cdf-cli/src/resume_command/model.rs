use std::path::{Path, PathBuf};

use cdf_kernel::{Checkpoint, CheckpointStatus, CheckpointStore, Receipt, StateDelta};
use cdf_package::PackageReader;
use cdf_package_contract::{PackageReplayInputs, PackageStatus};
use cdf_project::{PackageReplayReport, ProjectReceiptSource};
use cdf_state_sqlite::{RunEvent, SqliteCheckpointStore};

use crate::output::CliError;

use super::report::ResumeCheckpointPointer;

pub(super) struct ResumePackageFacts {
    pub(super) path: PathBuf,
    pub(super) reader: PackageReader,
    pub(super) status: PackageStatus,
    pub(super) replay_inputs: Option<PackageReplayInputs>,
    pub(super) receipts: Vec<Receipt>,
}

impl ResumePackageFacts {
    pub(super) fn load(path: &Path) -> Result<Self, CliError> {
        let reader = PackageReader::open(path)?;
        let status = reader.manifest().lifecycle.status.clone();
        let replay_inputs = reader.replay_inputs().ok();
        let receipts = reader.receipts()?;
        Ok(Self {
            path: path.to_path_buf(),
            reader,
            status,
            replay_inputs,
            receipts,
        })
    }
}

pub(super) enum ResumeReplayReport {
    Generic(PackageReplayReport),
}

impl ResumeReplayReport {
    pub(super) fn common(&self) -> CommonReplayReport<'_> {
        match self {
            Self::Generic(report) => CommonReplayReport {
                checkpoint: &report.checkpoint,
                receipt: &report.receipt,
                receipt_source: report.receipt_source.clone(),
                package_status: &report.package_status,
            },
        }
    }
}

pub(super) struct CommonReplayReport<'a> {
    pub(super) checkpoint: &'a Checkpoint,
    pub(super) receipt: &'a Receipt,
    pub(super) receipt_source: ProjectReceiptSource,
    pub(super) package_status: &'a PackageStatus,
}

pub(super) struct CheckpointFacts {
    pub(super) proposed: bool,
    pub(super) committed: bool,
    pub(super) pointer: ResumeCheckpointPointer,
}

pub(super) enum StatusRepairProof {
    Exact(Box<Checkpoint>),
    Missing,
    NotExact,
}

pub(super) fn checkpoint_status(
    store: &SqliteCheckpointStore,
    delta: &StateDelta,
) -> Result<CheckpointFacts, CliError> {
    let history = store.history(&delta.pipeline_id, &delta.resource_id, &delta.scope)?;
    let checkpoint = history
        .iter()
        .rev()
        .find(|checkpoint| checkpoint.delta.checkpoint_id == delta.checkpoint_id);
    let proposed = checkpoint
        .map(|checkpoint| checkpoint.status == CheckpointStatus::Proposed)
        .unwrap_or(false);
    let committed = checkpoint
        .map(|checkpoint| checkpoint.status == CheckpointStatus::Committed)
        .unwrap_or(false);
    Ok(CheckpointFacts {
        proposed,
        committed,
        pointer: ResumeCheckpointPointer::from_checkpoint(checkpoint),
    })
}

pub(super) fn prove_status_repair_head(
    store: &SqliteCheckpointStore,
    delta: &StateDelta,
    receipt: &Receipt,
) -> Result<StatusRepairProof, CliError> {
    let Some(head) = store.head(&delta.pipeline_id, &delta.resource_id, &delta.scope)? else {
        return Ok(StatusRepairProof::Missing);
    };
    if head.status == CheckpointStatus::Committed
        && head.is_head
        && head.delta == *delta
        && head.receipt.as_ref() == Some(receipt)
    {
        Ok(StatusRepairProof::Exact(Box::new(head)))
    } else {
        Ok(StatusRepairProof::NotExact)
    }
}

pub(super) fn package_path_from_events(events: &[RunEvent]) -> Option<String> {
    let mut paths = Vec::new();
    for event in events {
        if let Some(path) = &event.package_path
            && !paths.contains(path)
        {
            paths.push(path.clone());
        }
    }
    if paths.len() == 1 { paths.pop() } else { None }
}

pub(super) fn select_receipt(package: &ResumePackageFacts, events: &[RunEvent]) -> Option<Receipt> {
    let ledger_receipt_ids = events
        .iter()
        .filter_map(|event| event.receipt_id.as_ref().map(ToString::to_string))
        .collect::<Vec<_>>();
    for receipt_id in ledger_receipt_ids.iter().rev() {
        if let Some(receipt) = package
            .receipts
            .iter()
            .rev()
            .find(|receipt| receipt.receipt_id.as_str() == receipt_id)
        {
            return Some(receipt.clone());
        }
    }
    package.receipts.last().cloned()
}

pub(super) fn resolve_project_path(root: &Path, value: &Path) -> PathBuf {
    if value.is_absolute() || value.starts_with(root) {
        value.to_path_buf()
    } else {
        root.join(value)
    }
}
