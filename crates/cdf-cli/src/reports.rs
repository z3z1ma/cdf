use std::{collections::BTreeMap, path::PathBuf};

use cdf_project::{ProjectReceiptSource, ProjectRunReport};
use cdf_state_sqlite::{RunEventDetails, RunEventValue, RunLedgerSnapshot};
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RunCliReport {
    command: &'static str,
    run_id: String,
    resource_id: String,
    pipeline_id: String,
    target: String,
    destination: RunDestinationReport,
    package_id: String,
    package_dir: String,
    package_hash: String,
    package_status: String,
    checkpoint_id: String,
    checkpoint: RunCheckpointReport,
    receipt_id: String,
    receipt: RunReceiptReport,
    receipt_source: RunReceiptSourceReport,
    row_count: u64,
    segment_count: usize,
    ledger_events: RunLedgerSummary,
    writes: WriteEffects,
}

impl RunCliReport {
    pub(crate) fn from_report(
        report: &ProjectRunReport,
        destination: RunDestinationReport,
    ) -> Self {
        let destination_kind = destination.kind;
        Self {
            command: "run",
            run_id: report.run_id.to_string(),
            resource_id: report.checkpoint.delta.resource_id.to_string(),
            pipeline_id: report.checkpoint.delta.pipeline_id.to_string(),
            target: report.receipt.target.to_string(),
            destination: destination
                .with_receipt_destination(report.receipt.destination.to_string()),
            package_id: report.package_id.clone(),
            package_dir: report.package_dir.display().to_string(),
            package_hash: report.package_hash.to_string(),
            package_status: report.package_status.as_str().to_owned(),
            checkpoint_id: report.checkpoint.delta.checkpoint_id.to_string(),
            checkpoint: RunCheckpointReport {
                checkpoint_id: report.checkpoint.delta.checkpoint_id.to_string(),
                status: report.checkpoint.status.as_str().to_owned(),
                committed: report.checkpoint.committed_at_ms.is_some(),
                is_head: report.checkpoint.is_head,
                committed_at_ms: report.checkpoint.committed_at_ms,
            },
            receipt_id: report.receipt.receipt_id.to_string(),
            receipt: RunReceiptReport::from_report(report),
            receipt_source: RunReceiptSourceReport::from_project(
                &report.receipt_source,
                destination_kind,
            ),
            row_count: report.row_count,
            segment_count: report.segment_count,
            ledger_events: RunLedgerSummary::from_snapshot(&report.ledger_snapshot),
            writes: WriteEffects {
                package: true,
                destination: true,
                checkpoint: true,
            },
        }
    }

    pub(crate) fn human_message(&self) -> String {
        format!(
            "ran resource {} as run {} into package {} for target {}; checkpoint {} committed after destination receipt verification, crossing the commit gate",
            self.resource_id, self.run_id, self.package_hash, self.target, self.checkpoint_id
        )
    }
}

pub(crate) struct PreparedReplayReportRef<'a> {
    pub(crate) checkpoint: &'a cdf_kernel::Checkpoint,
    pub(crate) receipt: &'a cdf_kernel::Receipt,
    pub(crate) receipt_source: ProjectReceiptSource,
    pub(crate) package_status: &'a cdf_package::PackageStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ReplayPackageCliReport {
    command: &'static str,
    run_id: String,
    package_id: String,
    package_dir: String,
    package_hash: String,
    destination: RunDestinationReport,
    target: String,
    package_status: String,
    checkpoint_id: String,
    checkpoint: RunCheckpointReport,
    receipt_id: String,
    receipt: RunReceiptReport,
    receipt_source: RunReceiptSourceReport,
    ledger_events: RunLedgerSummary,
    writes: WriteEffects,
}

impl ReplayPackageCliReport {
    pub(crate) fn from_report(
        run_id: String,
        package_id: String,
        package_dir: PathBuf,
        report: PreparedReplayReportRef<'_>,
        receipt_source: ProjectReceiptSource,
        destination: RunDestinationReport,
        ledger_snapshot: &RunLedgerSnapshot,
    ) -> Self {
        let destination_kind = destination.kind;
        Self {
            command: "replay package",
            run_id,
            package_id,
            package_dir: package_dir.display().to_string(),
            package_hash: report.receipt.package_hash.to_string(),
            destination,
            target: report.receipt.target.to_string(),
            package_status: report.package_status.as_str().to_owned(),
            checkpoint_id: report.checkpoint.delta.checkpoint_id.to_string(),
            checkpoint: RunCheckpointReport {
                checkpoint_id: report.checkpoint.delta.checkpoint_id.to_string(),
                status: report.checkpoint.status.as_str().to_owned(),
                committed: report.checkpoint.committed_at_ms.is_some(),
                is_head: report.checkpoint.is_head,
                committed_at_ms: report.checkpoint.committed_at_ms,
            },
            receipt_id: report.receipt.receipt_id.to_string(),
            receipt: RunReceiptReport::from_receipt(report.receipt),
            receipt_source: RunReceiptSourceReport::from_project(&receipt_source, destination_kind),
            ledger_events: RunLedgerSummary::from_snapshot(ledger_snapshot),
            writes: WriteEffects {
                package: true,
                destination: true,
                checkpoint: true,
            },
        }
    }

    pub(crate) fn human_message(&self) -> String {
        format!(
            "replayed package {} into destination {} target {}; receipt {} from {}; checkpoint {} status {}; package status {}",
            self.package_hash,
            self.destination
                .destination_id
                .as_deref()
                .unwrap_or("unknown"),
            self.target,
            self.receipt_id,
            receipt_source_summary(&self.receipt_source),
            self.checkpoint_id,
            self.checkpoint.status,
            self.package_status
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RunCheckpointReport {
    checkpoint_id: String,
    status: String,
    committed: bool,
    is_head: bool,
    committed_at_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RunDestinationReport {
    kind: &'static str,
    destination_id: Option<String>,
    target: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    database_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    root: Option<String>,
}

impl RunDestinationReport {
    pub(crate) fn duckdb(database_path: String, target: String) -> Self {
        Self {
            kind: "duckdb",
            destination_id: None,
            target,
            database_path: Some(database_path),
            root: None,
        }
    }

    pub(crate) fn parquet(root: String, target: String) -> Self {
        Self {
            kind: "parquet",
            destination_id: None,
            target,
            database_path: None,
            root: Some(root),
        }
    }

    pub(crate) fn postgres(target: String) -> Self {
        Self {
            kind: "postgres",
            destination_id: None,
            target,
            database_path: None,
            root: None,
        }
    }

    pub(crate) fn with_receipt_destination(mut self, destination_id: String) -> Self {
        self.destination_id = Some(destination_id);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RunReceiptReport {
    receipt_id: String,
    destination_id: String,
    target: String,
    package_hash: String,
    disposition: String,
    committed_at_ms: i64,
    segment_ack_count: usize,
    counts: cdf_kernel::CommitCounts,
}

impl RunReceiptReport {
    fn from_report(report: &ProjectRunReport) -> Self {
        Self::from_receipt(&report.receipt)
    }

    fn from_receipt(receipt: &cdf_kernel::Receipt) -> Self {
        Self {
            receipt_id: receipt.receipt_id.to_string(),
            destination_id: receipt.destination.to_string(),
            target: receipt.target.to_string(),
            package_hash: receipt.package_hash.to_string(),
            disposition: write_disposition_name(&receipt.disposition).to_owned(),
            committed_at_ms: receipt.committed_at_ms,
            segment_ack_count: receipt.segment_acks.len(),
            counts: receipt.counts.clone(),
        }
    }
}

fn write_disposition_name(disposition: &cdf_kernel::WriteDisposition) -> &'static str {
    match disposition {
        cdf_kernel::WriteDisposition::Append => "append",
        cdf_kernel::WriteDisposition::Replace => "replace",
        cdf_kernel::WriteDisposition::Merge => "merge",
        cdf_kernel::WriteDisposition::CdcApply => "cdc_apply",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RunReceiptSourceReport {
    DuckDbCommit {
        duplicate: bool,
        no_op: bool,
        package_receipt_recorded: bool,
    },
    DestinationCommit {
        duplicate: bool,
        no_op: bool,
        package_receipt_recorded: bool,
    },
    DestinationCommitReceiptOnly {
        package_receipt_recorded: bool,
    },
    SuppliedDurableReceipt,
}

impl RunReceiptSourceReport {
    fn from_project(source: &ProjectReceiptSource, destination_kind: &str) -> Self {
        match source {
            ProjectReceiptSource::DestinationCommit {
                duplicate,
                package_receipt_recorded,
            } if destination_kind == "duckdb" => Self::DuckDbCommit {
                duplicate: *duplicate,
                no_op: *duplicate,
                package_receipt_recorded: *package_receipt_recorded,
            },
            ProjectReceiptSource::DestinationCommit {
                duplicate,
                package_receipt_recorded,
            } => Self::DestinationCommit {
                duplicate: *duplicate,
                no_op: *duplicate,
                package_receipt_recorded: *package_receipt_recorded,
            },
            ProjectReceiptSource::DestinationCommitReceiptOnly {
                package_receipt_recorded,
            } => Self::DestinationCommitReceiptOnly {
                package_receipt_recorded: *package_receipt_recorded,
            },
            ProjectReceiptSource::SuppliedDurableReceipt => Self::SuppliedDurableReceipt,
        }
    }

    fn duplicate_no_op(&self) -> Option<(bool, bool)> {
        match self {
            Self::DuckDbCommit {
                duplicate, no_op, ..
            }
            | Self::DestinationCommit {
                duplicate, no_op, ..
            } => Some((*duplicate, *no_op)),
            Self::DestinationCommitReceiptOnly { .. } | Self::SuppliedDurableReceipt => None,
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::DuckDbCommit { .. } => "duck_db_commit",
            Self::DestinationCommit { .. } => "destination_commit",
            Self::DestinationCommitReceiptOnly { .. } => "destination_commit_receipt_only",
            Self::SuppliedDurableReceipt => "supplied_durable_receipt",
        }
    }
}

pub(crate) fn replay_event_details(
    source: &ProjectReceiptSource,
    destination_kind: &str,
    package_status: &str,
) -> RunEventDetails {
    let mut attributes = BTreeMap::from([(
        "package_status".to_owned(),
        RunEventValue::String(package_status.to_owned()),
    )]);
    match source {
        ProjectReceiptSource::DestinationCommit {
            duplicate,
            package_receipt_recorded,
        } => {
            let receipt_source = if destination_kind == "duckdb" {
                "duck_db_commit"
            } else {
                "destination_commit"
            };
            attributes.insert(
                "receipt_source".to_owned(),
                RunEventValue::String(receipt_source.to_owned()),
            );
            attributes.insert("duplicate".to_owned(), RunEventValue::Bool(*duplicate));
            attributes.insert("no_op".to_owned(), RunEventValue::Bool(*duplicate));
            attributes.insert(
                "package_receipt_recorded".to_owned(),
                RunEventValue::Bool(*package_receipt_recorded),
            );
        }
        ProjectReceiptSource::DestinationCommitReceiptOnly {
            package_receipt_recorded,
        } => {
            attributes.insert(
                "receipt_source".to_owned(),
                RunEventValue::String("destination_commit_receipt_only".to_owned()),
            );
            attributes.insert(
                "package_receipt_recorded".to_owned(),
                RunEventValue::Bool(*package_receipt_recorded),
            );
        }
        ProjectReceiptSource::SuppliedDurableReceipt => {
            attributes.insert(
                "receipt_source".to_owned(),
                RunEventValue::String("supplied_durable_receipt".to_owned()),
            );
        }
    }
    RunEventDetails { attributes }
}

fn receipt_source_summary(source: &RunReceiptSourceReport) -> String {
    match source.duplicate_no_op() {
        Some((duplicate, no_op)) => {
            format!(
                "{} duplicate={} no_op={}",
                source.kind_name(),
                duplicate,
                no_op
            )
        }
        None => source.kind_name().to_owned(),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
struct RunLedgerSummary {
    event_count: usize,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    terminal_kind: Option<String>,
    kinds: BTreeMap<String, usize>,
    events: Vec<RunLedgerEventSummary>,
}

impl RunLedgerSummary {
    fn from_snapshot(snapshot: &RunLedgerSnapshot) -> Self {
        let mut kinds = BTreeMap::new();
        for event in &snapshot.events {
            *kinds.entry(event.kind.as_str().to_owned()).or_insert(0) += 1;
        }
        Self {
            event_count: snapshot.events.len(),
            first_sequence: snapshot.events.first().map(|event| event.sequence),
            last_sequence: snapshot.events.last().map(|event| event.sequence),
            terminal_kind: snapshot
                .events
                .last()
                .map(|event| event.kind.as_str().to_owned()),
            kinds,
            events: snapshot
                .events
                .iter()
                .map(RunLedgerEventSummary::from_event)
                .collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RunLedgerEventSummary {
    sequence: u64,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    resource_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    package_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    package_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination_id: Option<String>,
}

impl RunLedgerEventSummary {
    fn from_event(event: &cdf_state_sqlite::RunEvent) -> Self {
        Self {
            sequence: event.sequence,
            kind: event.kind.as_str().to_owned(),
            resource_id: event.resource_id.as_ref().map(ToString::to_string),
            package_id: event.package_id.clone(),
            package_hash: event.package_hash.as_ref().map(ToString::to_string),
            checkpoint_id: event.checkpoint_id.as_ref().map(ToString::to_string),
            receipt_id: event.receipt_id.as_ref().map(ToString::to_string),
            destination_id: event.destination_id.as_ref().map(ToString::to_string),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub(crate) struct WriteEffects {
    package: bool,
    destination: bool,
    checkpoint: bool,
}
