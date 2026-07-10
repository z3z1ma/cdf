use std::{collections::BTreeMap, path::PathBuf};

use cdf_kernel::TargetName;
use cdf_project::{
    DiscoveryManifestArtifact, DiscoveryParticipation, ProjectDestinationDescription,
    ProjectReceiptSource, ProjectRunReport,
};
use cdf_state_sqlite::{RunEventDetails, RunEventValue, RunLedgerSnapshot};
use serde::Serialize;

use crate::render::{
    RenderDocument,
    humanize::humanize_rows,
    primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine},
    redaction::redact_uri_userinfo,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaSnapshotActionReport {
    pub(crate) outcome: &'static str,
    pub(crate) schema_hash: String,
    pub(crate) path: String,
    pub(crate) snapshot_written: bool,
    pub(crate) lockfile_written: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) discovery: Option<DiscoveryCoverageReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct DiscoveryCoverageReport {
    pub(crate) coverage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) selector: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sample_files: Option<u64>,
    pub(crate) matched_files: usize,
    pub(crate) probed_files: usize,
    pub(crate) unprobed_files: usize,
}

impl DiscoveryCoverageReport {
    pub(crate) fn from_manifest(manifest: &DiscoveryManifestArtifact) -> Self {
        let probed_files = manifest
            .candidates
            .iter()
            .filter(|candidate| candidate.participation == DiscoveryParticipation::Probed)
            .count();
        Self {
            coverage: match manifest.coverage {
                cdf_project::DiscoveryCoverageMode::Exhaustive => "exhaustive",
                cdf_project::DiscoveryCoverageMode::Sampled => "sampled",
            }
            .to_owned(),
            selector: manifest
                .selector
                .as_ref()
                .map(|selector| selector.selector.clone()),
            sample_files: manifest
                .selector
                .as_ref()
                .map(|selector| selector.sample_files),
            matched_files: manifest.candidates.len(),
            probed_files,
            unprobed_files: manifest.candidates.len() - probed_files,
        }
    }
}

pub(crate) fn discovery_coverage_panel(report: &DiscoveryCoverageReport) -> KeyValuePanel {
    KeyValuePanel::new("Discovery Coverage")
        .row("coverage", report.coverage.clone())
        .row(
            "selector",
            report.selector.clone().unwrap_or_else(|| "none".to_owned()),
        )
        .row(
            "sample files",
            report
                .sample_files
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned()),
        )
        .row("matched files", report.matched_files.to_string())
        .row("probed files", report.probed_files.to_string())
        .row("unprobed files", report.unprobed_files.to_string())
}

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
    schema_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_snapshot: Option<SchemaSnapshotActionReport>,
    checkpoint_id: String,
    checkpoint: RunCheckpointReport,
    receipt_id: String,
    receipt: RunReceiptReport,
    receipt_source: RunReceiptSourceReport,
    row_count: u64,
    segment_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_manifest: Option<RunFileManifestReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    adhoc: Option<AdhocRunReport>,
    ledger_events: RunLedgerSummary,
    writes: WriteEffects,
}

impl RunCliReport {
    pub(crate) fn from_report(
        report: &ProjectRunReport,
        destination: RunDestinationReport,
        schema_snapshot: Option<SchemaSnapshotActionReport>,
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
            schema_hash: report.checkpoint.delta.schema_hash.to_string(),
            schema_snapshot,
            checkpoint_id: report.checkpoint.delta.checkpoint_id.to_string(),
            checkpoint: RunCheckpointReport::from_checkpoint(&report.checkpoint),
            receipt_id: report.receipt.receipt_id.to_string(),
            receipt: RunReceiptReport::from_report(report),
            receipt_source: RunReceiptSourceReport::from_project(
                &report.receipt_source,
                destination_kind,
            ),
            row_count: report.row_count,
            segment_count: report.segment_count,
            file_manifest: report
                .file_manifest
                .as_ref()
                .map(RunFileManifestReport::from_project),
            adhoc: None,
            ledger_events: RunLedgerSummary::from_snapshot(&report.ledger_snapshot),
            writes: run_write_effects(&report.receipt_source),
        }
    }

    pub(crate) fn with_adhoc(mut self, adhoc: AdhocRunReport) -> Self {
        self.adhoc = Some(adhoc);
        self
    }

    pub(crate) fn render_document(&self) -> RenderDocument {
        let document = RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                StatusKind::Success,
                format!("run {} completed for {}", self.run_id, self.resource_id),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Run")
                    .row("run", self.run_id.clone())
                    .row("resource", self.resource_id.clone())
                    .row("pipeline", self.pipeline_id.clone())
                    .row("target", self.target.clone())
                    .row("destination", self.destination.summary()),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Package")
                    .row("package", self.package_id.clone())
                    .row("status", self.package_status.clone())
                    .row("hash", self.package_hash.clone())
                    .row("schema", self.schema_hash.clone())
                    .row("dir", safe_display_value(&self.package_dir)),
            );
        let document = if let Some(snapshot) = &self.schema_snapshot {
            let document = document.blank_line().push(
                KeyValuePanel::new("Schema Snapshot")
                    .row("outcome", snapshot.outcome)
                    .row("hash", snapshot.schema_hash.clone())
                    .row("path", snapshot.path.clone())
                    .row("snapshot written", yes_no(snapshot.snapshot_written))
                    .row("lockfile written", yes_no(snapshot.lockfile_written)),
            );
            if let Some(discovery) = &snapshot.discovery {
                document
                    .blank_line()
                    .push(discovery_coverage_panel(discovery))
            } else {
                document
            }
        } else {
            document
        };
        let document = document.blank_line().push(
            KeyValuePanel::new("Rows")
                .row("rows", humanize_rows(self.row_count))
                .row("segments", self.segment_count.to_string())
                .row(
                    "receipt rows",
                    humanize_rows(self.receipt.counts.rows_written),
                )
                .row(
                    "receipt segments",
                    self.receipt.segment_ack_count.to_string(),
                ),
        );
        let document = if let Some(panel) = file_manifest_panel(self.file_manifest.as_ref()) {
            document.blank_line().push(panel)
        } else {
            document
        };
        let document = if let Some(adhoc) = &self.adhoc {
            let panel = KeyValuePanel::new("Ad-hoc Resource")
                .row("resource", adhoc.resource_id.clone())
                .row("config", adhoc.config_path.clone())
                .row("reused", yes_no(adhoc.reused))
                .row("make permanent", adhoc.make_permanent_command.clone());
            let panel = match &adhoc.source_artifact_path {
                Some(path) => panel.row("staged source", path.clone()),
                None => panel,
            };
            document.blank_line().push(panel)
        } else {
            document
        };
        document
            .blank_line()
            .push(
                KeyValuePanel::new("Verdicts")
                    .row("package", self.package_status.clone())
                    .row("checkpoint", self.checkpoint.status.clone())
                    .row(
                        "ledger terminal",
                        self.ledger_events
                            .terminal_kind
                            .clone()
                            .unwrap_or_else(|| "none".to_owned()),
                    )
                    .row("events", self.ledger_events.event_count.to_string()),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Receipt")
                    .row("receipt", self.receipt_id.clone())
                    .row("destination", self.receipt.destination_id.clone())
                    .row("target", self.receipt.target.clone())
                    .row("disposition", self.receipt.disposition.clone())
                    .row("source", receipt_source_summary(&self.receipt_source)),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Gate")
                    .row("checkpoint", self.checkpoint_id.clone())
                    .row("committed", yes_no(self.checkpoint.committed))
                    .row("head", yes_no(self.checkpoint.is_head))
                    .row("package written", yes_no(self.writes.package))
                    .row("destination written", yes_no(self.writes.destination))
                    .row("checkpoint written", yes_no(self.writes.checkpoint))
                    .row(
                        "condition",
                        "destination receipt verified before checkpoint commit",
                    ),
            )
            .blank_line()
            .push(NextCommand::new(format!("cdf inspect run {}", self.run_id)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct AdhocRunReport {
    pub(crate) resource_id: String,
    pub(crate) config_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_artifact_path: Option<String>,
    pub(crate) reused: bool,
    pub(crate) make_permanent_command: String,
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
            checkpoint: RunCheckpointReport::from_checkpoint(report.checkpoint),
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

    pub(crate) fn render_document(&self) -> RenderDocument {
        RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                StatusKind::Success,
                format!("replay package {} completed", self.package_id),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Replay")
                    .row("run", self.run_id.clone())
                    .row("package", self.package_id.clone())
                    .row("status", self.package_status.clone())
                    .row("hash", self.package_hash.clone())
                    .row("dir", safe_display_value(&self.package_dir)),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Destination")
                    .row("destination", self.destination.summary())
                    .row("target", self.target.clone()),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Duplicate")
                    .row("source", receipt_source_summary(&self.receipt_source))
                    .row("duplicate", duplicate_value(&self.receipt_source))
                    .row("no-op", no_op_value(&self.receipt_source)),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Receipt")
                    .row("receipt", self.receipt_id.clone())
                    .row("destination", self.receipt.destination_id.clone())
                    .row("target", self.receipt.target.clone())
                    .row("rows", humanize_rows(self.receipt.counts.rows_written))
                    .row("segments", self.receipt.segment_ack_count.to_string()),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Checkpoint")
                    .row("checkpoint", self.checkpoint_id.clone())
                    .row("status", self.checkpoint.status.clone())
                    .row("committed", yes_no(self.checkpoint.committed))
                    .row("head", yes_no(self.checkpoint.is_head))
                    .row(
                        "ledger terminal",
                        self.ledger_events
                            .terminal_kind
                            .clone()
                            .unwrap_or_else(|| "none".to_owned()),
                    ),
            )
            .blank_line()
            .push(NextCommand::new(format!("cdf inspect run {}", self.run_id)))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RunCheckpointReport {
    checkpoint_id: String,
    status: String,
    committed: bool,
    is_head: bool,
    committed_at_ms: Option<i64>,
}

impl RunCheckpointReport {
    pub(crate) fn from_checkpoint(checkpoint: &cdf_kernel::Checkpoint) -> Self {
        Self {
            checkpoint_id: checkpoint.delta.checkpoint_id.to_string(),
            status: checkpoint.status.as_str().to_owned(),
            committed: checkpoint.committed_at_ms.is_some(),
            is_head: checkpoint.is_head,
            committed_at_ms: checkpoint.committed_at_ms,
        }
    }
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
    pub(crate) fn from_project(
        description: &ProjectDestinationDescription,
        target: &TargetName,
    ) -> Self {
        match description
            .schemes
            .first()
            .copied()
            .unwrap_or("destination")
        {
            "duckdb" => Self::duckdb(description.label.clone(), target.to_string()),
            "parquet" => Self::parquet(description.label.clone(), target.to_string()),
            "postgres" => Self::postgres(target.to_string()),
            _ => Self {
                kind: "destination",
                destination_id: None,
                target: target.to_string(),
                database_path: None,
                root: None,
            },
        }
    }

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

    fn summary(&self) -> String {
        let destination = self.destination_id.as_deref().unwrap_or(self.kind);
        match self.kind {
            "duckdb" => format!(
                "{} {} target {}",
                destination,
                self.database_path
                    .as_deref()
                    .map(safe_display_value)
                    .unwrap_or_else(|| "unknown".to_owned()),
                self.target
            ),
            "parquet" => format!(
                "{} {} target {}",
                destination,
                self.root
                    .as_deref()
                    .map(safe_display_value)
                    .unwrap_or_else(|| "unknown".to_owned()),
                self.target
            ),
            _ => format!("{destination} target {}", self.target),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RunReceiptReport {
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

    pub(crate) fn from_receipt(receipt: &cdf_kernel::Receipt) -> Self {
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
    FileManifestNoChangedFiles {
        no_op: bool,
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
            ProjectReceiptSource::FileManifestNoChangedFiles => {
                Self::FileManifestNoChangedFiles { no_op: true }
            }
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
            Self::FileManifestNoChangedFiles { no_op } => Some((false, *no_op)),
            Self::DestinationCommitReceiptOnly { .. } | Self::SuppliedDurableReceipt => None,
        }
    }

    fn kind_name(&self) -> &'static str {
        match self {
            Self::DuckDbCommit { .. } => "duck_db_commit",
            Self::DestinationCommit { .. } => "destination_commit",
            Self::DestinationCommitReceiptOnly { .. } => "destination_commit_receipt_only",
            Self::FileManifestNoChangedFiles { .. } => "file_manifest_no_changed_files",
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
        ProjectReceiptSource::FileManifestNoChangedFiles => {
            attributes.insert(
                "receipt_source".to_owned(),
                RunEventValue::String("file_manifest_no_changed_files".to_owned()),
            );
            attributes.insert("duplicate".to_owned(), RunEventValue::Bool(false));
            attributes.insert("no_op".to_owned(), RunEventValue::Bool(true));
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RunFileManifestReport {
    total_file_count: usize,
    changed_file_count: usize,
    unchanged_file_count: usize,
    no_changed_files: bool,
}

impl RunFileManifestReport {
    fn from_project(summary: &cdf_project::FileManifestRunSummary) -> Self {
        Self {
            total_file_count: summary.total_file_count,
            changed_file_count: summary.changed_file_count,
            unchanged_file_count: summary.unchanged_file_count,
            no_changed_files: summary.total_file_count > 0 && summary.changed_file_count == 0,
        }
    }
}

fn file_manifest_panel(summary: Option<&RunFileManifestReport>) -> Option<KeyValuePanel> {
    summary.map(|summary| {
        KeyValuePanel::new("Files")
            .row("total", summary.total_file_count.to_string())
            .row("changed", summary.changed_file_count.to_string())
            .row("unchanged", summary.unchanged_file_count.to_string())
            .row("no changed files", yes_no(summary.no_changed_files))
    })
}

fn run_write_effects(source: &ProjectReceiptSource) -> WriteEffects {
    match source {
        ProjectReceiptSource::FileManifestNoChangedFiles => WriteEffects {
            package: false,
            destination: false,
            checkpoint: false,
        },
        ProjectReceiptSource::DestinationCommit { .. }
        | ProjectReceiptSource::DestinationCommitReceiptOnly { .. }
        | ProjectReceiptSource::SuppliedDurableReceipt => WriteEffects {
            package: true,
            destination: true,
            checkpoint: true,
        },
    }
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

fn duplicate_value(source: &RunReceiptSourceReport) -> String {
    source
        .duplicate_no_op()
        .map(|(duplicate, _)| yes_no(duplicate).to_owned())
        .unwrap_or_else(|| "not reported".to_owned())
}

fn no_op_value(source: &RunReceiptSourceReport) -> String {
    source
        .duplicate_no_op()
        .map(|(_, no_op)| yes_no(no_op).to_owned())
        .unwrap_or_else(|| "not reported".to_owned())
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn safe_display_value(value: &str) -> String {
    redact_uri_userinfo(value)
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
    details: RunEventDetails,
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
            details: event.details.clone(),
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

impl WriteEffects {
    pub(crate) fn none() -> Self {
        Self {
            package: false,
            destination: false,
            checkpoint: false,
        }
    }

    pub(crate) fn all() -> Self {
        Self {
            package: true,
            destination: true,
            checkpoint: true,
        }
    }

    pub(crate) fn package(&self) -> bool {
        self.package
    }

    pub(crate) fn destination(&self) -> bool {
        self.destination
    }

    pub(crate) fn checkpoint(&self) -> bool {
        self.checkpoint
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_rendering_redacts_secret_like_destination_uri_userinfo() {
        let report = RunCliReport {
            command: "run",
            run_id: "run-redacted".to_owned(),
            resource_id: "local.events".to_owned(),
            pipeline_id: "pipeline".to_owned(),
            target: "events".to_owned(),
            destination: RunDestinationReport::duckdb(
                "postgres://user:secret-value@localhost/db".to_owned(),
                "events".to_owned(),
            )
            .with_receipt_destination("duckdb".to_owned()),
            package_id: "pkg-redacted".to_owned(),
            package_dir: ".cdf/packages/pkg-redacted".to_owned(),
            package_hash: "sha256:package".to_owned(),
            package_status: "checkpointed".to_owned(),
            schema_hash: "sha256:schema".to_owned(),
            schema_snapshot: None,
            checkpoint_id: "checkpoint-redacted".to_owned(),
            checkpoint: RunCheckpointReport {
                checkpoint_id: "checkpoint-redacted".to_owned(),
                status: "committed".to_owned(),
                committed: true,
                is_head: true,
                committed_at_ms: Some(1),
            },
            receipt_id: "receipt-redacted".to_owned(),
            receipt: RunReceiptReport {
                receipt_id: "receipt-redacted".to_owned(),
                destination_id: "duckdb".to_owned(),
                target: "events".to_owned(),
                package_hash: "sha256:package".to_owned(),
                disposition: "append".to_owned(),
                committed_at_ms: 1,
                segment_ack_count: 1,
                counts: cdf_kernel::CommitCounts {
                    rows_written: 2,
                    rows_inserted: None,
                    rows_updated: None,
                    rows_deleted: None,
                },
            },
            receipt_source: RunReceiptSourceReport::DestinationCommit {
                duplicate: false,
                no_op: false,
                package_receipt_recorded: true,
            },
            row_count: 2,
            segment_count: 1,
            file_manifest: None,
            adhoc: None,
            ledger_events: RunLedgerSummary::default(),
            writes: WriteEffects::all(),
        };

        let rendered = report
            .render_document()
            .render(&crate::render::RenderConfig::headless_for_width(96));

        assert!(!rendered.contains("secret-value"));
        assert!(rendered.contains("postgres://[redacted]@localhost/db"));
    }
}
