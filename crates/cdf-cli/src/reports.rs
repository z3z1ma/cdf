use std::{collections::BTreeMap, path::PathBuf};

use cdf_kernel::{SchemaObservationScope, TargetName, TerminalSchemaObservationQuarantine};
use cdf_project::{
    DiscoveryManifestArtifact, DiscoveryParticipation, PackageCollectionAction,
    PackageCollectionClassification, PackageCollectionPlan, ProjectDestinationDescription,
    ProjectReceiptSource, ProjectRunNoOpReport, ProjectRunReport,
};
use cdf_state_sqlite::{RunEventDetails, RunEventValue, RunLedgerSnapshot};
use serde::Serialize;

use cdf_cli_core::render::{
    RenderDocument,
    humanize::{humanize_bytes, humanize_duration, humanize_rate, humanize_rows},
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine},
    redaction::redact_uri_userinfo,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct SchemaSnapshotActionReport {
    pub(crate) outcome: &'static str,
    pub(crate) schema_hash: String,
    pub(crate) path: String,
    pub(crate) snapshot_written: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) discovery: Option<DiscoveryCoverageReport>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct DiscoveryCoverageReport {
    pub(crate) file_coverage: String,
    pub(crate) within_file_coverage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) selector: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) sample_files: Option<u64>,
    pub(crate) matched_files: usize,
    pub(crate) selected_files: usize,
    pub(crate) unobserved_files: usize,
}

impl DiscoveryCoverageReport {
    pub(crate) fn from_manifest(manifest: &DiscoveryManifestArtifact) -> Self {
        let selected_files = manifest
            .candidates
            .iter()
            .filter(|candidate| candidate.participation == DiscoveryParticipation::Observed)
            .count();
        Self {
            file_coverage: match manifest.file_coverage {
                cdf_project::DiscoveryFileCoverage::AllFiles => "all_files",
                cdf_project::DiscoveryFileCoverage::SampledFiles => "sampled_files",
            }
            .to_owned(),
            within_file_coverage: match manifest.within_file_coverage {
                cdf_project::DiscoveryWithinFileCoverage::FormatMetadata => "format_metadata",
                cdf_project::DiscoveryWithinFileCoverage::BoundedContent => "bounded_content",
                cdf_project::DiscoveryWithinFileCoverage::FullContent => "full_content",
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
            selected_files,
            unobserved_files: manifest.candidates.len() - selected_files,
        }
    }
}

pub(crate) fn discovery_coverage_panel(report: &DiscoveryCoverageReport) -> KeyValuePanel {
    KeyValuePanel::new("Discovery Coverage")
        .row("file coverage", report.file_coverage.clone())
        .row("within-file coverage", report.within_file_coverage.clone())
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
        .row("selected files", report.selected_files.to_string())
        .row("unobserved files", report.unobserved_files.to_string())
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
    #[serde(skip_serializing_if = "Option::is_none")]
    package_collection: Option<RunPackageCollectionReport>,
    schema_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_snapshot: Option<SchemaSnapshotActionReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_authority: Option<RunSchemaAuthorityReport>,
    checkpoint_id: String,
    checkpoint: RunCheckpointReport,
    receipt_id: String,
    receipt: RunReceiptReport,
    receipt_source: RunReceiptSourceReport,
    row_count: u64,
    byte_count: u64,
    segment_count: u64,
    admission: RunAdmissionReport,
    elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_manifest: Option<RunFileManifestReport>,
    terminal_schema_quarantines: Vec<TerminalSchemaObservationQuarantine>,
    #[serde(skip_serializing_if = "cdf_kernel::SourceTransferReport::is_empty")]
    source_transfer: cdf_kernel::SourceTransferReport,
    memory: RunMemoryReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    adhoc: Option<AdhocRunReport>,
    ledger_events: RunLedgerSummary,
    writes: WriteEffects,
    #[serde(skip)]
    explain_memory: bool,
}

impl RunCliReport {
    pub(crate) fn from_report(
        report: &ProjectRunReport,
        destination: RunDestinationReport,
        schema_snapshot: Option<SchemaSnapshotActionReport>,
        schema_authority: Option<RunSchemaAuthorityReport>,
        memory: RunMemoryReport,
    ) -> Self {
        let receipt_source_kind = destination.receipt_source_kind;
        let byte_count = report
            .receipt
            .segment_acks
            .iter()
            .try_fold(0_u64, |total, segment| {
                total.checked_add(segment.byte_count)
            })
            .unwrap_or(u64::MAX);
        let elapsed_ms = report
            .ledger_snapshot
            .events
            .first()
            .zip(report.ledger_snapshot.events.last())
            .map(|(first, last)| last.timestamp_ms.saturating_sub(first.timestamp_ms))
            .and_then(|elapsed| u64::try_from(elapsed).ok())
            .unwrap_or(0);
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
            package_collection: None,
            schema_hash: report.checkpoint.delta.schema_hash.to_string(),
            schema_snapshot,
            schema_authority,
            checkpoint_id: report.checkpoint.delta.checkpoint_id.to_string(),
            checkpoint: RunCheckpointReport::from_checkpoint(&report.checkpoint),
            receipt_id: report.receipt.receipt_id.to_string(),
            receipt: RunReceiptReport::from_report(report),
            receipt_source: RunReceiptSourceReport::from_project(
                &report.receipt_source,
                receipt_source_kind,
            ),
            row_count: report.row_count,
            byte_count,
            segment_count: report.segment_count,
            admission: RunAdmissionReport::from_project(report),
            elapsed_ms,
            file_manifest: report
                .file_manifest
                .as_ref()
                .map(RunFileManifestReport::from_project),
            terminal_schema_quarantines: report.terminal_schema_quarantines.clone(),
            source_transfer: report.source_transfer.clone(),
            memory,
            adhoc: None,
            ledger_events: RunLedgerSummary::from_snapshot(&report.ledger_snapshot),
            writes: run_write_effects(&report.receipt_source),
            explain_memory: false,
        }
    }

    pub(crate) fn with_adhoc(mut self, adhoc: AdhocRunReport) -> Self {
        self.adhoc = Some(adhoc);
        self
    }

    pub(crate) fn with_explain_memory(mut self, explain_memory: bool) -> Self {
        self.explain_memory = explain_memory;
        self
    }

    pub(crate) fn with_package_collection(
        mut self,
        package_collection: RunPackageCollectionReport,
    ) -> Self {
        self.package_collection = Some(package_collection);
        self
    }

    pub(crate) fn render_document(&self) -> RenderDocument {
        let elapsed = std::time::Duration::from_millis(self.elapsed_ms);
        let seconds = elapsed.as_secs_f64();
        let row_rate = (seconds > 0.0).then(|| {
            format!(
                "{}/s",
                humanize_rows((self.row_count as f64 / seconds) as u64)
            )
        });
        let byte_rate = (seconds > 0.0).then(|| humanize_rate(self.byte_count as f64 / seconds));
        let quarantine_count = self.terminal_schema_quarantines.len();
        let mut summary = KeyValuePanel::summary()
            .row("resource", self.resource_id.clone())
            .row("destination", self.destination.summary())
            .row("rows", humanize_rows(self.row_count))
            .row("data", humanize_bytes(self.byte_count))
            .row("segments", self.segment_count.to_string())
            .row("elapsed", humanize_duration(elapsed));
        if let Some(row_rate) = row_rate {
            summary = summary.row("row rate", row_rate);
        }
        if let Some(byte_rate) = byte_rate {
            summary = summary.row("byte rate", byte_rate);
        }
        if quarantine_count > 0 {
            summary = summary.row("quarantined files", quarantine_count.to_string());
        }
        if self.admission.accepted_with_residual_rows > 0 {
            summary = summary.row(
                "rows with residuals",
                humanize_rows(self.admission.accepted_with_residual_rows),
            );
        }
        if self.admission.quarantined_rows > 0 {
            summary = summary.row(
                "quarantined rows",
                humanize_rows(self.admission.quarantined_rows),
            );
        }
        let document = RenderDocument::new()
            .push(StatusLine::new(
                StatusKind::Success,
                format!(
                    "Loaded {} rows from {}",
                    humanize_rows(self.row_count),
                    self.resource_id
                ),
            ))
            .blank_line()
            .push(summary)
            .blank_line()
            .push(
                KeyValuePanel::proof()
                    .row("package", self.package_id.clone())
                    .row("receipt", self.receipt_id.clone())
                    .row("checkpoint", self.checkpoint_id.clone())
                    .row(
                        "gate",
                        if self.checkpoint.committed {
                            "committed"
                        } else {
                            "not committed"
                        },
                    ),
            )
            .blank_line()
            .push_verbose(
                KeyValuePanel::new("Run detail")
                    .row("run", self.run_id.clone())
                    .row("pipeline", self.pipeline_id.clone())
                    .row("target", self.target.clone()),
            )
            .blank_line()
            .push_verbose(
                KeyValuePanel::new("Package detail")
                    .row("status", self.package_status.clone())
                    .row("hash", self.package_hash.clone())
                    .row("schema", self.schema_hash.clone())
                    .row("dir", safe_display_value(&self.package_dir)),
            );
        let document = if let Some(collection) = &self.package_collection {
            document.blank_line().push(
                KeyValuePanel::new("Package retention")
                    .row("evaluated", collection.evaluated_packages.to_string())
                    .row("collected", collection.collected_packages.to_string())
                    .row(
                        "files reclaimed",
                        collection.reclaimed_file_count.to_string(),
                    )
                    .row(
                        "bytes reclaimed",
                        humanize_bytes(collection.reclaimed_byte_count),
                    )
                    .row("tombstoned", collection.tombstoned_packages.to_string()),
            )
        } else {
            document
        };
        let document = if let Some(snapshot) = &self.schema_snapshot {
            let document = document.blank_line().push_verbose(
                KeyValuePanel::new("Schema Snapshot")
                    .row("outcome", snapshot.outcome)
                    .row("hash", snapshot.schema_hash.clone())
                    .row("path", snapshot.path.clone())
                    .row("snapshot written", yes_no(snapshot.snapshot_written)),
            );
            if let Some(discovery) = &snapshot.discovery {
                document
                    .blank_line()
                    .push_verbose(discovery_coverage_panel(discovery))
            } else {
                document
            }
        } else {
            document
        };
        let document = if let Some(authority) = &self.schema_authority {
            document.blank_line().push_verbose(
                KeyValuePanel::new("Schema Authority")
                    .row("status", authority.status)
                    .row("generation", authority.generation.to_string())
                    .row("hash", authority.schema_hash.clone())
                    .row("drift", authority.drift),
            )
        } else {
            document
        };
        let document = document.blank_line().push_verbose(
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
            document.blank_line().push_verbose(panel)
        } else {
            document
        };
        let document = if self.source_transfer.is_empty() {
            document
        } else {
            let panel = self.source_transfer.modes.iter().fold(
                KeyValuePanel::new("Source Boundary").row(
                    "control events",
                    self.source_transfer.control_events.to_string(),
                ),
                |panel, mode| {
                    panel.row(
                        source_transfer_mode_name(mode.mode),
                        format!(
                            "{} batches · {} rows · zero-copy verified {} · known-copy {} ({} bytes) · copy unknown {}",
                            mode.batches,
                            humanize_rows(mode.rows),
                            mode.zero_copy_verified_batches,
                            mode.known_copy_batches,
                            mode.known_copy_bytes,
                            mode.unknown_copy_batches,
                        ),
                    )
                },
            );
            document.blank_line().push_verbose(panel)
        };
        let document = if self.explain_memory {
            document.blank_line().push(self.memory.panel())
        } else {
            document
        };
        let document = if self.terminal_schema_quarantines.is_empty() {
            document
        } else {
            self.terminal_schema_quarantines
                .iter()
                .fold(document, |document, quarantine| {
                    let fields = quarantine
                        .fields()
                        .iter()
                        .map(|field| {
                            let path = match field.scope() {
                                SchemaObservationScope::FieldPath { path } => path.join("."),
                                SchemaObservationScope::WholeSchema => "<schema>".to_owned(),
                                _ => "<schema-scope>".to_owned(),
                            };
                            let observed = field
                                .observed_field()
                                .map(|field| format!("{:?}", field.data_type))
                                .unwrap_or_else(|| "missing".to_owned());
                            let expected = field
                                .effective_field()
                                .map(|field| format!("{:?}", field.data_type))
                                .unwrap_or_else(|| "missing".to_owned());
                            format!("{path}: observed {observed}; expected {expected}")
                        })
                        .collect::<Vec<_>>()
                        .join(" | ");
                    document.blank_line().push(
                        KeyValuePanel::new("Schema Quarantine")
                            .row("file/observation", quarantine.observation_id().to_owned())
                            .row("rule", quarantine.rule_id().to_owned())
                            .row("disposition", "quarantine_partition")
                            .row("fields", fields)
                            .row("remediation", quarantine.remediation().to_owned()),
                    )
                })
        };
        let document = if let Some(adhoc) = &self.adhoc {
            document.blank_line().push(adhoc_resource_panel(adhoc))
        } else {
            document
        };
        document
            .blank_line()
            .push_verbose(
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
            .push_verbose(
                KeyValuePanel::new("Receipt")
                    .row("receipt", self.receipt_id.clone())
                    .row("destination", self.receipt.destination_id.clone())
                    .row("target", self.receipt.target.clone())
                    .row("disposition", self.receipt.disposition.clone())
                    .row("source", receipt_source_summary(&self.receipt_source)),
            )
            .blank_line()
            .push_verbose(
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
pub(crate) struct RunPackageCollectionReport {
    evaluated_packages: usize,
    collected_packages: usize,
    retained_packages: usize,
    tombstoned_packages: usize,
    reclaimed_file_count: u64,
    reclaimed_byte_count: u64,
    promotion_availability_impacts: usize,
}

impl RunPackageCollectionReport {
    pub(crate) fn from_plan(plan: &PackageCollectionPlan) -> Self {
        let mut report = Self {
            evaluated_packages: plan.artifacts.len(),
            collected_packages: 0,
            retained_packages: 0,
            tombstoned_packages: 0,
            reclaimed_file_count: 0,
            reclaimed_byte_count: 0,
            promotion_availability_impacts: plan
                .promotion_availability
                .iter()
                .filter(|availability| availability.collection_removes_last_local_promotable_copy)
                .count(),
        };
        for artifact in &plan.artifacts {
            match artifact.classification {
                PackageCollectionClassification::Collected => report.collected_packages += 1,
                PackageCollectionClassification::Tombstoned => report.tombstoned_packages += 1,
                PackageCollectionClassification::Retained
                | PackageCollectionClassification::Missing
                | PackageCollectionClassification::Corrupt
                | PackageCollectionClassification::Protected => report.retained_packages += 1,
                PackageCollectionClassification::Collectible => {
                    if artifact.planned_action == PackageCollectionAction::WouldCollect {
                        report.retained_packages += 1;
                    }
                }
            }
            report.reclaimed_file_count = report
                .reclaimed_file_count
                .saturating_add(artifact.reclaimed_file_count.unwrap_or(0));
            report.reclaimed_byte_count = report
                .reclaimed_byte_count
                .saturating_add(artifact.reclaimed_byte_count.unwrap_or(0));
        }
        report
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RunAdmissionReport {
    dispositions: cdf_contract::AdmissionPolicy,
    accepted_main_rows: u64,
    accepted_with_residual_rows: u64,
    quarantined_rows: u64,
    failed_resource_count: u64,
    terminal_quarantined_partitions: u64,
}

impl RunAdmissionReport {
    fn from_project(report: &ProjectRunReport) -> Self {
        Self {
            dispositions: report.admission.clone(),
            accepted_main_rows: report.verdict_summary.accepted_rows
                - report.verdict_summary.accepted_with_residual_rows,
            accepted_with_residual_rows: report.verdict_summary.accepted_with_residual_rows,
            quarantined_rows: report.verdict_summary.quarantined_rows,
            failed_resource_count: 0,
            terminal_quarantined_partitions: report.terminal_schema_quarantines.len() as u64,
        }
    }
}

fn source_transfer_mode_name(mode: cdf_kernel::SourceTransferMode) -> &'static str {
    match mode {
        cdf_kernel::SourceTransferMode::ArrowCData => "arrow c data",
        cdf_kernel::SourceTransferMode::ArrowIpcStream => "arrow ipc stream",
        cdf_kernel::SourceTransferMode::RowCompat => "row compatibility",
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RunSchemaAuthorityReport {
    pub(crate) status: &'static str,
    pub(crate) authority_domain_id: String,
    pub(crate) project_id: String,
    pub(crate) environment: String,
    pub(crate) resource_id: String,
    pub(crate) generation: u64,
    pub(crate) schema_hash: String,
    pub(crate) prepared_precondition: cdf_kernel::SchemaAuthorityPrecondition,
    pub(crate) drift: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RunNoOpCliReport {
    command: &'static str,
    run_id: String,
    resource_id: String,
    pipeline_id: String,
    destination: RunDestinationReport,
    reason: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    current_checkpoint_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_snapshot: Option<SchemaSnapshotActionReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_authority: Option<RunSchemaAuthorityReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_manifest: Option<RunFileManifestReport>,
    row_count: u64,
    segment_count: u64,
    memory: RunMemoryReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    adhoc: Option<AdhocRunReport>,
    ledger_events: RunLedgerSummary,
    writes: WriteEffects,
    #[serde(skip)]
    explain_memory: bool,
}

impl RunNoOpCliReport {
    pub(crate) fn from_report(
        report: &ProjectRunNoOpReport,
        resource_id: String,
        pipeline_id: String,
        destination: RunDestinationReport,
        schema_snapshot: Option<SchemaSnapshotActionReport>,
        schema_authority: Option<RunSchemaAuthorityReport>,
        memory: RunMemoryReport,
    ) -> Self {
        Self {
            command: "run",
            run_id: report.run_id.to_string(),
            resource_id,
            pipeline_id,
            destination,
            reason: report.reason.as_str(),
            current_checkpoint_id: report
                .current_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.delta.checkpoint_id.to_string()),
            schema_hash: report
                .current_checkpoint
                .as_ref()
                .map(|checkpoint| checkpoint.delta.schema_hash.to_string()),
            schema_snapshot,
            schema_authority,
            file_manifest: report
                .file_manifest
                .as_ref()
                .map(RunFileManifestReport::from_project),
            row_count: 0,
            segment_count: 0,
            memory,
            adhoc: None,
            ledger_events: RunLedgerSummary::from_snapshot(&report.ledger_snapshot),
            writes: WriteEffects::none(),
            explain_memory: false,
        }
    }

    pub(crate) fn with_adhoc(mut self, adhoc: AdhocRunReport) -> Self {
        self.adhoc = Some(adhoc);
        self
    }

    pub(crate) fn with_explain_memory(mut self, explain_memory: bool) -> Self {
        self.explain_memory = explain_memory;
        self
    }

    pub(crate) fn render_document(&self) -> RenderDocument {
        let document = RenderDocument::new()
            .push(StatusLine::new(
                StatusKind::Success,
                format!("No changes for {}", self.resource_id),
            ))
            .blank_line()
            .push(
                KeyValuePanel::summary()
                    .row("destination", self.destination.summary())
                    .row("outcome", "no-op")
                    .row("reason", self.reason),
            )
            .blank_line()
            .push_verbose(
                KeyValuePanel::new("Run detail")
                    .row("run", self.run_id.clone())
                    .row("pipeline", self.pipeline_id.clone()),
            );
        let document = match &self.current_checkpoint_id {
            Some(checkpoint) => {
                let panel =
                    KeyValuePanel::new("State").row("current checkpoint", checkpoint.clone());
                let panel = if let Some(schema_hash) = &self.schema_hash {
                    panel.row("schema", schema_hash.clone())
                } else {
                    panel
                };
                document.blank_line().push_verbose(panel)
            }
            None => document,
        };
        let document = if let Some(panel) = file_manifest_panel(self.file_manifest.as_ref()) {
            document.blank_line().push_verbose(panel)
        } else {
            document
        };
        let document = if self.explain_memory {
            document.blank_line().push(self.memory.panel())
        } else {
            document
        };
        let document = if let Some(adhoc) = &self.adhoc {
            document.blank_line().push(adhoc_resource_panel(adhoc))
        } else {
            document
        };
        document
            .blank_line()
            .push_verbose(
                KeyValuePanel::effects()
                    .row("package written", "no")
                    .row("destination written", "no")
                    .row("checkpoint written", "no")
                    .row("events", self.ledger_events.event_count.to_string()),
            )
            .blank_line()
            .push(NextCommand::new(format!("cdf inspect run {}", self.run_id)))
    }
}

fn adhoc_resource_panel(adhoc: &AdhocRunReport) -> KeyValuePanel {
    let panel = KeyValuePanel::new("Ad-hoc Resource")
        .row("resource", adhoc.resource_id.clone())
        .row("definition", adhoc.definition_path.clone())
        .row("reused", yes_no(adhoc.reused))
        .row("make permanent", adhoc.make_permanent_command.clone());
    match &adhoc.source_artifact_path {
        Some(path) => panel.row("staged source", path.clone()),
        None => panel,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RunMemoryReport {
    budget: crate::runtime_budget::RuntimeBudgetReport,
    managed: cdf_memory::MemorySnapshot,
}

impl RunMemoryReport {
    pub(crate) fn capture(
        budget: crate::runtime_budget::RuntimeBudgetReport,
        managed: cdf_memory::MemorySnapshot,
    ) -> Self {
        Self { budget, managed }
    }

    fn panel(&self) -> KeyValuePanel {
        let resolution = &self.budget.resolution;
        let authority = &self.budget.memory_authority;
        let mut panel = KeyValuePanel::new("Memory")
            .row(
                "process budget",
                humanize_bytes(resolution.process_budget_bytes),
            )
            .row(
                "native headroom",
                humanize_bytes(resolution.native_headroom_bytes),
            )
            .row(
                "managed pool",
                humanize_bytes(resolution.managed_pool_bytes),
            )
            .row("managed peak", humanize_bytes(self.managed.peak_bytes))
            .row(
                "spill budget",
                humanize_bytes(resolution.spill_budget_bytes),
            )
            .row("spilled", humanize_bytes(self.managed.spill_bytes))
            .row("flushes", self.managed.flushes.to_string())
            .row(
                "enforcement",
                match authority.enforcement {
                    crate::runtime_budget::MemoryEnforcement::LinuxCgroupV2 => "linux cgroup v2",
                    crate::runtime_budget::MemoryEnforcement::Unavailable => "unavailable",
                },
            );
        if let Some(cgroup) = &authority.cgroup_v2 {
            panel = panel
                .row(
                    "cgroup limit",
                    cgroup
                        .max_bytes
                        .map(humanize_bytes)
                        .unwrap_or_else(|| "unbounded".to_owned()),
                )
                .row(
                    "cgroup peak",
                    cgroup
                        .peak_bytes
                        .map(humanize_bytes)
                        .unwrap_or_else(|| "unavailable".to_owned()),
                );
        }
        if !authority.caveats.is_empty() {
            panel = panel.row("caveat", authority.caveats.join("; "));
        }
        panel
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct AdhocRunReport {
    pub(crate) resource_id: String,
    pub(crate) definition_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) source_artifact_path: Option<String>,
    pub(crate) reused: bool,
    pub(crate) make_permanent_command: String,
}

pub(crate) struct PreparedReplayReportRef<'a> {
    pub(crate) checkpoint: &'a cdf_kernel::Checkpoint,
    pub(crate) receipt: &'a cdf_kernel::Receipt,
    pub(crate) receipt_source: ProjectReceiptSource,
    pub(crate) package_status: &'a cdf_package_contract::PackageStatus,
    pub(crate) phase_metrics: &'a [cdf_kernel::RunPhaseMetric],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct ReplayPackageCliReport {
    command: &'static str,
    input_authority: &'static str,
    effect_ceiling: &'static str,
    run_id: String,
    package_id: String,
    package_dir: String,
    package_hash: String,
    schema_hash: String,
    row_count: u64,
    byte_count: u64,
    segment_count: usize,
    destination: RunDestinationReport,
    target: String,
    package_status: String,
    checkpoint_id: String,
    checkpoint: RunCheckpointReport,
    receipt_id: String,
    receipt: RunReceiptReport,
    receipt_source: RunReceiptSourceReport,
    phases: Vec<cdf_kernel::RunPhaseMetric>,
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
        let receipt_source_kind = destination.receipt_source_kind;
        let row_count = report
            .receipt
            .segment_acks
            .iter()
            .map(|segment| segment.row_count)
            .sum();
        let byte_count = report
            .receipt
            .segment_acks
            .iter()
            .map(|segment| segment.byte_count)
            .sum();
        Self {
            command: "run",
            input_authority: "package",
            effect_ceiling: "execute",
            run_id,
            package_id,
            package_dir: package_dir.display().to_string(),
            package_hash: report.receipt.package_hash.to_string(),
            schema_hash: report.checkpoint.delta.schema_hash.to_string(),
            row_count,
            byte_count,
            segment_count: report.receipt.segment_acks.len(),
            destination,
            target: report.receipt.target.to_string(),
            package_status: report.package_status.as_str().to_owned(),
            checkpoint_id: report.checkpoint.delta.checkpoint_id.to_string(),
            checkpoint: RunCheckpointReport::from_checkpoint(report.checkpoint),
            receipt_id: report.receipt.receipt_id.to_string(),
            receipt: RunReceiptReport::from_receipt(report.receipt),
            receipt_source: RunReceiptSourceReport::from_project(
                &receipt_source,
                receipt_source_kind,
            ),
            phases: report.phase_metrics.to_vec(),
            ledger_events: RunLedgerSummary::from_snapshot(ledger_snapshot),
            writes: WriteEffects {
                package: true,
                destination: true,
                checkpoint: true,
            },
        }
    }

    pub(crate) fn render_document(&self) -> RenderDocument {
        let duplicate = self
            .receipt_source
            .duplicate_no_op()
            .is_some_and(|(_, no_op)| no_op);
        let outcome = if duplicate {
            "no-op (package already loaded)"
        } else {
            "loaded"
        };
        RenderDocument::new()
            .push(StatusLine::new(
                StatusKind::Success,
                if duplicate {
                    format!("Package {} was already loaded", self.package_id)
                } else {
                    format!(
                        "Loaded {} rows from {}",
                        humanize_rows(self.row_count),
                        self.package_id
                    )
                },
            ))
            .blank_line()
            .push(
                KeyValuePanel::summary()
                    .row("outcome", outcome)
                    .row("input authority", self.input_authority)
                    .row("effect ceiling", self.effect_ceiling)
                    .row("destination", self.destination.summary())
                    .row("rows", humanize_rows(self.row_count))
                    .row("data", humanize_bytes(self.byte_count))
                    .row("segments", self.segment_count.to_string()),
            )
            .blank_line()
            .push(
                KeyValuePanel::proof()
                    .row("receipt", self.receipt_id.clone())
                    .row("checkpoint", self.checkpoint_id.clone())
                    .row(
                        "gate",
                        if self.checkpoint.committed {
                            "committed"
                        } else {
                            "not committed"
                        },
                    ),
            )
            .blank_line()
            .push_verbose(
                KeyValuePanel::new("Package run detail")
                    .row("run", self.run_id.clone())
                    .row("status", self.package_status.clone())
                    .row("hash", self.package_hash.clone())
                    .row("schema", self.schema_hash.clone())
                    .row("dir", safe_display_value(&self.package_dir))
                    .row("target", self.target.clone()),
            )
            .blank_line()
            .push_verbose(
                KeyValuePanel::new("Duplicate")
                    .row("source", receipt_source_summary(&self.receipt_source))
                    .row("duplicate", duplicate_value(&self.receipt_source))
                    .row("no-op", no_op_value(&self.receipt_source)),
            )
            .blank_line()
            .push_verbose(
                KeyValuePanel::new("Receipt")
                    .row("receipt", self.receipt_id.clone())
                    .row("destination", self.receipt.destination_id.clone())
                    .row("target", self.receipt.target.clone())
                    .row("rows", humanize_rows(self.receipt.counts.rows_written))
                    .row("segments", self.receipt.segment_ack_count.to_string()),
            )
            .blank_line()
            .push_verbose(
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
    kind: String,
    destination_id: Option<String>,
    target: String,
    #[serde(flatten)]
    product_fields: BTreeMap<String, String>,
    #[serde(skip)]
    display_label: String,
    #[serde(skip)]
    receipt_source_kind: &'static str,
}

impl RunDestinationReport {
    pub(crate) fn from_project(
        description: &ProjectDestinationDescription,
        target: &TargetName,
    ) -> Self {
        let kind = description
            .schemes
            .first()
            .copied()
            .unwrap_or("destination")
            .to_owned();
        // This is the single output boundary for driver-provided location labels: both
        // structured and human reports consume the same redacted value from here onward.
        let display_label = redact_uri_userinfo(&description.label);
        let mut product_fields = BTreeMap::new();
        if let Some(field) = description.product_location_field {
            product_fields.insert(field.to_owned(), display_label.clone());
        }
        Self {
            kind,
            destination_id: None,
            target: target.to_string(),
            product_fields,
            display_label,
            receipt_source_kind: description.product_receipt_source,
        }
    }

    pub(crate) fn with_receipt_destination(mut self, destination_id: String) -> Self {
        self.destination_id = Some(destination_id);
        self
    }

    fn summary(&self) -> String {
        let destination = self.destination_id.as_deref().unwrap_or(&self.kind);
        if self.display_label == self.kind {
            format!("{destination} target {}", self.target)
        } else {
            format!(
                "{} {} target {}",
                destination,
                safe_display_value(&self.display_label),
                self.target
            )
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
struct RunReceiptSourceReport {
    kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    duplicate: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    no_op: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    package_receipt_recorded: Option<bool>,
}

impl RunReceiptSourceReport {
    fn from_project(source: &ProjectReceiptSource, receipt_source_kind: &'static str) -> Self {
        match source {
            ProjectReceiptSource::DestinationCommit {
                duplicate,
                package_receipt_recorded,
            } => Self {
                kind: receipt_source_kind,
                duplicate: Some(*duplicate),
                no_op: Some(*duplicate),
                package_receipt_recorded: Some(*package_receipt_recorded),
            },
            ProjectReceiptSource::DestinationCommitReceiptOnly {
                package_receipt_recorded,
            } => Self {
                kind: "destination_commit_receipt_only",
                duplicate: None,
                no_op: None,
                package_receipt_recorded: Some(*package_receipt_recorded),
            },
            ProjectReceiptSource::SuppliedDurableReceipt => Self {
                kind: "supplied_durable_receipt",
                duplicate: None,
                no_op: None,
                package_receipt_recorded: None,
            },
        }
    }

    fn duplicate_no_op(&self) -> Option<(bool, bool)> {
        self.no_op
            .map(|no_op| (self.duplicate.unwrap_or(false), no_op))
    }

    fn kind_name(&self) -> &'static str {
        self.kind
    }
}

pub(crate) fn replay_event_details(
    source: &ProjectReceiptSource,
    receipt_source_kind: &str,
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
            attributes.insert(
                "receipt_source".to_owned(),
                RunEventValue::String(receipt_source_kind.to_owned()),
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RunFileManifestReport {
    total_file_count: u64,
    changed_file_count: u64,
    unchanged_file_count: u64,
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

    fn test_memory_report() -> RunMemoryReport {
        let cli = cdf_cli_core::args::Cli::parse(["cdf", "version"].map(std::ffi::OsString::from))
            .unwrap();
        let budget = crate::runtime_budget::resolve(&cli).unwrap();
        let managed = cdf_memory::MemorySnapshot {
            budget_bytes: budget.resolution.managed_pool_bytes,
            peak_bytes: 1024,
            ..cdf_memory::MemorySnapshot::default()
        };
        RunMemoryReport::capture(budget, managed)
    }

    #[test]
    fn run_rendering_redacts_secret_like_destination_uri_userinfo() {
        let report = RunCliReport {
            command: "run",
            run_id: "run-redacted".to_owned(),
            resource_id: "local.events".to_owned(),
            pipeline_id: "pipeline".to_owned(),
            target: "events".to_owned(),
            destination: RunDestinationReport::from_project(
                &ProjectDestinationDescription::new(
                    cdf_kernel::DestinationId::new("duckdb").unwrap(),
                    &["duckdb"],
                    "postgres://user:secret-value@localhost/db",
                )
                .with_product_location_field("database_path")
                .with_product_receipt_source("duck_db_commit"),
                &TargetName::new("events").unwrap(),
            )
            .with_receipt_destination("duckdb".to_owned()),
            package_id: "pkg-redacted".to_owned(),
            package_dir: ".cdf/packages/pkg-redacted".to_owned(),
            package_hash: "sha256:package".to_owned(),
            package_status: "checkpointed".to_owned(),
            package_collection: None,
            schema_hash: "sha256:schema".to_owned(),
            schema_snapshot: None,
            schema_authority: None,
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
            receipt_source: RunReceiptSourceReport {
                kind: "duck_db_commit",
                duplicate: Some(false),
                no_op: Some(false),
                package_receipt_recorded: Some(true),
            },
            row_count: 2,
            byte_count: 256,
            segment_count: 1,
            admission: RunAdmissionReport {
                dispositions: cdf_contract::ContractPolicy::default().admission,
                accepted_main_rows: 2,
                accepted_with_residual_rows: 0,
                quarantined_rows: 0,
                failed_resource_count: 0,
                terminal_quarantined_partitions: 0,
            },
            elapsed_ms: 1,
            file_manifest: None,
            terminal_schema_quarantines: Vec::new(),
            source_transfer: cdf_kernel::SourceTransferReport::default(),
            memory: test_memory_report(),
            adhoc: None,
            ledger_events: RunLedgerSummary::default(),
            writes: WriteEffects::all(),
            explain_memory: false,
        };

        let rendered = report
            .render_document()
            .render(&cdf_cli_core::render::RenderConfig::headless_for_width(96));

        assert!(!rendered.contains("secret-value"));
        assert!(rendered.contains("postgres://[redacted]@localhost/db"));

        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.contains("secret-value"));
        assert!(json.contains(r#""database_path":"postgres://[redacted]@localhost/db""#));
    }
}
