use std::path::{Path, PathBuf};

use cdf_kernel::{CdfError, RunId, ScopeKey};
use cdf_package::PackageReader;
use cdf_state_sqlite::{
    RunEvent, RunEventDetails, RunEventKind, RunEventValue, RunLedgerSnapshot, SqliteRunLedger,
};
use serde::Serialize;

use crate::{
    context::ProjectContext,
    output::{CliError, CommandOutput},
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, SectionRule, StatusKind, StatusLine, Table},
        redaction::redact_uri_userinfo,
    },
};

pub(crate) fn inspect_run(context: &ProjectContext, id: String) -> Result<CommandOutput, CliError> {
    let run_id = RunId::new(id)?;
    let state_path = context.state_store_path()?;
    let ledger = SqliteRunLedger::open_read_only(&state_path)?;
    let snapshot = ledger.snapshot(&run_id)?.ok_or_else(|| {
        CdfError::data(format!(
            "run {} is not present in the selected environment run ledger",
            run_id
        ))
    })?;
    let report = InspectRunReport::from_snapshot(context, &snapshot)?;
    CommandOutput::rendered("inspect run", report.render_document(), report)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct InspectRunReport {
    command: &'static str,
    run_id: String,
    created_at_ms: i64,
    terminal_state: String,
    terminal_kind: Option<String>,
    recovery: RecoveryGuidanceReport,
    pointers: RunPointerReport,
    artifacts: RunArtifactReport,
    duplicate: DuplicateStatusReport,
    events: Vec<InspectRunEventReport>,
    writes: InspectRunWrites,
}

impl InspectRunReport {
    fn from_snapshot(
        context: &ProjectContext,
        snapshot: &RunLedgerSnapshot,
    ) -> Result<Self, CliError> {
        let pointers = RunPointerReport::from_events(&snapshot.events)?;
        let packages = inspect_package_artifacts(context, &snapshot.events);
        let checkpoint = CheckpointAvailabilityReport::from_events(&snapshot.events);
        let receipt =
            ReceiptAvailabilityReport::from_pointers_and_packages(&pointers.receipt_ids, &packages);
        let duplicate = DuplicateStatusReport::from_events(&snapshot.events);
        let latest_package_status = latest_package_status(&packages, &snapshot.events);
        let artifacts = RunArtifactReport {
            package_status: latest_package_status
                .clone()
                .unwrap_or_else(|| "not_recorded".to_owned()),
            packages,
            receipt,
            checkpoint,
        };
        let terminal_kind = snapshot
            .events
            .last()
            .map(|event| event.kind.as_str().to_owned());
        let terminal_state = terminal_state(snapshot.events.last());
        let recovery =
            RecoveryGuidanceReport::from_facts(&snapshot.events, &artifacts, &terminal_state);

        Ok(Self {
            command: "inspect run",
            run_id: snapshot.run.run_id.to_string(),
            created_at_ms: snapshot.run.created_at_ms,
            terminal_state,
            terminal_kind,
            recovery,
            pointers,
            artifacts,
            duplicate,
            events: snapshot
                .events
                .iter()
                .map(InspectRunEventReport::from_event)
                .collect(),
            writes: InspectRunWrites::default(),
        })
    }

    fn render_document(&self) -> RenderDocument {
        let missing_package_count = self
            .artifacts
            .packages
            .iter()
            .filter(|package| package.status != "available")
            .count();
        let missing_receipts = self.artifacts.receipt.missing_receipt_ids.len();
        let mut document = RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                StatusKind::Success,
                format!("run {} terminal {}", self.run_id, self.terminal_state),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Recovery")
                    .row("state", self.recovery.state.clone())
                    .row("action", self.recovery.action.clone())
                    .row("source contact", yes_no(self.recovery.source_contact))
                    .row("mutation required", yes_no(self.recovery.mutation_required))
                    .row("guidance", self.recovery.guidance.clone())
                    .row("next command", self.recovery_next_command()),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Artifacts")
                    .row("package status", self.artifacts.package_status.clone())
                    .row("packages", self.artifacts.packages.len().to_string())
                    .row("missing packages", missing_package_count.to_string())
                    .row("receipt status", self.artifacts.receipt.status.clone())
                    .row("missing receipts", missing_receipts.to_string())
                    .row(
                        "checkpoint status",
                        self.artifacts.checkpoint.status.clone(),
                    )
                    .row(
                        "checkpoint committed",
                        yes_no(self.artifacts.checkpoint.committed),
                    ),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Pointers")
                    .row("resources", list_or_none(&self.pointers.resource_ids))
                    .row("packages", list_or_none(&self.pointers.package_ids))
                    .row("checkpoints", list_or_none(&self.pointers.checkpoint_ids))
                    .row("receipts", list_or_none(&self.pointers.receipt_ids))
                    .row("destinations", list_or_none(&self.pointers.destination_ids)),
            )
            .blank_line()
            .push(
                KeyValuePanel::new("Duplicate")
                    .row("status", self.duplicate.status.clone())
                    .row("duplicate", optional_bool(self.duplicate.duplicate))
                    .row("no-op", optional_bool(self.duplicate.no_op))
                    .row(
                        "source event",
                        self.duplicate
                            .source_event_sequence
                            .map(|value| value.to_string())
                            .unwrap_or_else(|| "none".to_owned()),
                    ),
            );

        if self.artifacts.packages.is_empty() {
            document = document.blank_line().push(
                KeyValuePanel::new("Package artifacts")
                    .row("status", "not recorded")
                    .row("missing", "no package path is recorded in the run ledger"),
            );
        } else {
            let first_path = self
                .artifacts
                .packages
                .first()
                .map(|package| safe_display_value(&package.path))
                .unwrap_or_else(|| "none".to_owned());
            let first_issue = self
                .artifacts
                .packages
                .iter()
                .find_map(|package| package.reason.clone())
                .unwrap_or_else(|| "none".to_owned());
            document = document.blank_line().push(
                KeyValuePanel::new("Package artifacts")
                    .row("recorded", self.artifacts.packages.len().to_string())
                    .row("missing", missing_package_count.to_string())
                    .row("first path", first_path)
                    .row("first issue", first_issue),
            );
            let table = self.artifacts.packages.iter().fold(
                Table::new(["path", "status", "receipt artifact", "reason"]),
                |table, package| {
                    table.row([
                        safe_display_value(&package.path),
                        package.status.clone(),
                        package.receipt_artifact_status.clone(),
                        package
                            .reason
                            .clone()
                            .unwrap_or_else(|| "available".to_owned()),
                    ])
                },
            );
            document = document.blank_line().push(table);
        }

        let table = self.events.iter().fold(
            Table::new(["seq", "kind", "package", "checkpoint", "receipt"]),
            |table, event| {
                table.row([
                    event.sequence.to_string(),
                    event.kind.clone(),
                    event.package_id.clone().unwrap_or_else(|| "-".to_owned()),
                    event
                        .checkpoint_id
                        .clone()
                        .unwrap_or_else(|| "-".to_owned()),
                    event.receipt_id.clone().unwrap_or_else(|| "-".to_owned()),
                ])
            },
        );

        document
            .blank_line()
            .push(table)
            .blank_line()
            .push(NextCommand::new(self.recovery_next_command()))
    }

    fn recovery_next_command(&self) -> String {
        match self.recovery.action.as_str() {
            "rerun_extraction_from_last_committed_checkpoint" => self
                .pointers
                .resource_ids
                .first()
                .map(|resource| format!("cdf run {resource}"))
                .unwrap_or_else(|| "cdf run <resource>".to_owned()),
            "replay_package_without_source_contact"
            | "verify_receipt_then_commit_checkpoint"
            | "update_package_status" => format!("cdf resume {}", self.run_id),
            _ => format!("cdf inspect run {}", self.run_id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct InspectRunEventReport {
    sequence: u64,
    timestamp_ms: i64,
    kind: String,
    resource_id: Option<String>,
    scope: Option<ScopeKey>,
    partition_id: Option<String>,
    package_id: Option<String>,
    package_hash: Option<String>,
    package_path: Option<String>,
    checkpoint_id: Option<String>,
    receipt_id: Option<String>,
    destination_id: Option<String>,
    plan_id: Option<String>,
    details: RunEventDetails,
}

impl InspectRunEventReport {
    fn from_event(event: &RunEvent) -> Self {
        Self {
            sequence: event.sequence,
            timestamp_ms: event.timestamp_ms,
            kind: event.kind.as_str().to_owned(),
            resource_id: event.resource_id.as_ref().map(ToString::to_string),
            scope: event.scope.clone(),
            partition_id: event.partition_id.as_ref().map(ToString::to_string),
            package_id: event.package_id.clone(),
            package_hash: event.package_hash.as_ref().map(ToString::to_string),
            package_path: event.package_path.clone(),
            checkpoint_id: event.checkpoint_id.as_ref().map(ToString::to_string),
            receipt_id: event.receipt_id.as_ref().map(ToString::to_string),
            destination_id: event.destination_id.as_ref().map(ToString::to_string),
            plan_id: event.plan_id.as_ref().map(ToString::to_string),
            details: redact_details(&event.details),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
struct RunPointerReport {
    resource_ids: Vec<String>,
    scopes: Vec<serde_json::Value>,
    partition_ids: Vec<String>,
    package_ids: Vec<String>,
    package_hashes: Vec<String>,
    package_paths: Vec<String>,
    checkpoint_ids: Vec<String>,
    receipt_ids: Vec<String>,
    destination_ids: Vec<String>,
    plan_ids: Vec<String>,
}

impl RunPointerReport {
    fn from_events(events: &[RunEvent]) -> Result<Self, CliError> {
        let mut report = Self::default();
        let mut scope_keys = Vec::<String>::new();
        for event in events {
            if let Some(value) = &event.resource_id {
                push_unique(&mut report.resource_ids, value.to_string());
            }
            if let Some(scope) = &event.scope {
                let value = serde_json::to_value(scope).map_err(crate::commands::json_cli_error)?;
                let key = serde_json::to_string(&value).map_err(crate::commands::json_cli_error)?;
                if !scope_keys.contains(&key) {
                    scope_keys.push(key);
                    report.scopes.push(value);
                }
            }
            if let Some(value) = &event.partition_id {
                push_unique(&mut report.partition_ids, value.to_string());
            }
            if let Some(value) = &event.package_id {
                push_unique(&mut report.package_ids, value.clone());
            }
            if let Some(value) = &event.package_hash {
                push_unique(&mut report.package_hashes, value.to_string());
            }
            if let Some(value) = &event.package_path {
                push_unique(&mut report.package_paths, value.clone());
            }
            if let Some(value) = &event.checkpoint_id {
                push_unique(&mut report.checkpoint_ids, value.to_string());
            }
            if let Some(value) = &event.receipt_id {
                push_unique(&mut report.receipt_ids, value.to_string());
            }
            if let Some(value) = &event.destination_id {
                push_unique(&mut report.destination_ids, value.to_string());
            }
            if let Some(value) = &event.plan_id {
                push_unique(&mut report.plan_ids, value.to_string());
            }
        }
        Ok(report)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RunArtifactReport {
    package_status: String,
    packages: Vec<PackageAvailabilityReport>,
    receipt: ReceiptAvailabilityReport,
    checkpoint: CheckpointAvailabilityReport,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct PackageAvailabilityReport {
    path: String,
    status: String,
    ledger_package_id: Option<String>,
    ledger_package_hash: Option<String>,
    manifest_package_id: Option<String>,
    manifest_package_hash: Option<String>,
    lifecycle_status: Option<String>,
    segment_count: Option<usize>,
    receipt_artifact_status: String,
    receipt_ids: Vec<String>,
    reason: Option<String>,
}

impl PackageAvailabilityReport {
    fn inspect(context: &ProjectContext, pointer: PackagePointer) -> Self {
        let resolved = resolve_project_path(&context.root, &pointer.path);
        if !resolved.exists() {
            return Self {
                path: pointer.path,
                status: "missing".to_owned(),
                ledger_package_id: pointer.package_id,
                ledger_package_hash: pointer.package_hash,
                manifest_package_id: None,
                manifest_package_hash: None,
                lifecycle_status: None,
                segment_count: None,
                receipt_artifact_status: "unavailable".to_owned(),
                receipt_ids: Vec::new(),
                reason: Some("package path recorded in the run ledger does not exist".to_owned()),
            };
        }

        let reader = match PackageReader::open(&resolved) {
            Ok(reader) => reader,
            Err(error) => {
                return Self {
                    path: pointer.path,
                    status: "unavailable".to_owned(),
                    ledger_package_id: pointer.package_id,
                    ledger_package_hash: pointer.package_hash,
                    manifest_package_id: None,
                    manifest_package_hash: None,
                    lifecycle_status: None,
                    segment_count: None,
                    receipt_artifact_status: "unavailable".to_owned(),
                    receipt_ids: Vec::new(),
                    reason: Some(error.to_string()),
                };
            }
        };
        let manifest = reader.manifest();
        let mut segment_count = 0_usize;
        if let Err(error) = reader.for_each_identity_segment(&mut |_| {
            segment_count = segment_count.saturating_add(1);
            Ok(())
        }) {
            return Self {
                path: pointer.path,
                status: "unavailable".to_owned(),
                ledger_package_id: pointer.package_id,
                ledger_package_hash: pointer.package_hash,
                manifest_package_id: Some(manifest.identity.package_id.clone()),
                manifest_package_hash: Some(manifest.package_hash.clone()),
                lifecycle_status: Some(manifest.lifecycle.status.as_str().to_owned()),
                segment_count: None,
                receipt_artifact_status: "unavailable".to_owned(),
                receipt_ids: Vec::new(),
                reason: Some(error.to_string()),
            };
        }
        let mut receipt_ids = Vec::new();
        match reader.for_each_receipt(&mut |receipt| {
            receipt_ids.push(receipt.receipt_id.to_string());
            Ok(())
        }) {
            Ok(_) => Self {
                path: pointer.path,
                status: "available".to_owned(),
                ledger_package_id: pointer.package_id,
                ledger_package_hash: pointer.package_hash,
                manifest_package_id: Some(manifest.identity.package_id.clone()),
                manifest_package_hash: Some(manifest.package_hash.clone()),
                lifecycle_status: Some(manifest.lifecycle.status.as_str().to_owned()),
                segment_count: Some(segment_count),
                receipt_artifact_status: if receipt_ids.is_empty() {
                    "missing".to_owned()
                } else {
                    "available".to_owned()
                },
                receipt_ids,
                reason: None,
            },
            Err(error) => Self {
                path: pointer.path,
                status: "available".to_owned(),
                ledger_package_id: pointer.package_id,
                ledger_package_hash: pointer.package_hash,
                manifest_package_id: Some(manifest.identity.package_id.clone()),
                manifest_package_hash: Some(manifest.package_hash.clone()),
                lifecycle_status: Some(manifest.lifecycle.status.as_str().to_owned()),
                segment_count: Some(segment_count),
                receipt_artifact_status: "unavailable".to_owned(),
                receipt_ids: Vec::new(),
                reason: Some(error.to_string()),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct ReceiptAvailabilityReport {
    status: String,
    ledger_receipt_ids: Vec<String>,
    package_receipt_ids: Vec<String>,
    missing_receipt_ids: Vec<String>,
    reason: Option<String>,
}

impl ReceiptAvailabilityReport {
    fn from_pointers_and_packages(
        ledger_receipt_ids: &[String],
        packages: &[PackageAvailabilityReport],
    ) -> Self {
        let mut package_receipt_ids = Vec::new();
        for package in packages {
            for receipt_id in &package.receipt_ids {
                push_unique(&mut package_receipt_ids, receipt_id.clone());
            }
        }
        let missing_receipt_ids = ledger_receipt_ids
            .iter()
            .filter(|receipt_id| !package_receipt_ids.contains(receipt_id))
            .cloned()
            .collect::<Vec<_>>();

        let (status, reason) = if ledger_receipt_ids.is_empty() {
            ("not_recorded".to_owned(), None)
        } else if missing_receipt_ids.is_empty() {
            ("available".to_owned(), None)
        } else if packages.is_empty() {
            (
                "unavailable".to_owned(),
                Some("no package path is recorded for receipt artifact lookup".to_owned()),
            )
        } else if packages
            .iter()
            .any(|package| package.status == "missing" || package.status == "unavailable")
        {
            (
                "unavailable".to_owned(),
                Some("one or more package artifacts are unavailable".to_owned()),
            )
        } else {
            (
                "missing".to_owned(),
                Some("ledger receipt id is absent from package receipt artifacts".to_owned()),
            )
        };

        Self {
            status,
            ledger_receipt_ids: ledger_receipt_ids.to_vec(),
            package_receipt_ids,
            missing_receipt_ids,
            reason,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct CheckpointAvailabilityReport {
    status: String,
    checkpoint_ids: Vec<String>,
    committed: bool,
    reason: Option<String>,
}

impl CheckpointAvailabilityReport {
    fn from_events(events: &[RunEvent]) -> Self {
        let mut checkpoint_ids = Vec::new();
        let mut proposed = false;
        let mut committed = false;
        for event in events {
            if let Some(checkpoint_id) = &event.checkpoint_id {
                push_unique(&mut checkpoint_ids, checkpoint_id.to_string());
            }
            match event.kind {
                RunEventKind::CheckpointProposed => proposed = true,
                RunEventKind::CheckpointCommitted => committed = true,
                _ => {}
            }
        }
        let status = if committed {
            "committed"
        } else if proposed {
            "proposed"
        } else {
            "not_recorded"
        }
        .to_owned();
        let reason = if status == "not_recorded" && !checkpoint_ids.is_empty() {
            Some(
                "checkpoint id is present but no checkpoint transition event is recorded"
                    .to_owned(),
            )
        } else {
            None
        };
        Self {
            status,
            checkpoint_ids,
            committed,
            reason,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct DuplicateStatusReport {
    status: String,
    duplicate: Option<bool>,
    no_op: Option<bool>,
    source_event_sequence: Option<u64>,
}

impl DuplicateStatusReport {
    fn from_events(events: &[RunEvent]) -> Self {
        for event in events.iter().rev() {
            let duplicate = detail_bool(&event.details, "duplicate");
            let no_op = detail_bool(&event.details, "no_op");
            if duplicate.is_some() || no_op.is_some() {
                return Self {
                    status: match duplicate {
                        Some(true) => "duplicate",
                        Some(false) => "not_duplicate",
                        None => "unknown",
                    }
                    .to_owned(),
                    duplicate,
                    no_op,
                    source_event_sequence: Some(event.sequence),
                };
            }
        }
        Self {
            status: "unknown".to_owned(),
            duplicate: None,
            no_op: None,
            source_event_sequence: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RecoveryGuidanceReport {
    state: String,
    action: String,
    source_contact: bool,
    mutation_required: bool,
    guidance: String,
}

impl RecoveryGuidanceReport {
    fn from_facts(
        events: &[RunEvent],
        artifacts: &RunArtifactReport,
        terminal_state: &str,
    ) -> Self {
        let package_finalized = events
            .iter()
            .any(|event| event.kind == RunEventKind::PackageFinalized);
        let run_failed = events
            .iter()
            .any(|event| event.kind == RunEventKind::RunFailed);
        let receipt_recorded = !artifacts.receipt.ledger_receipt_ids.is_empty();
        let checkpoint_committed = artifacts.checkpoint.status == "committed";
        let package_checkpointed = artifacts.package_status == "checkpointed";

        if terminal_state == "succeeded" {
            return Self {
                state: "terminal_success".to_owned(),
                action: "no_op".to_owned(),
                source_contact: false,
                mutation_required: false,
                guidance: "terminal successful run; no recovery action is required".to_owned(),
            };
        }
        if run_failed && !package_finalized {
            return Self {
                state: "failed_before_finalized_package".to_owned(),
                action: "rerun_extraction_from_last_committed_checkpoint".to_owned(),
                source_contact: true,
                mutation_required: true,
                guidance:
                    "failed before a finalized package; rerun extraction from the last committed checkpoint"
                        .to_owned(),
            };
        }
        if package_finalized && !receipt_recorded {
            return Self {
                state: "package_finalized_without_receipt".to_owned(),
                action: "replay_package_without_source_contact".to_owned(),
                source_contact: false,
                mutation_required: true,
                guidance: "package is finalized but no receipt is recorded; replay the package without contacting the source".to_owned(),
            };
        }
        if receipt_recorded && !checkpoint_committed {
            if package_checkpointed {
                return Self {
                    state: "receipt_and_checkpoint_pointer_without_commit_event".to_owned(),
                    action: "inspect_missing_artifacts".to_owned(),
                    source_contact: false,
                    mutation_required: false,
                    guidance: "receipt/checkpoint pointers exist and package status is checkpointed, but no checkpoint commit event is recorded; inspect state artifacts before mutating".to_owned(),
                };
            }
            return Self {
                state: "receipt_recorded_without_checkpoint_commit".to_owned(),
                action: "verify_receipt_then_commit_checkpoint".to_owned(),
                source_contact: false,
                mutation_required: true,
                guidance: "receipt is recorded but checkpoint commit is not recorded; verify the receipt, then commit the checkpoint without contacting the source".to_owned(),
            };
        }
        if checkpoint_committed && !package_checkpointed {
            return Self {
                state: "checkpoint_committed_with_stale_package_status".to_owned(),
                action: "update_package_status".to_owned(),
                source_contact: false,
                mutation_required: true,
                guidance: "checkpoint is committed but package status is not checkpointed; update package status only".to_owned(),
            };
        }
        Self {
            state: "incomplete_or_missing_artifacts".to_owned(),
            action: "inspect_missing_artifacts".to_owned(),
            source_contact: false,
            mutation_required: false,
            guidance: "run evidence is incomplete; inspect missing artifacts/statuses before choosing recovery".to_owned(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
struct InspectRunWrites {
    package: bool,
    destination: bool,
    checkpoint: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PackagePointer {
    path: String,
    package_id: Option<String>,
    package_hash: Option<String>,
}

fn inspect_package_artifacts(
    context: &ProjectContext,
    events: &[RunEvent],
) -> Vec<PackageAvailabilityReport> {
    package_pointers(events)
        .into_iter()
        .map(|pointer| PackageAvailabilityReport::inspect(context, pointer))
        .collect()
}

fn package_pointers(events: &[RunEvent]) -> Vec<PackagePointer> {
    let mut pointers = Vec::<PackagePointer>::new();
    for event in events {
        let Some(path) = &event.package_path else {
            continue;
        };
        match pointers.iter_mut().find(|pointer| pointer.path == *path) {
            Some(pointer) => {
                if pointer.package_id.is_none() {
                    pointer.package_id = event.package_id.clone();
                }
                if pointer.package_hash.is_none() {
                    pointer.package_hash = event.package_hash.as_ref().map(ToString::to_string);
                }
            }
            None => pointers.push(PackagePointer {
                path: path.clone(),
                package_id: event.package_id.clone(),
                package_hash: event.package_hash.as_ref().map(ToString::to_string),
            }),
        }
    }
    pointers
}

fn latest_package_status(
    packages: &[PackageAvailabilityReport],
    events: &[RunEvent],
) -> Option<String> {
    packages
        .iter()
        .rev()
        .find_map(|package| package.lifecycle_status.clone())
        .or_else(|| {
            events.iter().rev().find_map(|event| {
                detail_string(&event.details, "status")
                    .or_else(|| detail_string(&event.details, "package_status"))
            })
        })
}

fn terminal_state(last_event: Option<&RunEvent>) -> String {
    match last_event.map(|event| event.kind) {
        Some(RunEventKind::RunSucceeded) => "succeeded",
        Some(RunEventKind::RunFailed) => "failed",
        Some(RunEventKind::ReplayRecorded) => "replay_recorded",
        _ => "incomplete",
    }
    .to_owned()
}

fn detail_bool(details: &RunEventDetails, key: &str) -> Option<bool> {
    match details.attributes.get(key) {
        Some(RunEventValue::Bool(value)) => Some(*value),
        _ => None,
    }
}

fn detail_string(details: &RunEventDetails, key: &str) -> Option<String> {
    match details.attributes.get(key) {
        Some(RunEventValue::String(value)) => Some(value.clone()),
        _ => None,
    }
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}

fn resolve_project_path(root: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() || path.starts_with(root) {
        path
    } else {
        root.join(path)
    }
}

fn redact_details(details: &RunEventDetails) -> RunEventDetails {
    RunEventDetails {
        attributes: details
            .attributes
            .iter()
            .map(|(key, value)| (key.clone(), redact_event_value(key, value)))
            .collect(),
    }
}

fn redact_event_value(key: &str, value: &RunEventValue) -> RunEventValue {
    if is_sensitive_key(key) && !value_contains_only_secret_refs(value) {
        return RunEventValue::String("[redacted]".to_owned());
    }
    match value {
        RunEventValue::List(values) => RunEventValue::List(
            values
                .iter()
                .map(|value| redact_event_value(key, value))
                .collect(),
        ),
        RunEventValue::Object(values) => RunEventValue::Object(
            values
                .iter()
                .map(|(nested_key, value)| {
                    (nested_key.clone(), redact_event_value(nested_key, value))
                })
                .collect(),
        ),
        value => value.clone(),
    }
}

fn value_contains_only_secret_refs(value: &RunEventValue) -> bool {
    match value {
        RunEventValue::SecretRef(_) => true,
        RunEventValue::List(values) => values.iter().all(value_contains_only_secret_refs),
        RunEventValue::Object(values) => values.values().all(value_contains_only_secret_refs),
        RunEventValue::Bool(_)
        | RunEventValue::I64(_)
        | RunEventValue::U64(_)
        | RunEventValue::String(_)
        | RunEventValue::PhaseMetric(_) => false,
    }
}

fn is_sensitive_key(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    key.contains("secret")
        || key.contains("token")
        || key.contains("password")
        || key.contains("credential")
        || key.contains("authorization")
        || key.contains("api_key")
        || key.contains("apikey")
        || key.contains("connection_string")
        || key.contains("dsn")
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn optional_bool(value: Option<bool>) -> String {
    value.map(yes_no).unwrap_or("unknown").to_owned()
}

fn list_or_none(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_owned()
    } else {
        values
            .iter()
            .map(|value| safe_display_value(value))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn safe_display_value(value: &str) -> String {
    redact_uri_userinfo(value)
}
