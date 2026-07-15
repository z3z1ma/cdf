#![allow(dead_code)] // 10x: progress still has grammar/backfill-only helpers until WS5C and display flag wiring land.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::Mutex,
    time::Duration,
};

use cdf_kernel::{
    RunEvent, RunEventKind, RunEventSink, RunEventSinkResult, RunEventValue, ScopeKey,
};

use crate::render::{
    RenderConfig, RenderDocument,
    config::DisplayMode,
    humanize::{humanize_bytes, humanize_duration, humanize_rows},
    primitives::{KeyValuePanel, SectionRule, StatusKind, StatusLine, Table},
    redaction::{is_sensitive_key, redact_uri_userinfo, redacted},
};
use crate::terminal::{OutputChannel, TerminalPolicy, Verbosity};

const DEFAULT_PROGRESS_CAPACITY: usize = 128;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum DisplayVerbosity {
    Quiet,
    #[default]
    Normal,
    Verbose,
}

impl DisplayVerbosity {
    fn records_milestone(self, status: ProgressStatus) -> bool {
        match self {
            Self::Quiet => status.is_terminal(),
            Self::Normal | Self::Verbose => true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProgressConfig {
    render: RenderConfig,
    verbosity: DisplayVerbosity,
    capacity: usize,
}

impl ProgressConfig {
    pub(crate) fn new(render: RenderConfig, verbosity: DisplayVerbosity) -> Self {
        Self {
            render,
            verbosity,
            capacity: DEFAULT_PROGRESS_CAPACITY,
        }
    }

    pub(crate) fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity.max(1);
        self
    }

    pub(crate) fn render_config(&self) -> &RenderConfig {
        &self.render
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProgressPhase {
    Plan,
    Extract,
    Validate,
    Package,
    Commit,
    Verify,
    Gate,
}

impl ProgressPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Extract => "extract",
            Self::Validate => "validate",
            Self::Package => "package",
            Self::Commit => "commit",
            Self::Verify => "verify",
            Self::Gate => "gate",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProgressEventDisposition {
    Accepted,
    Dropped,
    Duplicate,
    OutOfOrder,
    AfterTerminal,
    Terminal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TerminalState {
    Succeeded,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProgressStatus {
    Running,
    Succeeded,
    Failed,
}

impl ProgressStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed)
    }

    fn status_kind(self) -> StatusKind {
        match self {
            Self::Running => StatusKind::Warning,
            Self::Succeeded => StatusKind::Success,
            Self::Failed => StatusKind::Error,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProgressMilestone {
    run_id: String,
    sequence: u64,
    timestamp_ms: i64,
    phase: ProgressPhase,
    status: ProgressStatus,
    message: String,
    fields: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProgressSnapshot {
    current_phase: ProgressPhase,
    terminal: Option<TerminalState>,
    milestones: Vec<ProgressMilestone>,
    dropped_count: u64,
    last_disposition: Option<ProgressEventDisposition>,
}

impl ProgressSnapshot {
    pub(crate) fn current_phase(&self) -> ProgressPhase {
        self.current_phase
    }

    pub(crate) fn milestones(&self) -> &[ProgressMilestone] {
        &self.milestones
    }

    pub(crate) fn dropped_count(&self) -> u64 {
        self.dropped_count
    }

    pub(crate) fn last_disposition(&self) -> Option<ProgressEventDisposition> {
        self.last_disposition
    }

    pub(crate) fn latest_run_id(&self) -> Option<&str> {
        self.milestones
            .last()
            .map(|milestone| milestone.run_id.as_str())
    }

    pub(crate) fn latest_run_id_for_package(&self, package_id: &str) -> Option<&str> {
        self.milestones
            .iter()
            .rev()
            .find(|milestone| {
                milestone
                    .fields
                    .iter()
                    .any(|(key, value)| key == "package" && value == package_id)
            })
            .map(|milestone| milestone.run_id.as_str())
    }

    pub(crate) fn render(&self, config: &ProgressConfig) -> String {
        match config.render.display_mode() {
            DisplayMode::Headless => self.render_headless(),
            DisplayMode::Tty => self.render_interactive(config.render_config()),
        }
    }

    pub(crate) fn render_for_config(&self, render_config: &RenderConfig) -> String {
        self.render(&ProgressConfig::new(
            render_config.clone(),
            DisplayVerbosity::Normal,
        ))
    }

    fn render_headless(&self) -> String {
        let mut output = String::new();
        for milestone in &self.milestones {
            output.push_str(&format!(
                "{} [{}] {} {}",
                milestone.timestamp_ms,
                milestone.phase.as_str(),
                milestone.status.as_str(),
                milestone.message
            ));
            for (key, value) in &milestone.fields {
                output.push(' ');
                output.push_str(key);
                output.push('=');
                output.push_str(value);
            }
            output.push('\n');
        }
        if self.dropped_count > 0 {
            output.push_str(&format!(
                "progress_events_dropped count={}\n",
                self.dropped_count
            ));
        }
        output
    }

    fn render_interactive(&self, render_config: &RenderConfig) -> String {
        let status = self
            .milestones
            .last()
            .map(|milestone| milestone.status)
            .unwrap_or(ProgressStatus::Running);
        let mut document = RenderDocument::new()
            .push(SectionRule::new())
            .push(StatusLine::new(
                status.status_kind(),
                format!("{} progress", self.current_phase.as_str()),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Run progress")
                    .row("phase", self.current_phase.as_str())
                    .row("events", self.milestones.len().to_string())
                    .row("dropped", self.dropped_count.to_string()),
            );
        if !self.milestones.is_empty() {
            let mut table = Table::new(["seq", "phase", "status", "event", "details"]);
            for milestone in &self.milestones {
                table = table.row([
                    milestone.sequence.to_string(),
                    milestone.phase.as_str().to_owned(),
                    milestone.status.as_str().to_owned(),
                    milestone.message.clone(),
                    milestone
                        .fields
                        .iter()
                        .map(|(key, value)| format!("{key}={value}"))
                        .collect::<Vec<_>>()
                        .join(" "),
                ]);
            }
            document = document.blank_line().push(table);
        }
        document.render(render_config)
    }
}

#[derive(Debug)]
struct ProgressState {
    current_phase: ProgressPhase,
    active_run_id: Option<String>,
    seen_sequences: BTreeSet<(String, u64)>,
    max_sequence_by_run: BTreeMap<String, u64>,
    terminal: Option<TerminalState>,
    milestones: VecDeque<ProgressMilestone>,
    dropped_count: u64,
    last_disposition: Option<ProgressEventDisposition>,
}

impl Default for ProgressState {
    fn default() -> Self {
        Self {
            current_phase: ProgressPhase::Plan,
            active_run_id: None,
            seen_sequences: BTreeSet::new(),
            max_sequence_by_run: BTreeMap::new(),
            terminal: None,
            milestones: VecDeque::new(),
            dropped_count: 0,
            last_disposition: None,
        }
    }
}

impl ProgressState {
    fn apply_event(
        &mut self,
        event: &RunEvent,
        config: &ProgressConfig,
    ) -> ProgressEventDisposition {
        let run_id = event.run_id.as_str().to_owned();
        let sequence_key = (run_id.clone(), event.sequence);
        if self.seen_sequences.contains(&sequence_key) {
            return self.record_disposition(ProgressEventDisposition::Duplicate);
        }
        if self
            .max_sequence_by_run
            .get(&run_id)
            .is_some_and(|max_sequence| event.sequence < *max_sequence)
        {
            self.seen_sequences.insert(sequence_key);
            return self.record_disposition(ProgressEventDisposition::OutOfOrder);
        }

        self.seen_sequences.insert(sequence_key);
        self.max_sequence_by_run
            .entry(run_id.clone())
            .and_modify(|max_sequence| *max_sequence = (*max_sequence).max(event.sequence))
            .or_insert(event.sequence);

        if self.active_run_id.as_deref() != Some(run_id.as_str()) {
            self.active_run_id = Some(run_id);
            self.terminal = None;
        }

        if let Some(terminal) = self.terminal {
            if terminal == TerminalState::Failed && can_follow_failed_terminal(event.kind) {
                self.terminal = None;
            } else {
                return self.record_disposition(ProgressEventDisposition::AfterTerminal);
            }
        }

        let phase = match event.kind {
            RunEventKind::RunFailed => self.current_phase,
            _ => phase_for_event(event.kind),
        };
        self.current_phase = phase;

        let terminal = terminal_for_event(event.kind);
        if let Some(terminal) = terminal {
            self.terminal = Some(terminal);
        }

        let status = match terminal {
            Some(TerminalState::Succeeded) => ProgressStatus::Succeeded,
            Some(TerminalState::Failed) => ProgressStatus::Failed,
            None => ProgressStatus::Running,
        };

        if config.verbosity.records_milestone(status) {
            if self.milestones.len() >= config.capacity {
                if status.is_terminal() {
                    self.milestones.pop_front();
                } else {
                    self.dropped_count += 1;
                    return self.record_disposition(ProgressEventDisposition::Dropped);
                }
            }
            self.milestones.push_back(ProgressMilestone::from_event(
                event,
                phase,
                status,
                config.verbosity,
            ));
        }

        if terminal.is_some() {
            self.record_disposition(ProgressEventDisposition::Terminal)
        } else {
            self.record_disposition(ProgressEventDisposition::Accepted)
        }
    }

    fn snapshot(&self) -> ProgressSnapshot {
        ProgressSnapshot {
            current_phase: self.current_phase,
            terminal: self.terminal,
            milestones: self.milestones.iter().cloned().collect(),
            dropped_count: self.dropped_count,
            last_disposition: self.last_disposition,
        }
    }

    fn record_disposition(
        &mut self,
        disposition: ProgressEventDisposition,
    ) -> ProgressEventDisposition {
        self.last_disposition = Some(disposition);
        disposition
    }
}

pub(crate) struct CliProgressSink {
    config: ProgressConfig,
    state: Mutex<ProgressState>,
}

impl CliProgressSink {
    pub(crate) fn new(config: ProgressConfig) -> Self {
        Self {
            config,
            state: Mutex::new(ProgressState::default()),
        }
    }

    pub(crate) fn snapshot(&self) -> ProgressSnapshot {
        self.state.lock().unwrap().snapshot()
    }
}

pub(crate) fn human_progress_sink(
    json_mode: bool,
    terminal: &TerminalPolicy,
) -> Option<CliProgressSink> {
    terminal.progress_enabled(json_mode).then(|| {
        let verbosity = match terminal.verbosity {
            Verbosity::Quiet => DisplayVerbosity::Quiet,
            Verbosity::Normal => DisplayVerbosity::Normal,
            Verbosity::Verbose(_) => DisplayVerbosity::Verbose,
        };
        CliProgressSink::new(ProgressConfig::new(
            RenderConfig::detect(terminal, OutputChannel::Stderr),
            verbosity,
        ))
    })
}

impl RunEventSink for CliProgressSink {
    fn try_emit(&self, event: &RunEvent) -> RunEventSinkResult {
        let Ok(mut state) = self.state.try_lock() else {
            return RunEventSinkResult::Dropped;
        };
        match state.apply_event(event, &self.config) {
            ProgressEventDisposition::Dropped => RunEventSinkResult::Dropped,
            ProgressEventDisposition::Accepted
            | ProgressEventDisposition::Duplicate
            | ProgressEventDisposition::OutOfOrder
            | ProgressEventDisposition::AfterTerminal
            | ProgressEventDisposition::Terminal => RunEventSinkResult::Accepted,
        }
    }
}

impl ProgressMilestone {
    fn from_event(
        event: &RunEvent,
        phase: ProgressPhase,
        status: ProgressStatus,
        verbosity: DisplayVerbosity,
    ) -> Self {
        Self {
            run_id: redact_uri_userinfo(event.run_id.as_str()),
            sequence: event.sequence,
            timestamp_ms: event.timestamp_ms,
            phase,
            status,
            message: event.kind.as_str().replace('_', " "),
            fields: milestone_fields(event, verbosity),
        }
    }
}

fn phase_for_event(kind: RunEventKind) -> ProgressPhase {
    match kind {
        RunEventKind::RunStarted | RunEventKind::PlanRecorded => ProgressPhase::Plan,
        RunEventKind::PackageStarted | RunEventKind::PackageSegmentRecorded => {
            ProgressPhase::Extract
        }
        RunEventKind::ValidationDepthTransitionRecorded => ProgressPhase::Validate,
        RunEventKind::PackageFinalized | RunEventKind::PhaseMeasured => ProgressPhase::Package,
        RunEventKind::DestinationCommitStarted
        | RunEventKind::DestinationSegmentAcknowledged
        | RunEventKind::ReplayRecorded => ProgressPhase::Commit,
        RunEventKind::DestinationReceiptRecorded => ProgressPhase::Verify,
        RunEventKind::CheckpointProposed
        | RunEventKind::CheckpointCommitted
        | RunEventKind::PackageStatusUpdated
        | RunEventKind::RunSucceeded
        | RunEventKind::RunResumed
        | RunEventKind::RunFailed => ProgressPhase::Gate,
    }
}

fn terminal_for_event(kind: RunEventKind) -> Option<TerminalState> {
    match kind {
        RunEventKind::RunSucceeded | RunEventKind::RunResumed | RunEventKind::ReplayRecorded => {
            Some(TerminalState::Succeeded)
        }
        RunEventKind::RunFailed => Some(TerminalState::Failed),
        _ => None,
    }
}

fn can_follow_failed_terminal(kind: RunEventKind) -> bool {
    matches!(
        kind,
        RunEventKind::CheckpointProposed
            | RunEventKind::DestinationCommitStarted
            | RunEventKind::DestinationSegmentAcknowledged
            | RunEventKind::DestinationReceiptRecorded
            | RunEventKind::CheckpointCommitted
            | RunEventKind::PackageStatusUpdated
            | RunEventKind::RunResumed
            | RunEventKind::ReplayRecorded
    )
}

fn milestone_fields(event: &RunEvent, verbosity: DisplayVerbosity) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    fields.push(("run".to_owned(), redact_uri_userinfo(event.run_id.as_str())));
    push_optional(&mut fields, "resource", event.resource_id.as_ref());
    if let Some(scope) = &event.scope {
        fields.push(("scope".to_owned(), display_scope(scope)));
    }
    push_optional_str(&mut fields, "package", event.package_id.as_deref());
    push_optional(&mut fields, "checkpoint", event.checkpoint_id.as_ref());
    push_optional(&mut fields, "receipt", event.receipt_id.as_ref());
    push_metric_fields(&mut fields, event);

    if verbosity == DisplayVerbosity::Verbose {
        fields.push(("event".to_owned(), event.kind.as_str().to_owned()));
        fields.push(("sequence".to_owned(), event.sequence.to_string()));
        push_optional(&mut fields, "package_hash", event.package_hash.as_ref());
        push_optional_str(&mut fields, "package_path", event.package_path.as_deref());
        push_optional(&mut fields, "destination", event.destination_id.as_ref());
        push_optional(&mut fields, "plan", event.plan_id.as_ref());
        for (key, value) in &event.details.attributes {
            if !fields.iter().any(|(existing, _)| existing == key) {
                fields.push((key.clone(), display_event_value(key, value)));
            }
        }
    }

    fields
}

fn push_optional<T: AsRef<str>>(fields: &mut Vec<(String, String)>, key: &str, value: Option<&T>) {
    if let Some(value) = value {
        push_optional_str(fields, key, Some(value.as_ref()));
    }
}

fn display_scope(scope: &ScopeKey) -> String {
    match scope {
        ScopeKey::Resource => "resource".to_owned(),
        ScopeKey::Partition { partition_id } => {
            format!("partition:{}", redact_uri_userinfo(partition_id.as_str()))
        }
        ScopeKey::Window { start, end } => {
            format!(
                "window:{}..{}",
                redact_uri_userinfo(start),
                redact_uri_userinfo(end)
            )
        }
        ScopeKey::File { path } => format!("file:{}", redact_uri_userinfo(path)),
        ScopeKey::Stream { name } => format!("stream:{}", redact_uri_userinfo(name)),
        ScopeKey::SchemaContract { contract } => {
            format!("schema_contract:{}", redact_uri_userinfo(contract.as_str()))
        }
        ScopeKey::DestinationLoad {
            destination,
            target,
        } => format!(
            "destination_load:{}:{}",
            redact_uri_userinfo(destination.as_str()),
            redact_uri_userinfo(target.as_str())
        ),
        ScopeKey::Composite { parts } => parts
            .iter()
            .map(display_scope)
            .collect::<Vec<_>>()
            .join("+"),
    }
}

fn push_optional_str(fields: &mut Vec<(String, String)>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        fields.push((key.to_owned(), redact_uri_userinfo(value)));
    }
}

fn push_metric_fields(fields: &mut Vec<(String, String)>, event: &RunEvent) {
    for key in [
        "row_count",
        "rows_written",
        "rows_inserted",
        "rows_updated",
        "rows_deleted",
        "byte_count",
        "batch_count",
        "segment_index",
        "segment_count",
        "segment_ack_count",
        "quarantine_record_count",
        "migration_count",
        "retry_after_ms",
        "backoff_notice",
        "status",
        "package_status",
        "receipt_source",
        "duplicate",
        "no_op",
        "package_receipt_recorded",
        "source_contact",
        "mutation_required",
        "mutated",
        "state",
        "action",
        "result",
        "guidance",
        "from_depth",
        "to_depth",
        "trigger",
    ] {
        if let Some(value) = event.details.attributes.get(key) {
            fields.push((key.to_owned(), display_event_value(key, value)));
        }
    }
}

fn display_event_value(key: &str, value: &RunEventValue) -> String {
    if is_sensitive_key(key) && !value_contains_only_secret_refs(value) {
        return redacted();
    }
    match value {
        RunEventValue::Bool(value) => value.to_string(),
        RunEventValue::I64(value) => value.to_string(),
        RunEventValue::U64(value) => display_u64_value(key, *value),
        RunEventValue::String(value) => redact_uri_userinfo(value),
        RunEventValue::SecretRef(_) => redacted(),
        RunEventValue::List(values) => values
            .iter()
            .map(|value| display_event_value(key, value))
            .collect::<Vec<_>>()
            .join(","),
        RunEventValue::Object(values) => values
            .iter()
            .map(|(key, value)| format!("{key}:{}", display_event_value(key, value)))
            .collect::<Vec<_>>()
            .join(","),
        RunEventValue::PhaseMetric(metric) if metric.phase == cdf_kernel::RunPhase::SourceRead => {
            format!(
                "source_read {:?} {} physical / {} useful / {} waste across {} requests in {}",
                metric.status,
                humanize_bytes(metric.input_bytes),
                humanize_bytes(metric.output_bytes),
                humanize_bytes(metric.input_bytes.saturating_sub(metric.output_bytes)),
                metric.operations,
                humanize_duration(Duration::from_nanos(metric.duration_ns))
            )
        }
        RunEventValue::PhaseMetric(metric) => format!(
            "{} {:?} {} ns {}/{} bytes",
            metric.phase.as_str(),
            metric.status,
            metric.duration_ns,
            metric.input_bytes,
            metric.output_bytes
        ),
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

fn display_u64_value(key: &str, value: u64) -> String {
    match key {
        "row_count"
        | "rows_written"
        | "rows_inserted"
        | "rows_updated"
        | "rows_deleted"
        | "quarantine_record_count" => humanize_rows(value),
        "byte_count" => humanize_bytes(value),
        "elapsed_ms" | "retry_after_ms" => humanize_duration(Duration::from_millis(value)),
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use cdf_kernel::{
        CheckpointId, DestinationId, PackageHash, PlanId, ReceiptId, ResourceId, RunId, RunPhase,
        RunPhaseMetric, RunPhaseStatus, SecretReference,
    };

    use super::*;
    use crate::render::config::{DisplayMode as RenderDisplayMode, RenderEnv};

    fn sink(verbosity: DisplayVerbosity) -> CliProgressSink {
        CliProgressSink::new(ProgressConfig::new(headless_config(), verbosity))
    }

    #[test]
    fn cx1_quiet_policy_does_not_create_a_human_progress_sink() {
        let policy = TerminalPolicy {
            verbosity: Verbosity::Quiet,
            ..TerminalPolicy::default()
        };

        assert!(human_progress_sink(false, &policy).is_none());
    }

    fn bounded_sink(capacity: usize) -> CliProgressSink {
        CliProgressSink::new(
            ProgressConfig::new(headless_config(), DisplayVerbosity::Normal)
                .with_capacity(capacity),
        )
    }

    fn headless_config() -> RenderConfig {
        RenderConfig::headless_for_width(96)
    }

    fn tty_config() -> RenderConfig {
        RenderConfig::new(
            RenderDisplayMode::Tty,
            96,
            RenderEnv {
                no_color: false,
                clicolor_force: false,
                unicode_supported: true,
            },
            TerminalPolicy::default(),
        )
    }

    fn event(sequence: u64, kind: RunEventKind) -> RunEvent {
        event_for_run("run-progress-test", sequence, kind)
    }

    fn event_for_run(run_id: &str, sequence: u64, kind: RunEventKind) -> RunEvent {
        let mut attributes = BTreeMap::new();
        attributes.insert("row_count".to_owned(), RunEventValue::U64(12_345));
        RunEvent {
            run_id: RunId::new(run_id).unwrap(),
            sequence,
            timestamp_ms: 1_725_000_000_000 + i64::try_from(sequence).unwrap(),
            kind,
            resource_id: Some(ResourceId::new("local.events").unwrap()),
            scope: None,
            partition_id: None,
            package_id: Some("pkg-progress-test".to_owned()),
            package_hash: Some(PackageHash::new("pkg-hash-progress-test").unwrap()),
            package_path: Some("packages/pkg-progress-test".to_owned()),
            checkpoint_id: Some(CheckpointId::new("chk-progress-test").unwrap()),
            receipt_id: Some(ReceiptId::new("receipt-progress-test").unwrap()),
            destination_id: Some(DestinationId::new("duckdb").unwrap()),
            plan_id: Some(PlanId::new("plan-progress-test").unwrap()),
            details: cdf_kernel::RunEventDetails { attributes },
        }
    }

    #[test]
    fn source_read_metric_names_physical_useful_waste_and_requests() {
        let rendered = display_event_value(
            "metric",
            &RunEventValue::PhaseMetric(RunPhaseMetric {
                phase: RunPhase::SourceRead,
                status: RunPhaseStatus::Completed,
                duration_ns: 2_000_000,
                input_bytes: 10 * 1024 * 1024,
                output_bytes: 8 * 1024 * 1024,
                operations: 3,
            }),
        );
        assert_eq!(
            rendered,
            "source_read Completed 10 MiB physical / 8 MiB useful / 2 MiB waste across 3 requests in 2ms"
        );
    }

    #[test]
    fn phase_mapping_follows_live_progress_spec() {
        let sink = sink(DisplayVerbosity::Normal);

        for (kind, phase) in [
            (RunEventKind::RunStarted, ProgressPhase::Plan),
            (RunEventKind::PlanRecorded, ProgressPhase::Plan),
            (RunEventKind::PackageStarted, ProgressPhase::Extract),
            (RunEventKind::PackageSegmentRecorded, ProgressPhase::Extract),
            (
                RunEventKind::ValidationDepthTransitionRecorded,
                ProgressPhase::Validate,
            ),
            (RunEventKind::PackageFinalized, ProgressPhase::Package),
            (
                RunEventKind::DestinationCommitStarted,
                ProgressPhase::Commit,
            ),
            (
                RunEventKind::DestinationSegmentAcknowledged,
                ProgressPhase::Commit,
            ),
            (RunEventKind::ReplayRecorded, ProgressPhase::Commit),
            (
                RunEventKind::DestinationReceiptRecorded,
                ProgressPhase::Verify,
            ),
            (RunEventKind::CheckpointProposed, ProgressPhase::Gate),
            (RunEventKind::CheckpointCommitted, ProgressPhase::Gate),
            (RunEventKind::PackageStatusUpdated, ProgressPhase::Gate),
            (RunEventKind::RunSucceeded, ProgressPhase::Gate),
            (RunEventKind::RunResumed, ProgressPhase::Gate),
            (RunEventKind::RunFailed, ProgressPhase::Gate),
        ] {
            assert_eq!(phase_for_event(kind), phase);
        }

        for (sequence, kind, phase) in [
            (1, RunEventKind::RunStarted, ProgressPhase::Plan),
            (2, RunEventKind::PackageStarted, ProgressPhase::Extract),
            (
                3,
                RunEventKind::ValidationDepthTransitionRecorded,
                ProgressPhase::Validate,
            ),
            (4, RunEventKind::PackageFinalized, ProgressPhase::Package),
            (
                5,
                RunEventKind::DestinationCommitStarted,
                ProgressPhase::Commit,
            ),
            (
                6,
                RunEventKind::DestinationReceiptRecorded,
                ProgressPhase::Verify,
            ),
            (7, RunEventKind::CheckpointCommitted, ProgressPhase::Gate),
        ] {
            assert_eq!(
                sink.try_emit(&event(sequence, kind)),
                RunEventSinkResult::Accepted
            );
            let snapshot = sink.snapshot();
            assert_eq!(snapshot.current_phase(), phase);
            assert_eq!(
                snapshot.last_disposition(),
                Some(ProgressEventDisposition::Accepted)
            );
        }
    }

    #[test]
    fn run_failed_stays_on_current_failed_phase_and_closes_terminal_state() {
        let sink = sink(DisplayVerbosity::Normal);

        assert_eq!(
            sink.try_emit(&event(1, RunEventKind::DestinationCommitStarted)),
            RunEventSinkResult::Accepted
        );
        assert_eq!(
            sink.try_emit(&event(2, RunEventKind::RunFailed)),
            RunEventSinkResult::Accepted
        );
        assert_eq!(
            sink.try_emit(&event(3, RunEventKind::RunSucceeded)),
            RunEventSinkResult::Accepted
        );

        let snapshot = sink.snapshot();
        assert_eq!(snapshot.current_phase(), ProgressPhase::Commit);
        assert_eq!(
            snapshot.last_disposition(),
            Some(ProgressEventDisposition::AfterTerminal)
        );
        assert_eq!(
            snapshot.milestones().last().unwrap().status,
            ProgressStatus::Failed
        );
    }

    #[test]
    fn recovery_events_after_run_failed_reopen_failed_terminal_until_run_resumed() {
        let sink = sink(DisplayVerbosity::Normal);

        assert_eq!(
            sink.try_emit(&event(1, RunEventKind::PackageFinalized)),
            RunEventSinkResult::Accepted
        );
        assert_eq!(
            sink.try_emit(&event(2, RunEventKind::RunFailed)),
            RunEventSinkResult::Accepted
        );
        assert_eq!(
            sink.try_emit(&event(3, RunEventKind::DestinationReceiptRecorded)),
            RunEventSinkResult::Accepted
        );
        assert_eq!(
            sink.try_emit(&event(4, RunEventKind::RunResumed)),
            RunEventSinkResult::Accepted
        );
        assert_eq!(
            sink.try_emit(&event(5, RunEventKind::RunSucceeded)),
            RunEventSinkResult::Accepted
        );

        let snapshot = sink.snapshot();
        assert_eq!(snapshot.current_phase(), ProgressPhase::Gate);
        assert_eq!(
            snapshot.last_disposition(),
            Some(ProgressEventDisposition::AfterTerminal)
        );
        assert_eq!(snapshot.milestones().len(), 4);
        assert_eq!(snapshot.milestones().last().unwrap().message, "run resumed");
        assert_eq!(
            snapshot.milestones().last().unwrap().status,
            ProgressStatus::Succeeded
        );
    }

    #[test]
    fn duplicate_and_out_of_order_events_are_deterministic_noops() {
        let sink = sink(DisplayVerbosity::Normal);

        assert_eq!(
            sink.try_emit(&event(1, RunEventKind::RunStarted)),
            RunEventSinkResult::Accepted
        );
        assert_eq!(
            sink.try_emit(&event(3, RunEventKind::PackageStarted)),
            RunEventSinkResult::Accepted
        );
        assert_eq!(
            sink.try_emit(&event(3, RunEventKind::PackageFinalized)),
            RunEventSinkResult::Accepted
        );
        assert_eq!(
            sink.snapshot().last_disposition(),
            Some(ProgressEventDisposition::Duplicate)
        );
        assert_eq!(
            sink.try_emit(&event(2, RunEventKind::PlanRecorded)),
            RunEventSinkResult::Accepted
        );

        let snapshot = sink.snapshot();
        assert_eq!(
            snapshot.last_disposition(),
            Some(ProgressEventDisposition::OutOfOrder)
        );
        assert_eq!(snapshot.current_phase(), ProgressPhase::Extract);
        assert_eq!(snapshot.milestones().len(), 2);
    }

    #[test]
    fn restarted_sequences_from_distinct_runs_remain_visible_for_multi_slice_progress() {
        let sink = sink(DisplayVerbosity::Normal);

        for event in [
            event_for_run("run-progress-slice-1", 1, RunEventKind::RunStarted),
            event_for_run("run-progress-slice-1", 2, RunEventKind::RunSucceeded),
            event_for_run("run-progress-slice-2", 1, RunEventKind::RunStarted),
            event_for_run("run-progress-slice-2", 2, RunEventKind::RunSucceeded),
        ] {
            assert_eq!(sink.try_emit(&event), RunEventSinkResult::Accepted);
        }

        let snapshot = sink.snapshot();
        assert_eq!(snapshot.milestones().len(), 4);
        assert_eq!(snapshot.latest_run_id(), Some("run-progress-slice-2"));
        assert_eq!(
            snapshot.last_disposition(),
            Some(ProgressEventDisposition::Terminal)
        );
        let rendered = snapshot.render(&ProgressConfig::new(
            headless_config(),
            DisplayVerbosity::Normal,
        ));
        assert!(rendered.contains("run=run-progress-slice-1"));
        assert!(rendered.contains("run=run-progress-slice-2"));
    }

    #[test]
    fn latest_run_id_for_package_uses_matching_slice_package_only() {
        let sink = sink(DisplayVerbosity::Normal);
        let mut first = event_for_run("run-progress-slice-1", 1, RunEventKind::RunStarted);
        first.package_id = Some("pkg-progress-slice-1".to_owned());
        let mut second = event_for_run("run-progress-slice-2", 1, RunEventKind::RunStarted);
        second.package_id = Some("pkg-progress-slice-2".to_owned());

        assert_eq!(sink.try_emit(&first), RunEventSinkResult::Accepted);
        assert_eq!(sink.try_emit(&second), RunEventSinkResult::Accepted);

        let snapshot = sink.snapshot();
        assert_eq!(
            snapshot.latest_run_id_for_package("pkg-progress-slice-1"),
            Some("run-progress-slice-1")
        );
        assert_eq!(
            snapshot.latest_run_id_for_package("pkg-progress-slice-2"),
            Some("run-progress-slice-2")
        );
        assert_eq!(
            snapshot.latest_run_id_for_package("pkg-progress-slice-3"),
            None
        );
    }

    #[test]
    fn backpressure_drops_nonterminal_events_without_blocking() {
        let sink = bounded_sink(1);

        assert_eq!(
            sink.try_emit(&event(1, RunEventKind::RunStarted)),
            RunEventSinkResult::Accepted
        );
        assert_eq!(
            sink.try_emit(&event(2, RunEventKind::PackageStarted)),
            RunEventSinkResult::Dropped
        );
        let guard = sink.state.try_lock().unwrap();
        assert_eq!(
            sink.try_emit(&event(3, RunEventKind::PackageFinalized)),
            RunEventSinkResult::Dropped
        );
        drop(guard);

        let snapshot = sink.snapshot();
        assert_eq!(snapshot.dropped_count(), 1);
        assert_eq!(snapshot.milestones().len(), 1);
    }

    #[test]
    fn terminal_event_evicts_oldest_milestone_when_buffer_is_full() {
        let sink = bounded_sink(1);

        assert_eq!(
            sink.try_emit(&event(1, RunEventKind::RunStarted)),
            RunEventSinkResult::Accepted
        );
        assert_eq!(
            sink.try_emit(&event(2, RunEventKind::RunSucceeded)),
            RunEventSinkResult::Accepted
        );

        let snapshot = sink.snapshot();
        assert_eq!(
            snapshot.last_disposition(),
            Some(ProgressEventDisposition::Terminal)
        );
        assert_eq!(snapshot.milestones().len(), 1);
        assert_eq!(snapshot.milestones()[0].message, "run succeeded".to_owned());
    }

    #[test]
    fn redaction_applies_before_headless_and_interactive_rendering() {
        let sink = sink(DisplayVerbosity::Verbose);
        let mut event = event(1, RunEventKind::PackageFinalized);
        event.package_path = Some("postgres://user:secret-value@localhost/db".to_owned());
        event.details.attributes.insert(
            "api_token_raw".to_owned(),
            RunEventValue::String("raw-token-value".to_owned()),
        );
        event.details.attributes.insert(
            "api_token".to_owned(),
            RunEventValue::SecretRef(SecretReference::new("secret://env/API_TOKEN").unwrap()),
        );

        assert_eq!(sink.try_emit(&event), RunEventSinkResult::Accepted);
        let snapshot = sink.snapshot();
        let headless = snapshot.render(&ProgressConfig::new(
            headless_config(),
            DisplayVerbosity::Verbose,
        ));
        let interactive = snapshot.render(&ProgressConfig::new(
            tty_config(),
            DisplayVerbosity::Verbose,
        ));

        assert!(!headless.contains("secret-value"));
        assert!(!headless.contains("raw-token-value"));
        assert!(!headless.contains("secret://env/API_TOKEN"));
        assert!(headless.contains("package_path=postgres://[redacted]@localhost/db"));
        assert!(headless.contains("api_token=[redacted]"));
        assert!(headless.contains("api_token_raw=[redacted]"));
        assert!(!interactive.contains("secret-value"));
        assert!(!interactive.contains("raw-token-value"));
        assert!(!interactive.contains("secret://env/API_TOKEN"));
    }

    #[test]
    fn headless_formatting_is_line_oriented_and_ansi_free() {
        let sink = sink(DisplayVerbosity::Normal);

        assert_eq!(
            sink.try_emit(&event(1, RunEventKind::PackageFinalized)),
            RunEventSinkResult::Accepted
        );
        let rendered = sink.snapshot().render(&ProgressConfig::new(
            headless_config(),
            DisplayVerbosity::Normal,
        ));

        assert_eq!(
            rendered,
            "1725000000001 [package] running package finalized run=run-progress-test resource=local.events package=pkg-progress-test checkpoint=chk-progress-test receipt=receipt-progress-test row_count=12.3k\n"
        );
        assert!(!rendered.contains("\u{1b}["));
        assert!(!rendered.contains('\r'));
    }

    #[test]
    fn quiet_suppresses_live_progress_while_verbose_includes_event_details() {
        let quiet = sink(DisplayVerbosity::Quiet);
        assert_eq!(
            quiet.try_emit(&event(1, RunEventKind::PackageStarted)),
            RunEventSinkResult::Accepted
        );
        assert!(quiet.snapshot().milestones().is_empty());
        assert_eq!(
            quiet.try_emit(&event(2, RunEventKind::RunSucceeded)),
            RunEventSinkResult::Accepted
        );
        assert_eq!(quiet.snapshot().milestones().len(), 1);

        let verbose = sink(DisplayVerbosity::Verbose);
        assert_eq!(
            verbose.try_emit(&event(1, RunEventKind::PackageStarted)),
            RunEventSinkResult::Accepted
        );
        let rendered = verbose.snapshot().render(&ProgressConfig::new(
            headless_config(),
            DisplayVerbosity::Verbose,
        ));

        assert!(rendered.contains("run=run-progress-test"));
        assert!(rendered.contains("event=package_started"));
        assert!(rendered.contains("sequence=1"));
    }
}
