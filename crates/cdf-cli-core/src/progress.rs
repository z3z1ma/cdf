use std::{
    collections::{BTreeMap, VecDeque},
    io::Write,
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError},
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use cdf_kernel::{
    RunEvent, RunEventKind, RunEventSink, RunEventSinkResult, RunEventValue, RunPhase,
    RunProgressObservation, RunProgressObservationKind, RunProgressSink, ScopeKey,
};

use crate::render::{
    RenderConfig, RenderDocument,
    config::DisplayMode,
    humanize::{humanize_bytes, humanize_duration, humanize_rows},
    primitives::{ActivityLine, ActivityState, KeyValuePanel, StatusKind, StatusLine, Table},
    redaction::{is_sensitive_key, redact_uri_userinfo, redacted},
};
use crate::terminal::{OutputChannel, TerminalPolicy, Verbosity};

const DEFAULT_PROGRESS_CAPACITY: usize = 128;
const INTERACTIVE_REFRESH_INTERVAL: Duration = Duration::from_millis(100);
const HEADLESS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProgressDelivery {
    #[default]
    Buffered,
    LiveStderr,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DisplayVerbosity {
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
pub struct ProgressConfig {
    render: RenderConfig,
    verbosity: DisplayVerbosity,
    capacity: usize,
}

impl ProgressConfig {
    pub fn new(render: RenderConfig, verbosity: DisplayVerbosity) -> Self {
        Self {
            render,
            verbosity,
            capacity: DEFAULT_PROGRESS_CAPACITY,
        }
    }

    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity.max(1);
        self
    }

    pub fn render_config(&self) -> &RenderConfig {
        &self.render
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProgressPhase {
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
pub enum ProgressEventDisposition {
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
pub struct ProgressMilestone {
    run_id: String,
    sequence: u64,
    timestamp_ms: i64,
    phase: ProgressPhase,
    status: ProgressStatus,
    message: String,
    fields: Vec<(String, String)>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProgressMetrics {
    rows: Option<u64>,
    bytes: Option<u64>,
    segments: Option<u64>,
    batches: Option<u64>,
    quarantine_rows: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProgressPhaseSnapshot {
    run_id: String,
    phase: ProgressPhase,
    status: ProgressStatus,
    detail: String,
    timestamp_ms: i64,
    elapsed: Duration,
    metrics: ProgressMetrics,
    notice: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgressSnapshot {
    current_phase: ProgressPhase,
    terminal: Option<TerminalState>,
    milestones: Vec<ProgressMilestone>,
    phases: Vec<ProgressPhaseSnapshot>,
    dropped_count: u64,
    last_disposition: Option<ProgressEventDisposition>,
    verbosity: DisplayVerbosity,
    streamed_live: bool,
}

impl ProgressSnapshot {
    pub fn current_phase(&self) -> ProgressPhase {
        self.current_phase
    }

    pub fn milestones(&self) -> &[ProgressMilestone] {
        &self.milestones
    }

    pub fn dropped_count(&self) -> u64 {
        self.dropped_count
    }

    pub fn last_disposition(&self) -> Option<ProgressEventDisposition> {
        self.last_disposition
    }

    pub fn latest_run_id(&self) -> Option<&str> {
        self.milestones
            .last()
            .map(|milestone| milestone.run_id.as_str())
    }

    pub fn latest_run_id_for_package(&self, package_id: &str) -> Option<&str> {
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

    pub fn render(&self, config: &ProgressConfig) -> String {
        match config.render.display_mode() {
            DisplayMode::Headless => self.render_headless(config.verbosity),
            DisplayMode::Tty => self.render_interactive(config.render_config(), config.verbosity),
        }
    }

    pub fn render_for_config(&self, render_config: &RenderConfig) -> String {
        if self.streamed_live {
            return String::new();
        }
        self.render(&ProgressConfig::new(render_config.clone(), self.verbosity))
    }

    fn render_headless(&self, verbosity: DisplayVerbosity) -> String {
        let mut output = String::new();
        let milestones = self.display_milestones(verbosity);
        for milestone in milestones {
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

    fn render_interactive(
        &self,
        render_config: &RenderConfig,
        verbosity: DisplayVerbosity,
    ) -> String {
        let status = self
            .milestones
            .last()
            .map(|milestone| milestone.status)
            .unwrap_or(ProgressStatus::Running);
        let mut document = RenderDocument::new();
        if verbosity == DisplayVerbosity::Verbose {
            document = document
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
        } else {
            for phase in &self.phases {
                let state = match phase.status {
                    ProgressStatus::Running => ActivityState::Active,
                    ProgressStatus::Succeeded => ActivityState::Complete,
                    ProgressStatus::Failed => ActivityState::Failed,
                };
                let mut activity =
                    ActivityLine::new(state, activity_verb(phase.phase), phase.detail.clone());
                for metric in activity_metrics(phase) {
                    activity = activity.metric(metric);
                }
                if let Some(notice) = &phase.notice {
                    activity = activity.metric(notice.clone());
                }
                document = document.push(activity);
            }
            if self.dropped_count > 0 {
                document = document.push(ActivityLine::new(
                    ActivityState::Warning,
                    "Progress",
                    format!("{} events coalesced or dropped", self.dropped_count),
                ));
            }
        }
        document.render(render_config)
    }

    fn display_milestones(&self, verbosity: DisplayVerbosity) -> Vec<&ProgressMilestone> {
        if verbosity == DisplayVerbosity::Verbose {
            return self.milestones.iter().collect();
        }
        let mut by_phase = BTreeMap::new();
        for milestone in &self.milestones {
            by_phase.insert((milestone.run_id.as_str(), milestone.phase), milestone);
        }
        by_phase.into_values().collect()
    }
}

#[derive(Debug)]
struct PhaseProgress {
    run_id: String,
    phase: ProgressPhase,
    status: ProgressStatus,
    detail: String,
    timestamp_ms: i64,
    started_at: Instant,
    finished_at: Option<Instant>,
    metrics: ProgressMetrics,
    notice: Option<String>,
}

impl PhaseProgress {
    fn snapshot(&self, now: Instant) -> ProgressPhaseSnapshot {
        ProgressPhaseSnapshot {
            run_id: self.run_id.clone(),
            phase: self.phase,
            status: self.status,
            detail: self.detail.clone(),
            timestamp_ms: self.timestamp_ms,
            elapsed: self
                .finished_at
                .unwrap_or(now)
                .saturating_duration_since(self.started_at),
            metrics: self.metrics.clone(),
            notice: self.notice.clone(),
        }
    }

    fn complete(&mut self, status: ProgressStatus, now: Instant) {
        if self.finished_at.is_none() {
            self.finished_at = Some(now);
        }
        self.status = status;
    }
}

#[derive(Debug)]
struct ProgressState {
    current_phase: ProgressPhase,
    active_run_id: Option<String>,
    max_sequence_by_run: BTreeMap<String, u64>,
    terminal: Option<TerminalState>,
    milestones: VecDeque<ProgressMilestone>,
    phases: BTreeMap<(String, ProgressPhase), PhaseProgress>,
    dropped_count: u64,
    last_disposition: Option<ProgressEventDisposition>,
}

impl Default for ProgressState {
    fn default() -> Self {
        Self {
            current_phase: ProgressPhase::Plan,
            active_run_id: None,
            max_sequence_by_run: BTreeMap::new(),
            terminal: None,
            milestones: VecDeque::new(),
            phases: BTreeMap::new(),
            dropped_count: 0,
            last_disposition: None,
        }
    }
}

impl ProgressState {
    fn apply_observation(&mut self, observation: &RunProgressObservation, now: Instant) {
        let run_id = observation.run_id.as_str().to_owned();
        self.active_run_id = Some(run_id.clone());
        let phase = progress_phase_for_runtime_phase(observation.phase);
        if phase != self.current_phase
            && let Some(current) = self.phases.get_mut(&(run_id.clone(), self.current_phase))
            && current.status == ProgressStatus::Running
        {
            current.complete(ProgressStatus::Succeeded, now);
        }
        self.current_phase = phase;
        let progress = self
            .phases
            .entry((run_id, phase))
            .or_insert_with(|| PhaseProgress {
                run_id: redact_uri_userinfo(observation.run_id.as_str()),
                phase,
                status: ProgressStatus::Running,
                detail: redact_uri_userinfo(observation.resource_id.as_str()),
                timestamp_ms: 0,
                started_at: now,
                finished_at: None,
                metrics: ProgressMetrics::default(),
                notice: None,
            });
        progress.notice = Some(match &observation.kind {
            RunProgressObservationKind::SourceRetry {
                failed_attempt,
                cause,
                delay_ms,
            } => format!(
                "retry {failed_attempt} after {}; waiting {}",
                error_kind_label(cause),
                humanize_duration(Duration::from_millis(*delay_ms))
            ),
        });
    }

    fn apply_event(
        &mut self,
        event: &RunEvent,
        config: &ProgressConfig,
        overflow: MilestoneOverflow,
    ) -> ProgressEventDisposition {
        self.apply_event_at(event, config, overflow, Instant::now())
    }

    fn apply_event_at(
        &mut self,
        event: &RunEvent,
        config: &ProgressConfig,
        overflow: MilestoneOverflow,
        now: Instant,
    ) -> ProgressEventDisposition {
        let run_id = event.run_id.as_str().to_owned();
        if let Some(max_sequence) = self.max_sequence_by_run.get(&run_id) {
            if event.sequence == *max_sequence {
                return self.record_disposition(ProgressEventDisposition::Duplicate);
            }
            if event.sequence < *max_sequence {
                return self.record_disposition(ProgressEventDisposition::OutOfOrder);
            }
        }

        if !self.max_sequence_by_run.contains_key(&run_id)
            && self.max_sequence_by_run.len() >= config.capacity
            && let Some(evicted_run) = self.max_sequence_by_run.keys().next().cloned()
        {
            self.max_sequence_by_run.remove(&evicted_run);
            self.phases.retain(|(run_id, _), _| run_id != &evicted_run);
        }
        self.max_sequence_by_run
            .insert(run_id.clone(), event.sequence);

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
            _ => phase_for_event(event),
        };
        let physical_measurement = event.kind == RunEventKind::PhaseMeasured;
        if !physical_measurement {
            if phase != self.current_phase
                && let Some(active_run_id) = &self.active_run_id
                && let Some(current) = self
                    .phases
                    .get_mut(&(active_run_id.clone(), self.current_phase))
                && current.status == ProgressStatus::Running
            {
                current.complete(ProgressStatus::Succeeded, now);
            }
            self.current_phase = phase;
        }

        let terminal = terminal_for_event(event.kind);
        if let Some(terminal) = terminal {
            self.terminal = Some(terminal);
        }

        let status = match terminal {
            Some(TerminalState::Succeeded) => ProgressStatus::Succeeded,
            Some(TerminalState::Failed) => ProgressStatus::Failed,
            None => ProgressStatus::Running,
        };

        if !physical_measurement {
            let key = (event.run_id.as_str().to_owned(), phase);
            let detail = event_detail(event);
            let phase_state = self.phases.entry(key).or_insert_with(|| PhaseProgress {
                run_id: redact_uri_userinfo(event.run_id.as_str()),
                phase,
                status: ProgressStatus::Running,
                detail: detail.clone(),
                timestamp_ms: event.timestamp_ms,
                started_at: now,
                finished_at: None,
                metrics: ProgressMetrics::default(),
                notice: None,
            });
            phase_state.detail = detail;
            phase_state.metrics.apply_event(event);
            phase_state.notice = progress_notice(event);
            match status {
                ProgressStatus::Running => {
                    if phase_state.status.is_terminal() {
                        phase_state.status = ProgressStatus::Running;
                        phase_state.started_at = now;
                        phase_state.finished_at = None;
                    }
                }
                ProgressStatus::Succeeded | ProgressStatus::Failed => {
                    phase_state.complete(status, now);
                }
            }
        }

        if config.verbosity.records_milestone(status) {
            if self.milestones.len() >= config.capacity {
                if status.is_terminal() {
                    self.milestones.pop_front();
                } else if overflow == MilestoneOverflow::Coalesce {
                    self.milestones.pop_front();
                    self.dropped_count += 1;
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

    fn snapshot(&self, verbosity: DisplayVerbosity, streamed_live: bool) -> ProgressSnapshot {
        self.snapshot_at(verbosity, streamed_live, Instant::now())
    }

    fn snapshot_at(
        &self,
        verbosity: DisplayVerbosity,
        streamed_live: bool,
        now: Instant,
    ) -> ProgressSnapshot {
        ProgressSnapshot {
            current_phase: self.current_phase,
            terminal: self.terminal,
            milestones: self.milestones.iter().cloned().collect(),
            phases: self
                .phases
                .values()
                .map(|phase| phase.snapshot(now))
                .collect(),
            dropped_count: self.dropped_count,
            last_disposition: self.last_disposition,
            verbosity,
            streamed_live,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MilestoneOverflow {
    Drop,
    Coalesce,
}

enum ProgressSinkMode {
    Buffered,
    Live {
        sender: SyncSender<LiveProgressInput>,
        terminal_sender: SyncSender<RunEvent>,
        worker: JoinHandle<()>,
    },
}

enum LiveProgressInput {
    Event(RunEvent),
    Observation(RunProgressObservation),
}

struct CliProgressObservationSink {
    sender: SyncSender<LiveProgressInput>,
}

impl RunProgressSink for CliProgressObservationSink {
    fn try_emit_progress(&self, observation: &RunProgressObservation) -> RunEventSinkResult {
        match self
            .sender
            .try_send(LiveProgressInput::Observation(observation.clone()))
        {
            Ok(()) => RunEventSinkResult::Accepted,
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                RunEventSinkResult::Dropped
            }
        }
    }
}

pub struct CliProgressSink {
    config: ProgressConfig,
    state: Arc<Mutex<ProgressState>>,
    mode: ProgressSinkMode,
}

impl CliProgressSink {
    pub fn new(config: ProgressConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(ProgressState::default())),
            mode: ProgressSinkMode::Buffered,
        }
    }

    pub fn snapshot(&self) -> ProgressSnapshot {
        self.state
            .lock()
            .unwrap()
            .snapshot(self.config.verbosity, false)
    }

    fn live_stderr(config: ProgressConfig) -> Self {
        Self::live_with_writer(config, Box::new(std::io::stderr()))
    }

    fn live_with_writer(config: ProgressConfig, writer: Box<dyn Write + Send>) -> Self {
        let state = Arc::new(Mutex::new(ProgressState::default()));
        let (sender, receiver) = mpsc::sync_channel(config.capacity);
        let (terminal_sender, terminal_receiver) = mpsc::sync_channel(1);
        let worker = spawn_live_progress_worker(
            config.clone(),
            Arc::clone(&state),
            receiver,
            terminal_receiver,
            writer,
        );
        match worker {
            Ok(worker) => Self {
                config,
                state,
                mode: ProgressSinkMode::Live {
                    sender,
                    terminal_sender,
                    worker,
                },
            },
            Err(_) => Self {
                config,
                state,
                mode: ProgressSinkMode::Buffered,
            },
        }
    }

    pub fn finish(self) -> ProgressSnapshot {
        let streamed_live = match self.mode {
            ProgressSinkMode::Buffered => false,
            ProgressSinkMode::Live {
                sender,
                terminal_sender,
                worker,
            } => {
                drop(sender);
                drop(terminal_sender);
                worker.join().is_ok()
            }
        };
        self.state
            .lock()
            .unwrap()
            .snapshot(self.config.verbosity, streamed_live)
    }
}

pub fn human_progress_sink(
    json_mode: bool,
    terminal: &TerminalPolicy,
    delivery: ProgressDelivery,
) -> Option<CliProgressSink> {
    terminal.progress_enabled(json_mode).then(|| {
        let verbosity = match terminal.verbosity {
            Verbosity::Quiet => DisplayVerbosity::Quiet,
            Verbosity::Normal => DisplayVerbosity::Normal,
            Verbosity::Verbose(_) => DisplayVerbosity::Verbose,
        };
        let config = ProgressConfig::new(
            RenderConfig::detect(terminal, OutputChannel::Stderr),
            verbosity,
        );
        match delivery {
            ProgressDelivery::Buffered => CliProgressSink::new(config),
            ProgressDelivery::LiveStderr => CliProgressSink::live_stderr(config),
        }
    })
}

impl RunEventSink for CliProgressSink {
    fn try_emit(&self, event: &RunEvent) -> RunEventSinkResult {
        if let ProgressSinkMode::Live {
            sender,
            terminal_sender,
            ..
        } = &self.mode
        {
            return match sender.try_send(LiveProgressInput::Event(event.clone())) {
                Ok(()) => RunEventSinkResult::Accepted,
                Err(TrySendError::Full(LiveProgressInput::Event(event)))
                    if terminal_for_event(event.kind).is_some() =>
                {
                    match terminal_sender.try_send(event) {
                        Ok(()) => RunEventSinkResult::Accepted,
                        Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                            RunEventSinkResult::Dropped
                        }
                    }
                }
                Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                    RunEventSinkResult::Dropped
                }
            };
        }
        let Ok(mut state) = self.state.try_lock() else {
            return RunEventSinkResult::Dropped;
        };
        match state.apply_event(event, &self.config, MilestoneOverflow::Drop) {
            ProgressEventDisposition::Dropped => RunEventSinkResult::Dropped,
            ProgressEventDisposition::Accepted
            | ProgressEventDisposition::Duplicate
            | ProgressEventDisposition::OutOfOrder
            | ProgressEventDisposition::AfterTerminal
            | ProgressEventDisposition::Terminal => RunEventSinkResult::Accepted,
        }
    }

    fn progress_sink(&self) -> Option<Arc<dyn RunProgressSink>> {
        match &self.mode {
            ProgressSinkMode::Live { sender, .. } => Some(Arc::new(CliProgressObservationSink {
                sender: sender.clone(),
            })),
            ProgressSinkMode::Buffered => None,
        }
    }
}

fn spawn_live_progress_worker(
    config: ProgressConfig,
    state: Arc<Mutex<ProgressState>>,
    receiver: Receiver<LiveProgressInput>,
    terminal_receiver: Receiver<RunEvent>,
    writer: Box<dyn Write + Send>,
) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("cdf-cli-progress".to_owned())
        .spawn(move || {
            let mut renderer = LiveProgressRenderer::new(config, state, writer);
            loop {
                match receiver.recv_timeout(INTERACTIVE_REFRESH_INTERVAL) {
                    Ok(LiveProgressInput::Event(event)) => renderer.process(&event),
                    Ok(LiveProgressInput::Observation(observation)) => {
                        renderer.process_observation(&observation);
                    }
                    Err(RecvTimeoutError::Timeout) => renderer.refresh_if_due(),
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
            for event in terminal_receiver.try_iter() {
                renderer.process(&event);
            }
            renderer.finish();
        })
}

struct LiveProgressRenderer {
    config: ProgressConfig,
    state: Arc<Mutex<ProgressState>>,
    writer: Box<dyn Write + Send>,
    interactive: bool,
    last_refresh: Instant,
    pending_redraw: bool,
    rendered_lines: usize,
    headless_run_id: Option<String>,
    headless_phases: BTreeMap<(String, ProgressPhase), HeadlessPhaseEmission>,
}

#[derive(Clone, Copy)]
struct HeadlessPhaseEmission {
    status: ProgressStatus,
    last_emitted_at: Instant,
}

impl LiveProgressRenderer {
    fn new(
        config: ProgressConfig,
        state: Arc<Mutex<ProgressState>>,
        writer: Box<dyn Write + Send>,
    ) -> Self {
        let interactive = config.render_config().display_mode() == DisplayMode::Tty;
        let last_refresh = Instant::now()
            .checked_sub(INTERACTIVE_REFRESH_INTERVAL)
            .unwrap_or_else(Instant::now);
        Self {
            config,
            state,
            writer,
            interactive,
            last_refresh,
            pending_redraw: false,
            rendered_lines: 0,
            headless_run_id: None,
            headless_phases: BTreeMap::new(),
        }
    }

    fn process(&mut self, event: &RunEvent) {
        self.process_at(event, Instant::now());
    }

    fn process_at(&mut self, event: &RunEvent, now: Instant) {
        let disposition = {
            let Ok(mut state) = self.state.lock() else {
                return;
            };
            state.apply_event_at(event, &self.config, MilestoneOverflow::Coalesce, now)
        };
        if self.interactive {
            let terminal = disposition == ProgressEventDisposition::Terminal;
            if terminal
                || now.saturating_duration_since(self.last_refresh) >= INTERACTIVE_REFRESH_INTERVAL
            {
                self.redraw_at(now);
                self.last_refresh = now;
                self.pending_redraw = false;
            } else if matches!(
                disposition,
                ProgressEventDisposition::Accepted | ProgressEventDisposition::Terminal
            ) {
                self.pending_redraw = true;
            }
        } else if matches!(
            disposition,
            ProgressEventDisposition::Accepted | ProgressEventDisposition::Terminal
        ) {
            if self.headless_run_id.as_deref() != Some(event.run_id.as_str()) {
                self.headless_run_id = Some(event.run_id.as_str().to_owned());
            }
            self.emit_headless_updates(now, false, false, Some(event.run_id.as_str()));
        }
    }

    fn process_observation(&mut self, observation: &RunProgressObservation) {
        let now = Instant::now();
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        state.apply_observation(observation, now);
        drop(state);
        if self.interactive {
            self.redraw_at(now);
            self.last_refresh = now;
            self.pending_redraw = false;
        } else {
            if self.headless_run_id.as_deref() != Some(observation.run_id.as_str()) {
                self.headless_run_id = Some(observation.run_id.as_str().to_owned());
            }
            self.emit_headless_updates(now, false, true, Some(observation.run_id.as_str()));
        }
    }

    fn refresh_if_due(&mut self) {
        self.refresh_at(Instant::now());
    }

    fn refresh_at(&mut self, now: Instant) {
        if self.interactive {
            let has_running_phase = self.state.lock().is_ok_and(|state| {
                state
                    .phases
                    .values()
                    .any(|phase| phase.finished_at.is_none())
            });
            if (self.pending_redraw || has_running_phase)
                && now.saturating_duration_since(self.last_refresh) >= INTERACTIVE_REFRESH_INTERVAL
            {
                self.redraw_at(now);
                self.last_refresh = now;
                self.pending_redraw = false;
            }
        } else {
            self.emit_headless_updates(now, true, false, None);
        }
    }

    fn redraw(&mut self) {
        self.redraw_at(Instant::now());
    }

    fn redraw_at(&mut self, now: Instant) {
        self.rendered_lines = redraw_live_progress(
            &self.state,
            &self.config,
            &mut self.writer,
            self.rendered_lines,
            now,
        );
    }

    fn emit_headless_updates(
        &mut self,
        now: Instant,
        heartbeat_only: bool,
        force_current: bool,
        run_filter: Option<&str>,
    ) {
        let Some(snapshot) = self
            .state
            .lock()
            .ok()
            .map(|state| state.snapshot_at(self.config.verbosity, false, now))
        else {
            return;
        };
        self.headless_phases.retain(|(run_id, phase), _| {
            snapshot
                .phases
                .iter()
                .any(|candidate| &candidate.run_id == run_id && candidate.phase == *phase)
        });
        for phase in snapshot
            .phases
            .iter()
            .filter(|phase| run_filter.is_none_or(|run_id| phase.run_id == run_id))
        {
            let key = (phase.run_id.clone(), phase.phase);
            let previous = self.headless_phases.get(&key).copied();
            let status_changed = previous.is_none_or(|previous| previous.status != phase.status);
            let heartbeat_due = previous.is_some_and(|previous| {
                phase.status == ProgressStatus::Running
                    && now.saturating_duration_since(previous.last_emitted_at)
                        >= HEADLESS_HEARTBEAT_INTERVAL
            });
            if (!heartbeat_only && status_changed)
                || heartbeat_due
                || (force_current && phase.phase == snapshot.current_phase)
            {
                let _ = write_headless_phase(&mut self.writer, phase);
                self.headless_phases.insert(
                    key,
                    HeadlessPhaseEmission {
                        status: phase.status,
                        last_emitted_at: now,
                    },
                );
            }
        }
        let _ = self.writer.flush();
    }

    fn finish(&mut self) {
        if self.interactive {
            self.redraw();
            self.pending_redraw = false;
        }
    }
}

fn redraw_live_progress(
    state: &Arc<Mutex<ProgressState>>,
    config: &ProgressConfig,
    writer: &mut dyn Write,
    previous_lines: usize,
    now: Instant,
) -> usize {
    let snapshot = match state.lock() {
        Ok(state) => state.snapshot_at(config.verbosity, false, now),
        Err(_) => return previous_lines,
    };
    let rendered = snapshot.render_interactive(config.render_config(), config.verbosity);
    if rendered.is_empty() {
        return previous_lines;
    }
    if previous_lines > 0 {
        for _ in 0..previous_lines {
            let _ = writer.write_all(b"\x1b[1A\r\x1b[2K");
        }
    }
    let _ = writer.write_all(rendered.as_bytes());
    let _ = writer.flush();
    rendered.lines().count()
}

fn write_headless_phase(
    writer: &mut dyn Write,
    phase: &ProgressPhaseSnapshot,
) -> std::io::Result<()> {
    let timestamp_ms = phase
        .timestamp_ms
        .saturating_add(i64::try_from(phase.elapsed.as_millis()).unwrap_or(i64::MAX));
    write!(
        writer,
        "{} [{}] {} {} run={} elapsed={}",
        timestamp_ms,
        phase.phase.as_str(),
        phase.status.as_str(),
        phase.detail,
        phase.run_id,
        humanize_progress_duration(phase.elapsed)
    )?;
    if let Some(rows) = phase.metrics.rows {
        write!(writer, " rows={}", humanize_rows(rows))?;
    }
    if let Some(bytes) = phase.metrics.bytes {
        write!(writer, " bytes={}", humanize_bytes(bytes))?;
    }
    if let Some(segments) = phase.metrics.segments {
        write!(writer, " segments={segments}")?;
    }
    if let Some(batches) = phase.metrics.batches {
        write!(writer, " batches={batches}")?;
    }
    if let Some(rows) = phase.metrics.quarantine_rows {
        write!(writer, " quarantine_rows={}", humanize_rows(rows))?;
    }
    if let Some(rate) = progress_rate(phase) {
        write!(writer, " rate={rate}")?;
    }
    if let Some(notice) = &phase.notice {
        write!(writer, " status={}", notice.replace(' ', "_"))?;
    }
    writeln!(writer)
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

fn phase_for_event(event: &RunEvent) -> ProgressPhase {
    if event.kind == RunEventKind::PhaseMeasured
        && let Some(RunEventValue::PhaseMetric(metric)) = event.details.attributes.get("metric")
    {
        return progress_phase_for_runtime_phase(metric.phase);
    }
    match event.kind {
        RunEventKind::RunStarted | RunEventKind::PlanRecorded => ProgressPhase::Plan,
        RunEventKind::PackageStarted
        | RunEventKind::PackageSegmentRecorded
        | RunEventKind::SourceRetryRecorded => ProgressPhase::Extract,
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

fn progress_phase_for_runtime_phase(phase: RunPhase) -> ProgressPhase {
    match phase {
        RunPhase::SourceRead => ProgressPhase::Extract,
        RunPhase::Decode | RunPhase::ValidationNormalization => ProgressPhase::Validate,
        RunPhase::PackageExecution
        | RunPhase::SegmentEncode
        | RunPhase::PersistHash
        | RunPhase::PackageFinalize => ProgressPhase::Package,
        RunPhase::DestinationIngress | RunPhase::DestinationWriteReceipt => ProgressPhase::Commit,
        RunPhase::CheckpointGate => ProgressPhase::Gate,
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

fn activity_verb(phase: ProgressPhase) -> &'static str {
    match phase {
        ProgressPhase::Plan => "Planned",
        ProgressPhase::Extract => "Read",
        ProgressPhase::Validate => "Validated",
        ProgressPhase::Package => "Packaged",
        ProgressPhase::Commit => "Loaded",
        ProgressPhase::Verify => "Verified",
        ProgressPhase::Gate => "Committed",
    }
}

fn event_detail(event: &RunEvent) -> String {
    event
        .resource_id
        .as_ref()
        .map(|resource| redact_uri_userinfo(resource.as_str()))
        .or_else(|| event.package_id.as_deref().map(redact_uri_userinfo))
        .unwrap_or_else(|| event.kind.as_str().replace('_', " "))
}

fn activity_metrics(phase: &ProgressPhaseSnapshot) -> Vec<String> {
    let mut metrics = vec![humanize_progress_duration(phase.elapsed)];
    if let Some(rows) = phase.metrics.rows {
        metrics.push(format!("{} rows", humanize_rows(rows)));
    }
    if let Some(bytes) = phase.metrics.bytes {
        metrics.push(humanize_bytes(bytes));
    }
    if let Some(segments) = phase.metrics.segments {
        metrics.push(format!("{segments} segments"));
    }
    if let Some(batches) = phase.metrics.batches {
        metrics.push(format!("{batches} batches"));
    }
    if let Some(quarantine_rows) = phase.metrics.quarantine_rows {
        metrics.push(format!("{} quarantined", humanize_rows(quarantine_rows)));
    }
    if let Some(rate) = progress_rate(phase) {
        metrics.push(rate);
    }
    metrics
}

fn progress_rate(phase: &ProgressPhaseSnapshot) -> Option<String> {
    if phase.elapsed.is_zero() {
        return None;
    }
    if let Some(rows) = phase.metrics.rows {
        let per_second = rows as f64 / phase.elapsed.as_secs_f64();
        return Some(format!(
            "{} rows/s",
            humanize_rows(per_second.round() as u64)
        ));
    }
    phase.metrics.bytes.map(|bytes| {
        let per_second = bytes as f64 / phase.elapsed.as_secs_f64();
        format!("{}/s", humanize_bytes(per_second.round() as u64))
    })
}

fn humanize_progress_duration(duration: Duration) -> String {
    if duration.as_secs() < 60 && duration >= Duration::from_secs(1) {
        return format!("{:.1}s", duration.as_secs_f64());
    }
    humanize_duration(duration)
}

impl ProgressMetrics {
    fn apply_event(&mut self, event: &RunEvent) {
        match event.kind {
            RunEventKind::PackageSegmentRecorded | RunEventKind::DestinationSegmentAcknowledged => {
                self.rows = add_optional(self.rows, event_u64(event, "row_count"));
                self.bytes = add_optional(self.bytes, event_u64(event, "byte_count"));
                self.batches = add_optional(self.batches, event_u64(event, "batch_count"));
                self.segments = Some(self.segments.unwrap_or(0).saturating_add(1));
            }
            RunEventKind::PackageFinalized => {
                self.rows = event_u64(event, "row_count");
                self.bytes = event_u64(event, "byte_count");
                self.batches = event_u64(event, "batch_count");
                self.segments = event_u64(event, "segment_count");
                self.quarantine_rows = event_u64(event, "quarantine_record_count");
            }
            RunEventKind::DestinationReceiptRecorded => {
                self.rows = event_u64(event, "rows_written");
                self.segments = event_u64(event, "segment_ack_count");
            }
            RunEventKind::CheckpointProposed | RunEventKind::CheckpointCommitted => {
                self.rows = event_u64(event, "row_count");
                self.bytes = event_u64(event, "byte_count");
                self.segments = event_u64(event, "segment_count");
            }
            _ => {}
        }
    }
}

fn event_u64(event: &RunEvent, key: &str) -> Option<u64> {
    match event.details.attributes.get(key) {
        Some(RunEventValue::U64(value)) => Some(*value),
        _ => None,
    }
}

fn add_optional(current: Option<u64>, increment: Option<u64>) -> Option<u64> {
    increment
        .map(|increment| current.unwrap_or(0).saturating_add(increment))
        .or(current)
}

fn progress_notice(event: &RunEvent) -> Option<String> {
    let retry_after = event_u64(event, "retry_after_ms");
    let waiting = event
        .details
        .attributes
        .get("backoff_notice")
        .is_some_and(|value| value == &RunEventValue::Bool(true));
    match (waiting, retry_after) {
        (true, Some(delay)) => Some(format!(
            "waiting {} before retry",
            humanize_duration(Duration::from_millis(delay))
        )),
        (true, None) => Some("waiting before retry".to_owned()),
        _ => None,
    }
}

fn error_kind_label(kind: &cdf_kernel::ErrorKind) -> &'static str {
    match kind {
        cdf_kernel::ErrorKind::Transient => "transient",
        cdf_kernel::ErrorKind::RateLimited => "rate_limited",
        cdf_kernel::ErrorKind::Auth => "auth",
        cdf_kernel::ErrorKind::Contract => "contract",
        cdf_kernel::ErrorKind::Data => "data",
        cdf_kernel::ErrorKind::Destination => "destination",
        cdf_kernel::ErrorKind::Environment => "environment",
        cdf_kernel::ErrorKind::Internal => "internal",
    }
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
        "metric",
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
            let mode = match &metric.context {
                Some(cdf_kernel::RunPhaseContext::SourceRead { mode }) => mode.as_str(),
                None => "unclassified",
            };
            format!(
                "source_read {mode} {:?} {} physical / {} useful / {} waste across {} requests in {}",
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
    use std::{
        collections::BTreeMap,
        io,
        sync::Arc,
        time::{Duration, Instant},
    };

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

        assert!(human_progress_sink(false, &policy, ProgressDelivery::Buffered).is_none());
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

    #[derive(Clone)]
    struct SlowSharedWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
        delay: Duration,
    }

    impl Write for SlowSharedWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            std::thread::sleep(self.delay);
            self.bytes.lock().unwrap().extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn headless_live_coalescing_state_stays_bounded_across_many_runs() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(Mutex::new(ProgressState::default()));
        let mut renderer = LiveProgressRenderer::new(
            ProgressConfig::new(headless_config(), DisplayVerbosity::Normal),
            state,
            Box::new(SlowSharedWriter {
                bytes,
                delay: Duration::ZERO,
            }),
        );

        for index in 0..1_000 {
            renderer.process(&event_for_run(
                &format!("run-progress-slice-{index:04}"),
                1,
                RunEventKind::RunStarted,
            ));
        }

        assert_eq!(
            renderer.headless_run_id.as_deref(),
            Some("run-progress-slice-0999")
        );
        assert_eq!(renderer.headless_phases.len(), DEFAULT_PROGRESS_CAPACITY);
    }

    #[test]
    fn interactive_progress_refreshes_a_deferred_phase_without_another_event() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(Mutex::new(ProgressState::default()));
        let mut renderer = LiveProgressRenderer::new(
            ProgressConfig::new(tty_config(), DisplayVerbosity::Normal),
            state,
            Box::new(SlowSharedWriter {
                bytes: Arc::clone(&bytes),
                delay: Duration::ZERO,
            }),
        );

        renderer.process(&event(1, RunEventKind::PackageFinalized));
        renderer.process(&event(2, RunEventKind::DestinationCommitStarted));
        assert!(renderer.pending_redraw);

        renderer.last_refresh = Instant::now()
            .checked_sub(INTERACTIVE_REFRESH_INTERVAL)
            .unwrap();
        renderer.refresh_if_due();

        assert!(!renderer.pending_redraw);
        let rendered = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(rendered.contains("Loaded"), "{rendered:?}");
    }

    #[test]
    fn active_phase_redraw_advances_elapsed_time_without_another_event() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(Mutex::new(ProgressState::default()));
        let mut renderer = LiveProgressRenderer::new(
            ProgressConfig::new(tty_config(), DisplayVerbosity::Normal),
            state,
            Box::new(SlowSharedWriter {
                bytes: Arc::clone(&bytes),
                delay: Duration::ZERO,
            }),
        );
        let started_at = Instant::now();
        let mut started = event(1, RunEventKind::PackageStarted);
        started.details.attributes.clear();

        renderer.process_at(&started, started_at);
        renderer.refresh_at(started_at + Duration::from_millis(1_100));
        renderer.refresh_at(started_at + Duration::from_millis(2_200));

        let rendered = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(rendered.contains("1.1s"), "{rendered:?}");
        assert!(rendered.contains("2.2s"), "{rendered:?}");
    }

    #[test]
    fn typed_phase_metrics_accumulate_increments_and_replace_cumulative_totals() {
        let config = ProgressConfig::new(tty_config(), DisplayVerbosity::Normal);
        let mut state = ProgressState::default();
        let started_at = Instant::now();
        let mut started = event(1, RunEventKind::PackageStarted);
        started.details.attributes.clear();
        state.apply_event_at(&started, &config, MilestoneOverflow::Coalesce, started_at);
        for (sequence, rows, bytes, batches) in [(2, 1_000, 1_048_576, 2), (3, 500, 524_288, 1)] {
            let mut segment = event(sequence, RunEventKind::PackageSegmentRecorded);
            segment
                .details
                .attributes
                .insert("row_count".to_owned(), RunEventValue::U64(rows));
            segment
                .details
                .attributes
                .insert("byte_count".to_owned(), RunEventValue::U64(bytes));
            segment
                .details
                .attributes
                .insert("batch_count".to_owned(), RunEventValue::U64(batches));
            state.apply_event_at(
                &segment,
                &config,
                MilestoneOverflow::Coalesce,
                started_at + Duration::from_secs(1),
            );
        }
        let mut proposed = event(4, RunEventKind::CheckpointProposed);
        proposed
            .details
            .attributes
            .insert("row_count".to_owned(), RunEventValue::U64(1_500));
        proposed
            .details
            .attributes
            .insert("byte_count".to_owned(), RunEventValue::U64(1_572_864));
        proposed
            .details
            .attributes
            .insert("segment_count".to_owned(), RunEventValue::U64(2));
        state.apply_event_at(
            &proposed,
            &config,
            MilestoneOverflow::Coalesce,
            started_at + Duration::from_secs(2),
        );
        let mut committed = event(5, RunEventKind::CheckpointCommitted);
        committed
            .details
            .attributes
            .insert("row_count".to_owned(), RunEventValue::U64(1_600));
        committed
            .details
            .attributes
            .insert("byte_count".to_owned(), RunEventValue::U64(1_677_721));
        committed
            .details
            .attributes
            .insert("segment_count".to_owned(), RunEventValue::U64(3));
        state.apply_event_at(
            &committed,
            &config,
            MilestoneOverflow::Coalesce,
            started_at + Duration::from_secs(3),
        );

        let snapshot = state.snapshot_at(
            DisplayVerbosity::Normal,
            false,
            started_at + Duration::from_secs(4),
        );
        let extract = snapshot
            .phases
            .iter()
            .find(|phase| phase.phase == ProgressPhase::Extract)
            .unwrap();
        assert_eq!(extract.metrics.rows, Some(1_500));
        assert_eq!(extract.metrics.bytes, Some(1_572_864));
        assert_eq!(extract.metrics.batches, Some(3));
        assert_eq!(extract.metrics.segments, Some(2));
        assert_eq!(progress_rate(extract).as_deref(), Some("750 rows/s"));
        let gate = snapshot
            .phases
            .iter()
            .find(|phase| phase.phase == ProgressPhase::Gate)
            .unwrap();
        assert_eq!(gate.metrics.rows, Some(1_600));
        assert_eq!(gate.metrics.bytes, Some(1_677_721));
        assert_eq!(gate.metrics.segments, Some(3));
    }

    #[test]
    fn normal_progress_omits_unknown_metrics_but_always_shows_elapsed() {
        let config = ProgressConfig::new(tty_config(), DisplayVerbosity::Normal);
        let mut state = ProgressState::default();
        let started_at = Instant::now();
        let mut started = event(1, RunEventKind::PackageStarted);
        started.details.attributes.clear();
        state.apply_event_at(&started, &config, MilestoneOverflow::Coalesce, started_at);
        let rendered = state
            .snapshot_at(
                DisplayVerbosity::Normal,
                false,
                started_at + Duration::from_millis(500),
            )
            .render_interactive(config.render_config(), DisplayVerbosity::Normal);

        assert!(rendered.contains("500ms"), "{rendered:?}");
        assert!(!rendered.contains(" rows"), "{rendered:?}");
        assert!(!rendered.contains(" bytes"), "{rendered:?}");
        assert!(!rendered.contains("segments"), "{rendered:?}");
        assert!(!rendered.contains("batches"), "{rendered:?}");
    }

    #[test]
    fn failure_preserves_current_phase_elapsed_and_last_known_metrics() {
        let config = ProgressConfig::new(tty_config(), DisplayVerbosity::Normal);
        let mut state = ProgressState::default();
        let started_at = Instant::now();
        state.apply_event_at(
            &event(1, RunEventKind::DestinationCommitStarted),
            &config,
            MilestoneOverflow::Coalesce,
            started_at,
        );
        let mut acknowledged = event(2, RunEventKind::DestinationSegmentAcknowledged);
        acknowledged
            .details
            .attributes
            .insert("row_count".to_owned(), RunEventValue::U64(640_000));
        acknowledged
            .details
            .attributes
            .insert("byte_count".to_owned(), RunEventValue::U64(459_276_288));
        state.apply_event_at(
            &acknowledged,
            &config,
            MilestoneOverflow::Coalesce,
            started_at + Duration::from_secs(2),
        );
        state.apply_event_at(
            &event(3, RunEventKind::RunFailed),
            &config,
            MilestoneOverflow::Coalesce,
            started_at + Duration::from_secs(3),
        );

        let snapshot = state.snapshot_at(
            DisplayVerbosity::Normal,
            false,
            started_at + Duration::from_secs(20),
        );
        let commit = snapshot
            .phases
            .iter()
            .find(|phase| phase.phase == ProgressPhase::Commit)
            .unwrap();
        assert_eq!(commit.status, ProgressStatus::Failed);
        assert_eq!(commit.elapsed, Duration::from_secs(3));
        assert_eq!(commit.metrics.rows, Some(640_000));
        assert_eq!(commit.metrics.bytes, Some(459_276_288));
        assert!(
            !snapshot
                .phases
                .iter()
                .any(|phase| phase.phase == ProgressPhase::Gate)
        );
    }

    #[test]
    fn headless_live_progress_emits_bounded_heartbeat_and_phase_completion() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(Mutex::new(ProgressState::default()));
        let mut renderer = LiveProgressRenderer::new(
            ProgressConfig::new(headless_config(), DisplayVerbosity::Normal),
            state,
            Box::new(SlowSharedWriter {
                bytes: Arc::clone(&bytes),
                delay: Duration::ZERO,
            }),
        );
        let started_at = Instant::now();
        let mut started = event(1, RunEventKind::PackageStarted);
        started.details.attributes.clear();
        renderer.process_at(&started, started_at);
        let start_len = bytes.lock().unwrap().len();

        renderer.refresh_at(started_at + Duration::from_secs(29));
        assert_eq!(bytes.lock().unwrap().len(), start_len);
        renderer.refresh_at(started_at + Duration::from_secs(30));
        let heartbeat_len = bytes.lock().unwrap().len();
        assert!(heartbeat_len > start_len);
        renderer.refresh_at(started_at + Duration::from_secs(45));
        assert_eq!(bytes.lock().unwrap().len(), heartbeat_len);

        let mut finalized = event(2, RunEventKind::PackageFinalized);
        finalized
            .details
            .attributes
            .insert("row_count".to_owned(), RunEventValue::U64(10));
        renderer.process_at(&finalized, started_at + Duration::from_secs(46));
        let rendered = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(rendered.contains("[extract] running"), "{rendered:?}");
        assert!(rendered.contains("elapsed=30.0s"), "{rendered:?}");
        assert!(rendered.contains("[extract] succeeded"), "{rendered:?}");
        assert!(rendered.contains("[package] running"), "{rendered:?}");
        assert!(!rendered.contains("\u{1b}["));
        assert!(!rendered.contains('\r'));
    }

    #[test]
    fn source_retry_observation_is_rendered_immediately_without_ledger_event() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let sink = CliProgressSink::live_with_writer(
            ProgressConfig::new(tty_config(), DisplayVerbosity::Normal),
            Box::new(SlowSharedWriter {
                bytes: Arc::clone(&bytes),
                delay: Duration::ZERO,
            }),
        );
        assert_eq!(
            sink.try_emit(&event(1, RunEventKind::PackageStarted)),
            RunEventSinkResult::Accepted
        );
        let progress = sink.progress_sink().unwrap();
        assert_eq!(
            progress.try_emit_progress(&RunProgressObservation {
                run_id: RunId::new("run-progress-test").unwrap(),
                resource_id: ResourceId::new("local.events").unwrap(),
                scope: ScopeKey::Resource,
                package_id: "pkg-progress-test".to_owned(),
                phase: RunPhase::SourceRead,
                kind: RunProgressObservationKind::SourceRetry {
                    failed_attempt: 1,
                    cause: cdf_kernel::ErrorKind::RateLimited,
                    delay_ms: 2_000,
                },
            }),
            RunEventSinkResult::Accepted
        );
        drop(progress);
        let snapshot = sink.finish();
        assert!(snapshot.streamed_live);
        let rendered = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(
            rendered.contains("retry 1 after rate_limited; waiting 2s"),
            "{rendered:?}"
        );
    }

    #[test]
    fn live_progress_never_backpressures_runtime_and_preserves_terminal_event() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let sink = CliProgressSink::live_with_writer(
            ProgressConfig::new(tty_config(), DisplayVerbosity::Normal).with_capacity(1),
            Box::new(SlowSharedWriter {
                bytes: Arc::clone(&bytes),
                delay: Duration::from_millis(25),
            }),
        );

        let started = Instant::now();
        let mut dropped = 0;
        for sequence in 1..=10_000 {
            if sink.try_emit(&event(sequence, RunEventKind::PackageSegmentRecorded))
                == RunEventSinkResult::Dropped
            {
                dropped += 1;
            }
        }
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "runtime event emission waited on the slow terminal writer"
        );
        assert!(dropped > 0, "the bounded live queue never saturated");
        assert_eq!(
            sink.try_emit(&event(10_001, RunEventKind::RunSucceeded)),
            RunEventSinkResult::Accepted
        );

        let snapshot = sink.finish();
        assert_eq!(
            snapshot.last_disposition(),
            Some(ProgressEventDisposition::Terminal)
        );
        assert!(snapshot.render_for_config(&tty_config()).is_empty());
        let rendered = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
        assert!(rendered.contains("Committed"), "{rendered:?}");
    }

    #[test]
    fn source_read_metric_names_physical_useful_waste_and_requests() {
        let metric = RunEventValue::PhaseMetric(RunPhaseMetric {
            phase: RunPhase::SourceRead,
            context: Some(cdf_kernel::RunPhaseContext::SourceRead {
                mode: cdf_kernel::SourceReadMode::GrowingSpool,
            }),
            status: RunPhaseStatus::Completed,
            duration_ns: 2_000_000,
            input_bytes: 10 * 1024 * 1024,
            output_bytes: 8 * 1024 * 1024,
            operations: 3,
        });
        let rendered = display_event_value("metric", &metric);
        assert_eq!(
            rendered,
            "source_read growing_spool Completed 10 MiB physical / 8 MiB useful / 2 MiB waste across 3 requests in 2ms"
        );

        let mut event = event(1, RunEventKind::PhaseMeasured);
        event.details.attributes.insert("metric".to_owned(), metric);
        assert!(
            milestone_fields(&event, DisplayVerbosity::Normal)
                .iter()
                .any(|(key, value)| key == "metric" && value == &rendered)
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
            assert_eq!(phase_for_event(&event(1, kind)), phase);
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
    fn sequence_bookkeeping_is_bounded_for_many_runs() {
        let sink = bounded_sink(4);
        for index in 0..1_000 {
            let run_id = format!("run-progress-slice-{index:04}");
            assert_eq!(
                sink.try_emit(&event_for_run(&run_id, 1, RunEventKind::RunStarted)),
                if index < 4 {
                    RunEventSinkResult::Accepted
                } else {
                    RunEventSinkResult::Dropped
                }
            );
        }

        assert_eq!(sink.state.lock().unwrap().max_sequence_by_run.len(), 4);
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
