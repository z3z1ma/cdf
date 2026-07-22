use super::{
    destinations::ResolvedProjectDestination, hooks::ReceiptVerifiedHook, prelude::*,
    resources::ProjectRunSource,
};
use cdf_kernel::TerminalSchemaObservationQuarantine;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunTelemetryConfig {
    pub phase_metrics: bool,
    pub max_phase_events: u16,
    pub statistics_profile: bool,
}

impl RunTelemetryConfig {
    pub const fn disabled() -> Self {
        Self {
            phase_metrics: false,
            max_phase_events: 0,
            statistics_profile: false,
        }
    }

    pub const fn phase_metrics() -> Self {
        Self {
            phase_metrics: true,
            max_phase_events: 32,
            statistics_profile: false,
        }
    }

    pub const fn with_statistics_profile(mut self, enabled: bool) -> Self {
        self.statistics_profile = enabled;
        self
    }
}

impl Default for RunTelemetryConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

pub struct PackageArtifactReplayRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: ResolvedProjectDestination,
    pub checkpoint_store: &'a Store,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactRecoveryRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub checkpoint_store: &'a Store,
    pub destination: ResolvedProjectDestination,
    pub receipt: Receipt,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageReplayReport {
    pub checkpoint: Checkpoint,
    pub receipt: Receipt,
    pub receipt_source: ProjectReceiptSource,
    pub package_status: PackageStatus,
    pub phase_metrics: Vec<cdf_kernel::RunPhaseMetric>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectReceiptSource {
    DestinationCommit {
        duplicate: bool,
        package_receipt_recorded: bool,
    },
    DestinationCommitReceiptOnly {
        package_receipt_recorded: bool,
    },
    SuppliedDurableReceipt,
}

pub struct ProjectRunRequest<'a> {
    pub resource: ProjectRunSource<'a>,
    pub plan: EnginePlan,
    pub package_root: PathBuf,
    pub state_store_path: PathBuf,
    pub pipeline_id: PipelineId,
    pub package_id: String,
    pub checkpoint_id: CheckpointId,
    pub destination: ResolvedProjectDestination,
    pub run_id: Option<RunId>,
    pub event_sink: Option<&'a dyn RunEventSink>,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectRunReport {
    pub run_id: RunId,
    pub ledger_snapshot: RunLedgerSnapshot,
    pub package_dir: PathBuf,
    pub package_id: String,
    pub package_hash: PackageHash,
    pub package_status: PackageStatus,
    pub checkpoint: Checkpoint,
    pub receipt: Receipt,
    pub receipt_source: ProjectReceiptSource,
    pub row_count: u64,
    pub segment_count: u64,
    pub file_manifest: Option<FileManifestRunSummary>,
    pub terminal_schema_quarantines: Vec<TerminalSchemaObservationQuarantine>,
    pub runtime_scheduler: cdf_runtime::RuntimeSchedulerReport,
    pub source_frontier: cdf_runtime::SourceFrontierReport,
    pub drain: Option<ProjectDrainRunReport>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProjectRunOutcome {
    Committed(Box<ProjectRunReport>),
    NoOp(Box<ProjectRunNoOpReport>),
}

impl ProjectRunOutcome {
    pub fn into_committed(self) -> Result<ProjectRunReport> {
        match self {
            Self::Committed(report) => Ok(*report),
            Self::NoOp(report) => Err(CdfError::data(format!(
                "run completed as a verified no-op ({})",
                report.reason.as_str()
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectRunNoOpReport {
    pub run_id: RunId,
    pub ledger_snapshot: RunLedgerSnapshot,
    pub reason: ProjectRunNoOpReason,
    pub current_checkpoint: Option<Checkpoint>,
    pub file_manifest: Option<FileManifestRunSummary>,
    pub terminal_schema_quarantines: Vec<TerminalSchemaObservationQuarantine>,
    pub runtime_scheduler: cdf_runtime::RuntimeSchedulerReport,
    pub source_frontier: cdf_runtime::SourceFrontierReport,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectRunNoOpReason {
    FileManifestUnchanged,
    SourcePositionUnchanged,
    SourceExhausted,
}

impl ProjectRunNoOpReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FileManifestUnchanged => "file_manifest_unchanged",
            Self::SourcePositionUnchanged => "source_position_unchanged",
            Self::SourceExhausted => "source_exhausted",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectDrainRunReport {
    pub epoch_count: u64,
    pub total_row_count: u64,
    pub total_segment_count: u64,
    pub first_run_id: RunId,
    pub last_epoch: Box<ProjectDrainEpochReport>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectDrainEpochReport {
    pub epoch_ordinal: u64,
    pub run_id: RunId,
    pub package_dir: PathBuf,
    pub package_id: String,
    pub package_hash: PackageHash,
    pub checkpoint: Checkpoint,
    pub receipt: Receipt,
    pub row_count: u64,
    pub segment_count: u64,
    pub closure: cdf_kernel::EpochClosureEvidence,
    pub observed_at_unix_milliseconds: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileManifestRunSummary {
    pub total_file_count: u64,
    pub changed_file_count: u64,
    pub unchanged_file_count: u64,
}
