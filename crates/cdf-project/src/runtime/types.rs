use super::{
    destinations::ResolvedProjectDestination, hooks::ReceiptVerifiedHook, prelude::*,
    resources::ProjectRunSource,
};

pub struct PreparedDuckDbReplayRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a DuckDbDestination,
    pub checkpoint_store: &'a Store,
    pub delta: StateDelta,
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub merge_keys: Vec<String>,
    pub schema_hash: SchemaHash,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PreparedDuckDbRecoveryRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a DuckDbDestination,
    pub checkpoint_store: &'a Store,
    pub delta: StateDelta,
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub schema_hash: SchemaHash,
    pub receipt: Receipt,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactDuckDbReplayRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a DuckDbDestination,
    pub checkpoint_store: &'a Store,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactDuckDbRecoveryRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a DuckDbDestination,
    pub checkpoint_store: &'a Store,
    pub receipt: Receipt,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactParquetRecoveryRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a ParquetDestination,
    pub checkpoint_store: &'a Store,
    pub receipt: Receipt,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactParquetReplayRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a ParquetDestination,
    pub checkpoint_store: &'a Store,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactPostgresRecoveryRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a PostgresDestination,
    pub checkpoint_store: &'a Store,
    pub receipt: Receipt,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PackageArtifactPostgresReplayRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: &'a PostgresDestination,
    pub checkpoint_store: &'a Store,
    pub target: PostgresTarget,
    pub dedup: MergeDedupPolicy,
    pub existing_table: Option<PostgresExistingTable>,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedDuckDbReplayReport {
    pub checkpoint: Checkpoint,
    pub receipt: Receipt,
    pub receipt_source: PreparedReceiptSource,
    pub package_status: PackageStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PreparedReceiptSource {
    DuckDbCommit {
        duplicate: bool,
        package_receipt_recorded: bool,
    },
    SuppliedDurableReceipt,
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

impl ProjectReceiptSource {
    pub(super) fn into_duckdb_receipt_source(self) -> PreparedReceiptSource {
        match self {
            Self::DestinationCommit {
                duplicate,
                package_receipt_recorded,
            } => PreparedReceiptSource::DuckDbCommit {
                duplicate,
                package_receipt_recorded,
            },
            Self::DestinationCommitReceiptOnly { .. } => {
                unreachable!("Postgres receipt-only metadata cannot become a DuckDB report")
            }
            Self::SuppliedDurableReceipt => PreparedReceiptSource::SuppliedDurableReceipt,
        }
    }
}

impl From<PreparedReceiptSource> for ProjectReceiptSource {
    fn from(source: PreparedReceiptSource) -> Self {
        match source {
            PreparedReceiptSource::DuckDbCommit {
                duplicate,
                package_receipt_recorded,
            } => Self::DestinationCommit {
                duplicate,
                package_receipt_recorded,
            },
            PreparedReceiptSource::SuppliedDurableReceipt => Self::SuppliedDurableReceipt,
        }
    }
}

pub struct LocalFileDuckDbRunRequest<'a> {
    pub resource: &'a CompiledResource,
    pub plan: EnginePlan,
    pub package_root: PathBuf,
    pub destination_path: PathBuf,
    pub state_store_path: PathBuf,
    pub pipeline_id: PipelineId,
    pub target: TargetName,
    pub package_id: String,
    pub checkpoint_id: CheckpointId,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
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
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFileDuckDbRunReport {
    pub package_dir: PathBuf,
    pub package_id: String,
    pub package_hash: PackageHash,
    pub package_status: PackageStatus,
    pub checkpoint: Checkpoint,
    pub receipt: Receipt,
    pub receipt_source: PreparedReceiptSource,
    pub row_count: u64,
    pub segment_count: usize,
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
    pub segment_count: usize,
}

impl ProjectRunReport {
    pub(super) fn into_local_file_duckdb_report(self) -> LocalFileDuckDbRunReport {
        LocalFileDuckDbRunReport {
            package_dir: self.package_dir,
            package_id: self.package_id,
            package_hash: self.package_hash,
            package_status: self.package_status,
            checkpoint: self.checkpoint,
            receipt: self.receipt,
            receipt_source: self.receipt_source.into_duckdb_receipt_source(),
            row_count: self.row_count,
            segment_count: self.segment_count,
        }
    }
}
