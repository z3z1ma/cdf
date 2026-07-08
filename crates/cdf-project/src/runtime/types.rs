use super::{
    destinations::ResolvedProjectDestination, hooks::ReceiptVerifiedHook, prelude::*,
    resources::ProjectRunSource,
};

pub struct PreparedPackageReplayRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: ResolvedProjectDestination,
    pub checkpoint_store: &'a Store,
    pub inputs: PackageReplayInputs,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
}

pub struct PreparedPackageRecoveryRequest<'a, Store: CheckpointStore + ?Sized> {
    pub package_dir: PathBuf,
    pub destination: ResolvedProjectDestination,
    pub checkpoint_store: &'a Store,
    pub inputs: PackageReplayInputs,
    pub receipt: Receipt,
    pub after_receipt_verified: Option<ReceiptVerifiedHook<'a>>,
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

#[cfg(test)]
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

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalFileDuckDbRunReport {
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
    #[cfg(test)]
    pub(super) fn into_local_file_duckdb_report(self) -> LocalFileDuckDbRunReport {
        LocalFileDuckDbRunReport {
            package_dir: self.package_dir,
            package_id: self.package_id,
            package_hash: self.package_hash,
            package_status: self.package_status,
            checkpoint: self.checkpoint,
            receipt: self.receipt,
            receipt_source: self.receipt_source,
            row_count: self.row_count,
            segment_count: self.segment_count,
        }
    }
}
