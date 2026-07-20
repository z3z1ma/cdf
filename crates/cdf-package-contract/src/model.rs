use cdf_kernel::{CdfError, PackageHash, Receipt, Result, SegmentId};
use serde::{Deserialize, Serialize};

pub const MANIFEST_VERSION: u16 = 2;
pub const MANIFEST_FILE: &str = "manifest.json";
pub const TRACE_FILE: &str = "trace.jsonl";
pub const RECEIPTS_FILE: &str = "destination/receipts.json";
pub const REQUIRED_DIRECTORIES: &[&str] = &[
    "plan",
    "schema",
    "data",
    "carryover",
    "quarantine",
    "stats",
    "lineage",
    "state",
    "destination",
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PackageManifest {
    pub manifest_version: u16,
    pub package_hash: String,
    pub identity: ManifestIdentity,
    pub lifecycle: LifecycleState,
    pub signature: SignatureSlot,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archives: Option<ManifestArchives>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestIdentity {
    pub manifest_version: u16,
    pub package_id: String,
    pub layout: Vec<String>,
    pub files: Vec<FileEntry>,
    pub segments: Vec<SegmentEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub byte_count: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentEntry {
    pub segment_id: SegmentId,
    pub path: String,
    pub package_row_ord_start: u64,
    pub row_count: u64,
    pub byte_count: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleState {
    pub status: PackageStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignatureSlot {
    pub signing_input: String,
    pub value: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestArchives {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parquet: Option<ParquetArchiveMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetArchiveMetadata {
    pub format_version: u16,
    pub fidelity_report_path: String,
    pub fidelity_statement: String,
    pub segments: Vec<ArchiveSegmentMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveSegmentMetadata {
    pub segment_id: String,
    pub source_path: String,
    pub source_byte_count: u64,
    pub source_sha256: String,
    pub source_row_count: u64,
    pub archive_path: String,
    pub archive_byte_count: u64,
    pub archive_sha256: String,
    pub archive_row_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PackageStatus {
    Planned,
    Extracting,
    Validated,
    Packaged,
    Loading,
    Loaded,
    Committed,
    Checkpointed,
    Archived,
}

impl PackageStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Extracting => "extracting",
            Self::Validated => "validated",
            Self::Packaged => "packaged",
            Self::Loading => "loading",
            Self::Loaded => "loaded",
            Self::Committed => "committed",
            Self::Checkpointed => "checkpointed",
            Self::Archived => "archived",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "planned" => Ok(Self::Planned),
            "extracting" => Ok(Self::Extracting),
            "validated" => Ok(Self::Validated),
            "packaged" => Ok(Self::Packaged),
            "loading" => Ok(Self::Loading),
            "loaded" => Ok(Self::Loaded),
            "committed" => Ok(Self::Committed),
            "checkpointed" => Ok(Self::Checkpointed),
            "archived" => Ok(Self::Archived),
            other => Err(CdfError::data(format!("unknown package status {other:?}"))),
        }
    }

    pub fn is_replayable(&self) -> bool {
        self.rank() >= Self::Packaged.rank() && self != &Self::Archived
    }

    pub fn is_archivable(&self) -> bool {
        matches!(
            self,
            Self::Packaged | Self::Loaded | Self::Committed | Self::Checkpointed
        )
    }

    fn rank(&self) -> u8 {
        match self {
            Self::Planned => 0,
            Self::Extracting => 1,
            Self::Validated => 2,
            Self::Packaged => 3,
            Self::Loading => 4,
            Self::Loaded => 5,
            Self::Committed => 6,
            Self::Checkpointed => 7,
            Self::Archived => 8,
        }
    }
}

impl TryFrom<&str> for PackageStatus {
    type Error = CdfError;

    fn try_from(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationReport {
    pub package_hash: String,
    pub checked_file_count: usize,
    pub checked_archive_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TombstoneReport {
    pub package_hash: String,
    pub removed_file_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayView {
    pub package_hash: PackageHash,
    pub status: PackageStatus,
    pub segments: Vec<SegmentEntry>,
    pub receipts: Vec<Receipt>,
}
