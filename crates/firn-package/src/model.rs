use firn_kernel::{FirnError, PackageHash, Receipt, Result, SegmentId};
use serde::{Deserialize, Serialize};

pub const MANIFEST_VERSION: u16 = 1;
pub const MANIFEST_FILE: &str = "manifest.json";
pub const TRACE_FILE: &str = "trace.jsonl";
pub const RECEIPTS_FILE: &str = "destination/receipts.json";
pub const REQUIRED_DIRECTORIES: &[&str] = &[
    "plan",
    "schema",
    "data",
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
            other => Err(FirnError::data(format!("unknown package status {other:?}"))),
        }
    }

    pub fn is_replayable(&self) -> bool {
        self.rank() >= Self::Packaged.rank() && self != &Self::Archived
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
    type Error = FirnError;

    fn try_from(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerificationReport {
    pub package_hash: String,
    pub checked_files: Vec<FileEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TombstoneReport {
    pub package_hash: String,
    pub removed_files: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplayView {
    pub package_hash: PackageHash,
    pub status: PackageStatus,
    pub segments: Vec<SegmentEntry>,
    pub receipts: Vec<Receipt>,
}
