use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourcePosition {
    Cursor(CursorPosition),
    Log(LogPosition),
    FileManifest(FileManifest),
    PageToken(PageToken),
    Composite(CompositePosition),
    ForeignState(ForeignState),
}

impl SourcePosition {
    pub fn version(&self) -> u16 {
        match self {
            Self::Cursor(position) => position.version,
            Self::Log(position) => position.version,
            Self::FileManifest(position) => position.version,
            Self::PageToken(position) => position.version,
            Self::Composite(position) => position.version,
            Self::ForeignState(position) => position.version,
        }
    }

    /// Whether this position describes an indivisible source unit rather than a row boundary.
    /// Such positions remain exact when one decoded batch is sliced into canonical segments.
    pub fn is_batch_slice_invariant(&self) -> bool {
        matches!(self, Self::FileManifest(_))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorPosition {
    pub version: u16,
    pub field: String,
    pub value: CursorValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CursorValue {
    String(String),
    I64(i64),
    U64(u64),
    DecimalString(String),
    TimestampMicros {
        micros: i64,
        timezone: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogPosition {
    pub version: u16,
    pub log: String,
    pub offset: i64,
    pub sequence: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileManifest {
    pub version: u16,
    pub files: Vec<FilePosition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilePosition {
    pub path: String,
    pub size_bytes: u64,
    /// Transport- or filesystem-provided generation observed without reading payload bytes.
    /// This remains distinct from a cryptographic content hash.
    pub source_generation: Option<String>,
    pub etag: Option<String>,
    pub object_version: Option<String>,
    pub sha256: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageToken {
    pub version: u16,
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositePosition {
    pub version: u16,
    pub positions: BTreeMap<String, SourcePosition>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignState {
    pub version: u16,
    pub protocol: String,
    pub opaque_blob: Vec<u8>,
    pub blob_sha256: String,
}
