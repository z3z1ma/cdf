use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{CdfError, Result};

pub const SOURCE_POSITION_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourcePositionKind {
    Cursor,
    Log,
    FileManifest,
    TableSnapshot,
    PageToken,
    Composite,
    ForeignState,
}

impl SourcePositionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cursor => "cursor",
            Self::Log => "log",
            Self::FileManifest => "file_manifest",
            Self::TableSnapshot => "table_snapshot",
            Self::PageToken => "page_token",
            Self::Composite => "composite",
            Self::ForeignState => "foreign_state",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SourcePosition {
    Cursor(CursorPosition),
    Log(LogPosition),
    FileManifest(FileManifest),
    TableSnapshot(Box<TableSnapshotPosition>),
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
            Self::TableSnapshot(position) => position.version,
            Self::PageToken(position) => position.version,
            Self::Composite(position) => position.version,
            Self::ForeignState(position) => position.version,
        }
    }

    /// Whether this position describes an indivisible source unit rather than a row boundary.
    /// Such positions remain exact when one decoded batch is sliced into canonical segments.
    pub fn is_batch_slice_invariant(&self) -> bool {
        matches!(self, Self::FileManifest(_) | Self::TableSnapshot(_))
    }

    pub const fn kind(&self) -> SourcePositionKind {
        match self {
            Self::Cursor(_) => SourcePositionKind::Cursor,
            Self::Log(_) => SourcePositionKind::Log,
            Self::FileManifest(_) => SourcePositionKind::FileManifest,
            Self::TableSnapshot(_) => SourcePositionKind::TableSnapshot,
            Self::PageToken(_) => SourcePositionKind::PageToken,
            Self::Composite(_) => SourcePositionKind::Composite,
            Self::ForeignState(_) => SourcePositionKind::ForeignState,
        }
    }

    /// Validates source-position structure before it is frozen into plan or checkpoint authority.
    pub fn validate(&self) -> Result<()> {
        if self.version() != SOURCE_POSITION_VERSION {
            return Err(CdfError::contract(format!(
                "source position version {} is unsupported; expected {}",
                self.version(),
                SOURCE_POSITION_VERSION
            )));
        }
        match self {
            Self::Cursor(position) => {
                require_text("cursor field", &position.field)?;
                if let CursorValue::DecimalString(value) = &position.value {
                    validate_decimal_string(value)?;
                }
                if let CursorValue::TimestampMicros {
                    timezone: Some(timezone),
                    ..
                } = &position.value
                {
                    require_text("cursor timezone", timezone)?;
                }
                Ok(())
            }
            Self::Log(position) => {
                require_text("log position name", &position.log)?;
                if let Some(sequence) = &position.sequence {
                    require_text("log position sequence", sequence)?;
                }
                Ok(())
            }
            Self::FileManifest(manifest) => {
                if manifest.files.is_empty() {
                    return Err(CdfError::contract(
                        "file-manifest source frontier requires at least one file",
                    ));
                }
                let mut prior = None::<&str>;
                for file in &manifest.files {
                    require_text("file-manifest path", &file.path)?;
                    if prior.is_some_and(|prior| prior >= file.path.as_str()) {
                        return Err(CdfError::contract(
                            "file-manifest source frontier paths must be unique and canonically sorted",
                        ));
                    }
                    if file.source_generation.is_none()
                        && file.etag.is_none()
                        && file.object_version.is_none()
                        && file.sha256.is_none()
                    {
                        return Err(CdfError::contract(format!(
                            "file-manifest source frontier `{}` requires generation, ETag, object version, or SHA-256 identity",
                            file.path
                        )));
                    }
                    for (label, value) in [
                        ("source generation", file.source_generation.as_deref()),
                        ("ETag", file.etag.as_deref()),
                        ("object version", file.object_version.as_deref()),
                    ] {
                        if let Some(value) = value {
                            require_text(label, value)?;
                        }
                    }
                    if let Some(hash) = &file.sha256 {
                        validate_sha256("file-manifest content", hash)?;
                    }
                    prior = Some(file.path.as_str());
                }
                Ok(())
            }
            Self::TableSnapshot(position) => position.validate(),
            Self::PageToken(position) => require_text("page token", &position.token),
            Self::Composite(position) => {
                if position.positions.is_empty() {
                    return Err(CdfError::contract(
                        "composite source frontier requires at least one named position",
                    ));
                }
                for (name, position) in &position.positions {
                    require_text("composite position name", name)?;
                    position.validate()?;
                }
                Ok(())
            }
            Self::ForeignState(position) => {
                require_text("foreign-state protocol", &position.protocol)?;
                validate_sha256("foreign-state blob", &position.blob_sha256)?;
                let observed = format!(
                    "sha256:{}",
                    hex::encode(Sha256::digest(&position.opaque_blob))
                );
                if observed != position.blob_sha256 {
                    return Err(CdfError::contract(
                        "foreign-state blob SHA-256 does not match its opaque payload",
                    ));
                }
                Ok(())
            }
        }
    }
}

fn require_text(label: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.chars().any(char::is_control) {
        return Err(CdfError::contract(format!(
            "{label} must be nonempty and control-free"
        )));
    }
    Ok(())
}

fn validate_sha256(label: &str, value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(CdfError::contract(format!(
            "{label} hash must use sha256:<64 lowercase hex>"
        )));
    };
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(CdfError::contract(format!(
            "{label} hash must use sha256:<64 lowercase hex>"
        )));
    }
    Ok(())
}

fn validate_decimal_string(value: &str) -> Result<()> {
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let mut parts = unsigned.split('.');
    let whole = parts.next().unwrap_or_default();
    let fractional = parts.next();
    let canonical = !whole.is_empty()
        && whole.bytes().all(|byte| byte.is_ascii_digit())
        && fractional
            .is_none_or(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()))
        && parts.next().is_none();
    if !canonical {
        return Err(CdfError::contract(
            "cursor decimal string must be canonical digits with an optional leading minus and fractional part",
        ));
    }
    Ok(())
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

/// One immutable table snapshot selected before extraction begins.
///
/// This is source-neutral checkpoint authority. File identities remain in scan-task artifacts;
/// every partition of a table run attests this same value and aggregation commits it only after
/// the complete selected task set reaches a terminal outcome.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSnapshotPosition {
    pub version: u16,
    pub protocol: String,
    pub catalog: String,
    pub namespace: Vec<String>,
    pub table: String,
    pub selector: TableSnapshotSelector,
    pub snapshot_id: i64,
    pub sequence_number: i64,
    pub parent_snapshot_id: Option<i64>,
    pub metadata_location: String,
    pub metadata_generation: String,
}

impl TableSnapshotPosition {
    pub fn validate(&self) -> Result<()> {
        if self.version != SOURCE_POSITION_VERSION {
            return Err(CdfError::contract(format!(
                "table-snapshot source position version {} is unsupported; expected {}",
                self.version, SOURCE_POSITION_VERSION
            )));
        }
        validate_protocol(&self.protocol)?;
        require_text("table-snapshot catalog identity", &self.catalog)?;
        if self.namespace.is_empty() {
            return Err(CdfError::contract(
                "table-snapshot namespace requires at least one component",
            ));
        }
        for component in &self.namespace {
            require_text("table-snapshot namespace component", component)?;
        }
        require_text("table-snapshot table identity", &self.table)?;
        require_text("table-snapshot metadata location", &self.metadata_location)?;
        require_text(
            "table-snapshot metadata generation",
            &self.metadata_generation,
        )?;
        if self.snapshot_id <= 0 {
            return Err(CdfError::contract(
                "table-snapshot snapshot id must be positive",
            ));
        }
        if self.sequence_number < 0 {
            return Err(CdfError::contract(
                "table-snapshot sequence number must be nonnegative",
            ));
        }
        if let Some(parent) = self.parent_snapshot_id
            && (parent <= 0 || parent == self.snapshot_id)
        {
            return Err(CdfError::contract(
                "table-snapshot parent id must be positive and differ from the selected snapshot",
            ));
        }
        match &self.selector {
            TableSnapshotSelector::Current => {}
            TableSnapshotSelector::Branch { name } => {
                require_text("table-snapshot branch", name)?;
            }
            TableSnapshotSelector::Tag { name } => {
                require_text("table-snapshot tag", name)?;
            }
            TableSnapshotSelector::Snapshot { snapshot_id } => {
                if *snapshot_id != self.snapshot_id {
                    return Err(CdfError::contract(
                        "table-snapshot selector id must equal the selected snapshot id",
                    ));
                }
            }
            TableSnapshotSelector::Timestamp { timestamp_ms } => {
                if *timestamp_ms < 0 {
                    return Err(CdfError::contract(
                        "table-snapshot selector timestamp must be nonnegative",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TableSnapshotSelector {
    Current,
    Branch { name: String },
    Tag { name: String },
    Snapshot { snapshot_id: i64 },
    Timestamp { timestamp_ms: i64 },
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

fn validate_protocol(value: &str) -> Result<()> {
    require_text("table-snapshot protocol", value)?;
    let mut bytes = value.bytes();
    if !bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        || !bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
    {
        return Err(CdfError::contract(
            "table-snapshot protocol must be a canonical lowercase ASCII identifier",
        ));
    }
    Ok(())
}
