use std::path::PathBuf;

use cdf_object_access::FileTransportResource;
use cdf_runtime::{ByteTransformId, FormatDetection, GenerationStrength};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResolvedFileMatch {
    pub(super) open: ResolvedFileOpen,
    pub(super) path_text: String,
    pub(super) size_bytes: u64,
    pub(super) source_generation: Option<String>,
    pub(super) identity_strength: GenerationStrength,
    pub(super) sha256: Option<String>,
    pub(super) etag: Option<String>,
    pub(super) version: Option<String>,
    pub(super) modified_ms: Option<String>,
    pub(super) exact_ranges: bool,
    pub(super) bytes_loaded: Option<u64>,
    pub(super) compression: CompressionEvidence,
    pub(super) format: FormatEvidence,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FileInventoryRecord {
    pub(super) path_text: String,
    pub(super) size_bytes: u64,
    pub(super) source_generation: Option<String>,
    pub(super) identity_strength: GenerationStrength,
    pub(super) sha256: Option<String>,
    pub(super) etag: Option<String>,
    pub(super) version: Option<String>,
    pub(super) modified_ms: Option<String>,
    pub(super) bytes_loaded: Option<u64>,
    pub(super) compression: CompressionEvidence,
    pub(super) format: FormatEvidence,
}

impl From<&ResolvedFileMatch> for FileInventoryRecord {
    fn from(file: &ResolvedFileMatch) -> Self {
        Self {
            path_text: file.path_text.clone(),
            size_bytes: file.size_bytes,
            source_generation: file.source_generation.clone(),
            identity_strength: file.identity_strength,
            sha256: file.sha256.clone(),
            etag: file.etag.clone(),
            version: file.version.clone(),
            modified_ms: file.modified_ms.clone(),
            bytes_loaded: file.bytes_loaded,
            compression: file.compression.clone(),
            format: file.format.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ResolvedFileOpen {
    LocalPath(PathBuf),
    Transport(FileTransportResource),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CompressionEvidence {
    pub(super) transform_id: Option<ByteTransformId>,
    pub(super) extension_signal: CompressionSignal,
    pub(super) magic_signal: CompressionSignal,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(super) struct CompressionSignal(pub(super) Option<ByteTransformId>);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FormatEvidence {
    pub(super) format_id: String,
    pub(super) driver_version: String,
    pub(super) extension: Option<String>,
    pub(super) detection: FormatDetection,
}

impl CompressionSignal {
    pub(super) fn as_str(&self) -> &str {
        self.0.as_ref().map_or("none", ByteTransformId::as_str)
    }

    pub(super) fn transform_id(&self) -> Option<&ByteTransformId> {
        self.0.as_ref()
    }
}

impl CompressionEvidence {
    pub(super) fn mode_name(&self) -> &str {
        self.transform_id
            .as_ref()
            .map_or("none", ByteTransformId::as_str)
    }
}
