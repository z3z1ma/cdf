use arrow_schema::Schema;
use cdf_kernel::{CdfError, DestinationCommitRequest, Result};
use serde::{Deserialize, Serialize};

use crate::{
    capability_types::{DestinationIngressMode, DestinationWriterModel},
    execution_host::ExecutionHostCapabilities,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BulkOrdering {
    ManifestOrder,
    SegmentIndependent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BulkFallbackMode {
    PreflightOnly,
    Forbidden,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BulkBatchMode {
    DestinationControlled,
    PassThrough,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BulkPathEvidence {
    Measured { version: String },
    Inconclusive { version: String },
    Unmeasured,
}

impl BulkPathEvidence {
    pub const fn status(&self) -> &'static str {
        match self {
            Self::Measured { .. } => "measured",
            Self::Inconclusive { .. } => "inconclusive",
            Self::Unmeasured => "unmeasured",
        }
    }

    pub fn version(&self) -> Option<&str> {
        match self {
            Self::Measured { version } | Self::Inconclusive { version } => Some(version),
            Self::Unmeasured => None,
        }
    }

    fn validate(&self) -> Result<()> {
        let Some(version) = self.version() else {
            return Ok(());
        };
        if version.is_empty()
            || version.len() > 128
            || version.chars().any(|ch| {
                !(ch.is_ascii_lowercase()
                    || ch.is_ascii_digit()
                    || matches!(ch, '-' | '_' | '.' | '@'))
            })
        {
            return Err(CdfError::contract(
                "bulk evidence version must contain 1..=128 lowercase ASCII letters, digits, `-`, `_`, `.`, or `@`",
            ));
        }
        Ok(())
    }
}

impl BulkFallbackMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PreflightOnly => "preflight_only",
            Self::Forbidden => "forbidden",
        }
    }
}

impl std::fmt::Display for BulkFallbackMode {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkSizeRange {
    pub minimum: u64,
    pub preferred: u64,
    pub maximum: u64,
}

impl BulkSizeRange {
    pub fn validate(&self, label: &str) -> Result<()> {
        if self.minimum == 0 || self.minimum > self.preferred || self.preferred > self.maximum {
            return Err(CdfError::contract(format!(
                "bulk {label} range must satisfy 0 < minimum <= preferred <= maximum"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkPathDescriptor {
    pub path_id: String,
    pub version: u16,
    pub ingress_mode: DestinationIngressMode,
    pub writer_model: DestinationWriterModel,
    pub ordering: BulkOrdering,
    pub rows: BulkSizeRange,
    pub bytes: BulkSizeRange,
    pub batch_mode: BulkBatchMode,
    pub maximum_writers: u16,
    pub blocking_lane: Option<String>,
    pub native_internal_parallelism: u16,
    pub external_staging: bool,
    pub fallback: BulkFallbackMode,
    pub schema_preflight_version: String,
    pub evidence: BulkPathEvidence,
}

impl BulkPathDescriptor {
    pub fn validate(&self) -> Result<()> {
        if self.path_id.is_empty()
            || self.path_id.len() > 128
            || self.path_id.chars().any(|ch| {
                !(ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-')
            })
        {
            return Err(CdfError::contract(
                "bulk path id must contain 1..=128 lowercase ASCII letters, digits, `_`, or `-`",
            ));
        }
        if self.version == 0 || self.maximum_writers == 0 || self.native_internal_parallelism == 0 {
            return Err(CdfError::contract(
                "bulk path version, writer count, and native parallelism must be nonzero",
            ));
        }
        if self.schema_preflight_version.is_empty()
            || self.schema_preflight_version.len() > 128
            || self.schema_preflight_version.chars().any(|ch| {
                !(ch.is_ascii_lowercase()
                    || ch.is_ascii_digit()
                    || matches!(ch, '-' | '_' | '.' | '@'))
            })
        {
            return Err(CdfError::contract(
                "bulk schema-preflight version must contain 1..=128 lowercase ASCII letters, digits, `-`, `_`, `.`, or `@`",
            ));
        }
        self.evidence.validate()?;
        self.rows.validate("row")?;
        self.bytes.validate("byte")
    }
}

pub struct BulkPathPreparationInput<'a> {
    pub output_schema: &'a Schema,
    pub commit: Option<&'a DestinationCommitRequest>,
    pub execution: Option<ExecutionHostCapabilities>,
}

impl<'a> BulkPathPreparationInput<'a> {
    pub fn new(output_schema: &'a Schema) -> Self {
        Self {
            output_schema,
            commit: None,
            execution: None,
        }
    }

    pub fn with_commit(mut self, commit: &'a DestinationCommitRequest) -> Self {
        self.commit = Some(commit);
        self
    }

    pub fn with_execution(mut self, execution: ExecutionHostCapabilities) -> Self {
        self.execution = Some(execution);
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PreparedBulkPath {
    pub descriptor: BulkPathDescriptor,
    pub rows_per_batch: Option<u64>,
    pub bytes_per_batch: Option<u64>,
    pub writers: u16,
}

impl PreparedBulkPath {
    pub fn validate(&self) -> Result<()> {
        self.descriptor.validate()?;
        let batching_is_valid = match (
            self.descriptor.batch_mode,
            self.rows_per_batch,
            self.bytes_per_batch,
        ) {
            (BulkBatchMode::DestinationControlled, Some(rows), Some(bytes)) => {
                (self.descriptor.rows.minimum..=self.descriptor.rows.maximum).contains(&rows)
                    && (self.descriptor.bytes.minimum..=self.descriptor.bytes.maximum)
                        .contains(&bytes)
            }
            (BulkBatchMode::PassThrough, None, None) => true,
            _ => false,
        };
        if !batching_is_valid || self.writers == 0 || self.writers > self.descriptor.maximum_writers
        {
            return Err(CdfError::contract(
                "prepared bulk settings are outside the descriptor's safe ranges",
            ));
        }
        Ok(())
    }

    pub fn controlled_batch_sizes(&self) -> Result<(u64, u64)> {
        match (
            self.descriptor.batch_mode,
            self.rows_per_batch,
            self.bytes_per_batch,
        ) {
            (BulkBatchMode::DestinationControlled, Some(rows), Some(bytes)) => Ok((rows, bytes)),
            _ => Err(CdfError::contract(format!(
                "bulk path `{}` does not control destination batch sizing",
                self.descriptor.path_id
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BulkPathRejection {
    pub path_id: String,
    pub field: Option<String>,
    pub reason: String,
    pub fixes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BulkPathPreparation {
    pub selected_path_id: String,
    pub eligible: Vec<PreparedBulkPath>,
    pub rejected: Vec<BulkPathRejection>,
}

impl BulkPathPreparation {
    pub fn validate(&self) -> Result<()> {
        if self.eligible.is_empty() {
            return Err(CdfError::contract(
                "destination bulk preparation produced no eligible path",
            ));
        }
        if self.selected_path_id.is_empty()
            || !self
                .eligible
                .iter()
                .any(|path| path.descriptor.path_id == self.selected_path_id)
        {
            return Err(CdfError::contract(
                "destination bulk preparation must select one eligible path",
            ));
        }
        let mut ids = std::collections::BTreeSet::new();
        for path in &self.eligible {
            path.validate()?;
            if !ids.insert(path.descriptor.path_id.as_str()) {
                return Err(CdfError::contract(
                    "destination bulk preparation contains duplicate eligible path ids",
                ));
            }
        }
        Ok(())
    }
}
