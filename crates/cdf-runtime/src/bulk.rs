use arrow_schema::Schema;
use cdf_kernel::{CdfError, DestinationCommitRequest, Result};
use serde::{Deserialize, Serialize};

use crate::{
    DestinationIngressMode, DestinationRuntimeCapabilities, DestinationWriterModel,
    ExecutionHostCapabilities,
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
    pub max_useful_writers: u16,
    pub blocking_lane: Option<String>,
    pub native_internal_parallelism: u16,
    pub external_staging: bool,
    pub fallback: BulkFallbackMode,
    pub schema_preflight_version: String,
    pub measured_evidence_version: Option<String>,
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
        if self.version == 0
            || self.max_useful_writers == 0
            || self.native_internal_parallelism == 0
        {
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
    pub rows_per_batch: u64,
    pub bytes_per_batch: u64,
    pub writers: u16,
}

impl PreparedBulkPath {
    pub fn validate(&self) -> Result<()> {
        self.descriptor.validate()?;
        if !(self.descriptor.rows.minimum..=self.descriptor.rows.maximum)
            .contains(&self.rows_per_batch)
            || !(self.descriptor.bytes.minimum..=self.descriptor.bytes.maximum)
                .contains(&self.bytes_per_batch)
            || self.writers == 0
            || self.writers > self.descriptor.max_useful_writers
        {
            return Err(CdfError::contract(
                "prepared bulk settings are outside the descriptor's safe ranges",
            ));
        }
        Ok(())
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
    pub fn from_capabilities(capabilities: &DestinationRuntimeCapabilities) -> Result<Self> {
        capabilities.validate()?;
        let selected_path_id = capabilities
            .bulk_path
            .clone()
            .ok_or_else(|| CdfError::contract("destination has no selected bulk path"))?;
        let eligible = capabilities
            .bulk_paths
            .iter()
            .cloned()
            .map(|descriptor| PreparedBulkPath {
                rows_per_batch: descriptor.rows.preferred,
                bytes_per_batch: descriptor.bytes.preferred,
                writers: 1,
                descriptor,
            })
            .collect();
        let preparation = Self {
            selected_path_id,
            eligible,
            rejected: Vec::new(),
        };
        preparation.validate()?;
        Ok(preparation)
    }

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

    pub fn into_selected(
        self,
        capabilities: &DestinationRuntimeCapabilities,
    ) -> Result<PreparedBulkPath> {
        self.validate()?;
        for path in &self.eligible {
            capabilities.validate_prepared_bulk_path(path)?;
        }
        let selected = self
            .eligible
            .into_iter()
            .find(|path| path.descriptor.path_id == self.selected_path_id)
            .expect("validated selected path");
        Ok(selected)
    }
}
