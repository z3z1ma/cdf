use arrow_schema::Schema;
use cdf_kernel::{
    CdfError, CommitBatch, DestinationCommitRequest, Receipt, Result, SegmentAck, SegmentId,
    StateSegment,
};
use cdf_package::SegmentEntry;
use serde::{Deserialize, Serialize};

use crate::{DestinationIngressMode, DestinationWriterModel, ExecutionHostCapabilities};

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
    RollbackFullRedrive,
    Forbidden,
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
        self.rows.validate("row")?;
        self.bytes.validate("byte")
    }
}

pub struct BulkPathPreparationInput<'a> {
    pub output_schema: &'a Schema,
    pub commit: &'a DestinationCommitRequest,
    pub segments: &'a [SegmentEntry],
    pub execution: &'a ExecutionHostCapabilities,
}

#[derive(Clone, Debug, PartialEq, Eq)]
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BulkBatchAck {
    pub segment_id: SegmentId,
    pub batch_ordinal: u32,
    pub row_count: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
}

pub trait BulkWriterAttempt {
    fn apply_migrations(&mut self) -> Result<()>;
    fn write_batch(&mut self, batch: CommitBatch) -> Result<BulkBatchAck>;
    fn finish_segment(&mut self, state: &StateSegment) -> Result<SegmentAck>;
    fn finalize(self: Box<Self>) -> Result<Receipt>;
    fn abort(self: Box<Self>) -> Result<()>;
}
