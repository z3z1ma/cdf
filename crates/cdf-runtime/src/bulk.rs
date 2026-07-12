use arrow_schema::Schema;
use cdf_kernel::{
    CdfError, CommitBatch, DestinationCommitRequest, Receipt, Result, SegmentAck, SegmentId,
    StateSegment,
};
use cdf_package::SegmentEntry;
use serde::{Deserialize, Serialize};

use crate::{
    DestinationIngressMode, DestinationWriterModel, ExecutionHostCapabilities, LoadAttemptId,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkAbortProof {
    pub attempt_id: LoadAttemptId,
    pub path_id: String,
    pub zero_target_visibility: bool,
    pub external_staging_cleaned: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkAttemptEvidence {
    pub attempt_id: LoadAttemptId,
    pub path_id: String,
    pub path_version: u16,
    pub rows_per_batch: u64,
    pub bytes_per_batch: u64,
    pub writers: u16,
    pub fallback_reason: Option<String>,
    pub aborted_before_fallback: bool,
}

pub struct BulkAttemptCoordinator {
    paths: std::vec::IntoIter<PreparedBulkPath>,
    current: Option<(LoadAttemptId, PreparedBulkPath)>,
    evidence: Vec<BulkAttemptEvidence>,
}

impl BulkAttemptCoordinator {
    pub fn new(preparation: BulkPathPreparation) -> Result<Self> {
        preparation.validate()?;
        Ok(Self {
            paths: preparation.eligible.into_iter(),
            current: None,
            evidence: Vec::new(),
        })
    }

    pub fn start(&mut self, attempt_id: LoadAttemptId) -> Result<&PreparedBulkPath> {
        if self.current.is_some() {
            return Err(CdfError::contract(
                "bulk attempt coordinator already has an active attempt",
            ));
        }
        let path = self
            .paths
            .next()
            .ok_or_else(|| CdfError::destination("bulk path ladder is exhausted"))?;
        self.evidence.push(BulkAttemptEvidence {
            attempt_id: attempt_id.clone(),
            path_id: path.descriptor.path_id.clone(),
            path_version: path.descriptor.version,
            rows_per_batch: path.rows_per_batch,
            bytes_per_batch: path.bytes_per_batch,
            writers: path.writers,
            fallback_reason: None,
            aborted_before_fallback: false,
        });
        self.current = Some((attempt_id, path));
        Ok(&self.current.as_ref().expect("set above").1)
    }

    pub fn fallback(
        &mut self,
        proof: BulkAbortProof,
        reason: impl Into<String>,
        next_attempt_id: LoadAttemptId,
    ) -> Result<&PreparedBulkPath> {
        let (attempt_id, path) = self
            .current
            .as_ref()
            .ok_or_else(|| CdfError::contract("bulk fallback requires an active attempt"))?;
        if proof.attempt_id != *attempt_id || proof.path_id != path.descriptor.path_id {
            return Err(CdfError::contract(
                "bulk abort proof does not match the active attempt and path",
            ));
        }
        if next_attempt_id == *attempt_id {
            return Err(CdfError::contract(
                "bulk fallback must redrive under a new load attempt id",
            ));
        }
        if path.descriptor.fallback != BulkFallbackMode::RollbackFullRedrive
            || !proof.zero_target_visibility
            || (path.descriptor.external_staging && !proof.external_staging_cleaned)
        {
            return Err(CdfError::destination(
                "bulk runtime fallback requires a rollback/full-redrive path, zero target visibility, and cleaned external staging",
            ));
        }
        self.current.take();
        let evidence = self
            .evidence
            .last_mut()
            .ok_or_else(|| CdfError::internal("bulk attempt evidence is missing"))?;
        evidence.fallback_reason = Some(reason.into());
        evidence.aborted_before_fallback = true;
        self.start(next_attempt_id)
    }

    pub fn complete(&mut self) -> Result<()> {
        self.current
            .take()
            .ok_or_else(|| CdfError::contract("bulk completion requires an active attempt"))?;
        Ok(())
    }

    pub fn evidence(&self) -> &[BulkAttemptEvidence] {
        &self.evidence
    }
}

pub trait BulkWriterAttempt {
    fn apply_migrations(&mut self) -> Result<()>;
    fn write_batch(&mut self, batch: CommitBatch) -> Result<BulkBatchAck>;
    fn finish_segment(&mut self, state: &StateSegment) -> Result<SegmentAck>;
    fn finalize(self: Box<Self>) -> Result<Receipt>;
    fn abort(self: Box<Self>) -> Result<BulkAbortProof>;
}
