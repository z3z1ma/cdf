use std::collections::BTreeSet;

use arrow_array::RecordBatch;
use cdf_kernel::{
    CdfError, CommitPlan, DestinationCommitRequest, DestinationId, IdempotencyToken, PackageHash,
    PlanId, Receipt, Result, SchemaHash, SegmentId, TargetName, WriteDisposition,
};
use cdf_package::{PackageReader, SegmentEntry, VerificationReport};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LoadAttemptId(String);

impl LoadAttemptId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 256
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(CdfError::contract(
                "load attempt id must contain 1..=256 ASCII alphanumeric, `-`, or `_` bytes",
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for LoadAttemptId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagingRecoveryMode {
    Resumable,
    RollbackRedrive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagingVisibility {
    IsolatedUntilFinalBinding,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedIngressCapabilities {
    pub recovery: StagingRecoveryMode,
    pub visibility: StagingVisibility,
    pub abort_idempotent: bool,
    pub lifecycle_cleanup: bool,
    pub final_binding_requires_exclusive_writer: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingSchedulingContext {
    pub max_in_flight_segments: u16,
    pub max_in_flight_bytes: u64,
}

impl StagingSchedulingContext {
    pub fn new(max_in_flight_segments: u16, max_in_flight_bytes: u64) -> Result<Self> {
        if max_in_flight_segments == 0 || max_in_flight_bytes == 0 {
            return Err(CdfError::contract(
                "staged ingress scheduling bounds must be nonzero",
            ));
        }
        Ok(Self {
            max_in_flight_segments,
            max_in_flight_bytes,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingAttemptBinding {
    pub destination_id: DestinationId,
    pub target: TargetName,
    pub disposition: WriteDisposition,
    pub schema_hash: SchemaHash,
    pub plan_id: PlanId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedIngressRequest {
    pub attempt_id: LoadAttemptId,
    pub binding: StagingAttemptBinding,
    pub scheduling: StagingSchedulingContext,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedSegmentIdentity {
    pub segment_id: SegmentId,
    pub sha256: String,
    pub row_count: u64,
    pub byte_count: u64,
    pub schema_hash: SchemaHash,
    pub ordinal: u32,
}

impl StagedSegmentIdentity {
    pub fn from_manifest_entry(
        entry: &SegmentEntry,
        schema_hash: SchemaHash,
        ordinal: u32,
    ) -> Result<Self> {
        if !entry.sha256.starts_with("sha256:") || entry.sha256.len() <= "sha256:".len() {
            return Err(CdfError::data(format!(
                "segment {} has malformed SHA-256 identity",
                entry.segment_id
            )));
        }
        Ok(Self {
            segment_id: entry.segment_id.clone(),
            sha256: entry.sha256.clone(),
            row_count: entry.row_count,
            byte_count: entry.byte_count,
            schema_hash,
            ordinal,
        })
    }
}

pub trait DurableSegmentReader: Send {
    fn identity(&self) -> &StagedSegmentIdentity;
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

pub struct StagedSegmentRequest {
    pub identity: StagedSegmentIdentity,
    reader: Box<dyn DurableSegmentReader>,
}

impl StagedSegmentRequest {
    pub fn new(
        identity: StagedSegmentIdentity,
        reader: Box<dyn DurableSegmentReader>,
    ) -> Result<Self> {
        if reader.identity() != &identity {
            return Err(CdfError::contract(
                "staged segment request identity does not match its durable reader",
            ));
        }
        Ok(Self { identity, reader })
    }

    pub fn reader_mut(&mut self) -> &mut dyn DurableSegmentReader {
        self.reader.as_mut()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedSegmentAck {
    pub attempt_id: LoadAttemptId,
    pub identity: StagedSegmentIdentity,
    pub external_durable: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagingSnapshot {
    pub attempt_id: LoadAttemptId,
    pub binding: StagingAttemptBinding,
    pub recovery: StagingRecoveryMode,
    pub accepted_segments: Vec<StagedSegmentIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedFinalBinding {
    pub(crate) attempt_id: LoadAttemptId,
    pub(crate) commit: DestinationCommitRequest,
    pub(crate) schema_hash: SchemaHash,
    pub(crate) plan: CommitPlan,
    pub(crate) ordered_segments: Vec<StagedSegmentIdentity>,
}

impl VerifiedFinalBinding {
    pub fn attempt_id(&self) -> &LoadAttemptId {
        &self.attempt_id
    }

    pub fn commit(&self) -> &DestinationCommitRequest {
        &self.commit
    }

    pub fn schema_hash(&self) -> &SchemaHash {
        &self.schema_hash
    }

    pub fn plan(&self) -> &CommitPlan {
        &self.plan
    }

    pub fn ordered_segments(&self) -> &[StagedSegmentIdentity] {
        &self.ordered_segments
    }

    pub fn from_verified_package(
        attempt_id: LoadAttemptId,
        reader: &PackageReader,
        verification: &VerificationReport,
        target: TargetName,
        disposition: WriteDisposition,
        schema_hash: SchemaHash,
        plan: CommitPlan,
    ) -> Result<Self> {
        let view = reader.replay_view()?;
        if verification.package_hash != reader.manifest().package_hash
            || verification.package_hash != view.package_hash.as_str()
        {
            return Err(CdfError::data(
                "verified package report does not match the final package manifest hash",
            ));
        }
        if plan.target != target || plan.disposition != disposition {
            return Err(CdfError::contract(
                "final package binding target/disposition does not match its commit plan",
            ));
        }
        let mut seen = BTreeSet::new();
        let ordered_segments = view
            .segments
            .iter()
            .enumerate()
            .map(|(ordinal, entry)| {
                if !seen.insert(entry.segment_id.clone()) {
                    return Err(CdfError::data(format!(
                        "final package repeats segment {}",
                        entry.segment_id
                    )));
                }
                let ordinal = u32::try_from(ordinal)
                    .map_err(|_| CdfError::data("final package has too many segments"))?;
                StagedSegmentIdentity::from_manifest_entry(entry, schema_hash.clone(), ordinal)
            })
            .collect::<Result<Vec<_>>>()?;
        let package_hash = PackageHash::new(verification.package_hash.clone())?;
        let commit = DestinationCommitRequest {
            package_hash: package_hash.clone(),
            target,
            disposition,
            segments: reader.replay_inputs()?.state_delta.segments,
            idempotency_token: IdempotencyToken::new(package_hash.as_str())?,
        };
        let commit_ids = commit
            .segments
            .iter()
            .map(|segment| &segment.segment_id)
            .collect::<Vec<_>>();
        let manifest_ids = ordered_segments
            .iter()
            .map(|segment| &segment.segment_id)
            .collect::<Vec<_>>();
        if commit_ids != manifest_ids {
            return Err(CdfError::data(
                "final package state delta segment order does not match manifest identity",
            ));
        }
        Ok(Self {
            attempt_id,
            commit,
            schema_hash,
            plan,
            ordered_segments,
        })
    }

    pub fn validate_staged_identities(&self, staged: &[StagedSegmentIdentity]) -> Result<()> {
        if staged != self.ordered_segments {
            return Err(CdfError::destination(
                "staged segment identities do not exactly match the verified final package",
            ));
        }
        Ok(())
    }
}

pub trait StagedIngressSession: Send {
    fn stage_segment(&mut self, segment: StagedSegmentRequest) -> Result<StagedSegmentAck>;
    fn snapshot(&self) -> Result<StagingSnapshot>;
    fn bind_final(self: Box<Self>, binding: VerifiedFinalBinding) -> Result<Receipt>;
    fn abort(self: Box<Self>) -> Result<()>;
}
