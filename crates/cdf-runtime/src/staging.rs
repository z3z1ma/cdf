use std::{
    collections::BTreeSet,
    fs::File,
    io::{Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use cdf_kernel::{
    BatchStats, CdfError, CommitPlan, DestinationCommitRequest, DestinationId, PackageHash, PlanId,
    Result, SchemaHash, SegmentId, TargetName, WriteDisposition,
};
use cdf_package_contract::{SegmentEntry, VerifiedPackageAccess};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{StagingLease, StagingMutationGuard};

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
    pub output_arrow_schema_hash: SchemaHash,
    pub merge_keys: Vec<String>,
    pub execution_plan_id: PlanId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagedIngressRequest {
    attempt_id: LoadAttemptId,
    binding: StagingAttemptBinding,
    staging_lease: StagingLease,
    mutation_guard: StagingMutationGuard,
    bulk_path: crate::PreparedBulkPath,
    scheduling: StagingSchedulingContext,
    output_schema: Schema,
    segment_schema: Schema,
}

impl StagedIngressRequest {
    pub fn new(
        attempt_id: LoadAttemptId,
        binding: StagingAttemptBinding,
        staging_lease: StagingLease,
        mutation_guard: StagingMutationGuard,
        bulk_path: crate::PreparedBulkPath,
        scheduling: StagingSchedulingContext,
        output_schema: Schema,
    ) -> Result<Self> {
        let observed = cdf_kernel::canonical_arrow_schema_hash(&output_schema)?;
        if observed != binding.output_arrow_schema_hash {
            return Err(CdfError::contract(format!(
                "staged ingress output schema hash {observed} does not match binding {}",
                binding.output_arrow_schema_hash
            )));
        }
        if staging_lease.identity.destination_id != binding.destination_id
            || staging_lease.identity.target != binding.target
            || staging_lease.identity.attempt_id != attempt_id
        {
            return Err(CdfError::contract(
                "staged ingress request does not match its staging lease identity",
            ));
        }
        let guarded_lease = mutation_guard.assert_current()?;
        if !guarded_lease.same_generation(&staging_lease) {
            return Err(CdfError::contract(
                "staged ingress mutation guard does not bind its staging lease generation",
            ));
        }
        let segment_schema = cdf_package_contract::canonical_segment_schema(&output_schema)?;
        Ok(Self {
            attempt_id,
            binding,
            staging_lease,
            mutation_guard,
            bulk_path,
            scheduling,
            output_schema,
            segment_schema,
        })
    }

    pub fn attempt_id(&self) -> &LoadAttemptId {
        &self.attempt_id
    }

    pub fn binding(&self) -> &StagingAttemptBinding {
        &self.binding
    }

    pub fn staging_lease(&self) -> &StagingLease {
        &self.staging_lease
    }

    pub fn mutation_guard(&self) -> &StagingMutationGuard {
        &self.mutation_guard
    }

    pub fn bulk_path(&self) -> &crate::PreparedBulkPath {
        &self.bulk_path
    }

    pub fn scheduling(&self) -> &StagingSchedulingContext {
        &self.scheduling
    }

    pub fn output_schema(&self) -> &Schema {
        &self.output_schema
    }

    pub fn segment_schema(&self) -> &Schema {
        &self.segment_schema
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StagingCleanupCandidate {
    namespace: String,
    lease: StagingLease,
}

impl StagingCleanupCandidate {
    pub fn new(namespace: impl Into<String>, lease: StagingLease) -> Result<Self> {
        let namespace = namespace.into();
        if namespace.is_empty()
            || namespace.len() > 4_096
            || namespace.chars().any(char::is_control)
        {
            return Err(CdfError::contract(
                "staging cleanup namespace must contain 1..=4096 non-control characters",
            ));
        }
        lease.validate()?;
        Ok(Self { namespace, lease })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn lease(&self) -> &StagingLease {
        &self.lease
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedSegmentIdentity {
    pub segment_id: SegmentId,
    pub sha256: String,
    pub package_row_ord_start: u64,
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
        let canonical_package_digest =
            entry.sha256.len() == 64 && entry.sha256.bytes().all(|byte| byte.is_ascii_hexdigit());
        let algorithm_qualified =
            entry.sha256.starts_with("sha256:") && entry.sha256.len() > "sha256:".len();
        if !canonical_package_digest && !algorithm_qualified {
            return Err(CdfError::data(format!(
                "segment {} has malformed SHA-256 identity",
                entry.segment_id
            )));
        }
        Ok(Self {
            segment_id: entry.segment_id.clone(),
            sha256: entry.sha256.clone(),
            package_row_ord_start: entry.package_row_ord_start,
            row_count: entry.row_count,
            byte_count: entry.byte_count,
            schema_hash,
            ordinal,
        })
    }
}

pub trait DurableSegmentReader: Send {
    fn identity(&self) -> &StagedSegmentIdentity;
    /// Transfers a retained, package-scoped capability for opening the durable local file.
    ///
    /// Destinations that consume canonical bytes directly open it only while a worker is actively
    /// scanning the segment. This keeps descriptor ownership bounded by admitted concurrency.
    /// `DurableLocalFileAccess` verifies the exact manifest digest on the newly opened handle
    /// before exposing it, so neither the pathname spelling nor file length becomes authority.
    fn take_durable_local_file_access(&mut self) -> Result<Option<DurableLocalFileAccess>> {
        Ok(None)
    }
    fn next_batch(&mut self) -> Result<Option<RecordBatch>>;
}

#[derive(Clone)]
pub struct DurableLocalFileAccess {
    path: PathBuf,
    expected_byte_count: u64,
    expected_sha256: String,
    opener: Arc<dyn Fn() -> Result<File> + Send + Sync>,
}

impl std::fmt::Debug for DurableLocalFileAccess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableLocalFileAccess")
            .field("path", &self.path)
            .field("expected_byte_count", &self.expected_byte_count)
            .field("expected_sha256", &self.expected_sha256)
            .finish_non_exhaustive()
    }
}

impl DurableLocalFileAccess {
    pub fn new<F>(
        path: impl Into<PathBuf>,
        expected_byte_count: u64,
        expected_sha256: impl Into<String>,
        opener: F,
    ) -> Self
    where
        F: Fn() -> Result<File> + Send + Sync + 'static,
    {
        Self {
            path: path.into(),
            expected_byte_count,
            expected_sha256: expected_sha256.into(),
            opener: Arc::new(opener),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub const fn expected_byte_count(&self) -> u64 {
        self.expected_byte_count
    }

    pub fn expected_sha256(&self) -> &str {
        &self.expected_sha256
    }

    pub fn open(&self) -> Result<DurableLocalFile> {
        let mut file = (self.opener)()?;
        let metadata = file.metadata().map_err(|error| {
            CdfError::data(format!(
                "inspect durable staged segment at {}: {error}",
                self.path.display()
            ))
        })?;
        if !metadata.is_file() || metadata.len() != self.expected_byte_count {
            return Err(CdfError::data(format!(
                "durable staged segment at {} must be a file of exactly {} bytes, observed {} bytes",
                self.path.display(),
                self.expected_byte_count,
                metadata.len()
            )));
        }
        let mut hasher = Sha256::new();
        let observed_byte_count = std::io::copy(&mut file, &mut hasher).map_err(|error| {
            CdfError::data(format!(
                "hash durable staged segment at {}: {error}",
                self.path.display()
            ))
        })?;
        let observed_sha256 = hex::encode(hasher.finalize());
        let expected_sha256 = self
            .expected_sha256
            .strip_prefix("sha256:")
            .unwrap_or(&self.expected_sha256);
        if observed_byte_count != self.expected_byte_count
            || !observed_sha256.eq_ignore_ascii_case(expected_sha256)
        {
            return Err(CdfError::data(format!(
                "durable staged segment at {} changed after publication: expected {} bytes with sha256 {}, observed {} bytes with sha256 {}",
                self.path.display(),
                self.expected_byte_count,
                self.expected_sha256,
                observed_byte_count,
                observed_sha256
            )));
        }
        file.seek(SeekFrom::Start(0)).map_err(|error| {
            CdfError::data(format!(
                "rewind durable staged segment at {}: {error}",
                self.path.display()
            ))
        })?;
        Ok(DurableLocalFile::new(self.path.clone(), file))
    }
}

/// An already-opened local segment object plus its diagnostic pathname spelling.
///
/// The `File` handle, not `path`, is the access authority. Keeping the spelling only for
/// diagnostics prevents a destination from reopening a different object after verification.
#[derive(Debug)]
pub struct DurableLocalFile {
    path: PathBuf,
    file: File,
}

impl DurableLocalFile {
    fn new(path: impl Into<PathBuf>, file: File) -> Self {
        Self {
            path: path.into(),
            file,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn into_parts(self) -> (PathBuf, File) {
        (self.path, self.file)
    }
}

pub struct StagedSegmentRequest {
    pub identity: StagedSegmentIdentity,
    reader: Box<dyn DurableSegmentReader>,
    durable_local_file_access: Option<DurableLocalFileAccess>,
}

/// A bounded, acknowledgement-bearing stream of durable segments.
///
/// The destination drives the stream for one native ingress lifetime. It may retain multiple
/// segment readers only within the request's declared scheduling bounds and must acknowledge each
/// exact identity after consuming it successfully or transferring it to equally authoritative
/// destination accounting. Acknowledgements may complete out of order; final binding remains in
/// canonical ordinal order.
pub trait StagedSegmentStream {
    fn next_segment(&mut self) -> Result<Option<StagedSegmentRequest>>;
    fn acknowledge(&mut self, acknowledgement: StagedSegmentAck) -> Result<()>;
}

impl StagedSegmentRequest {
    pub fn new(
        identity: StagedSegmentIdentity,
        mut reader: Box<dyn DurableSegmentReader>,
    ) -> Result<Self> {
        if reader.identity() != &identity {
            return Err(CdfError::contract(
                "staged segment request identity does not match its durable reader",
            ));
        }
        let durable_local_file_access = reader.take_durable_local_file_access()?;
        if let Some(access) = durable_local_file_access.as_ref()
            && (access.expected_byte_count() != identity.byte_count
                || access.expected_sha256() != identity.sha256)
        {
            return Err(CdfError::data(format!(
                "durable staged segment {} at {} declares {} bytes with sha256 {} but its identity requires {} bytes with sha256 {}",
                identity.segment_id,
                access.path().display(),
                access.expected_byte_count(),
                access.expected_sha256(),
                identity.byte_count,
                identity.sha256
            )));
        }
        Ok(Self {
            identity,
            reader,
            durable_local_file_access,
        })
    }

    pub fn take_durable_local_file_access(&mut self) -> Option<DurableLocalFileAccess> {
        self.durable_local_file_access.take()
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
    pub(crate) execution_plan_id: PlanId,
    pub(crate) commit: DestinationCommitRequest,
    pub(crate) schema_hash: SchemaHash,
    pub(crate) output_arrow_schema_hash: SchemaHash,
    pub(crate) merge_keys: Vec<String>,
    pub(crate) plan: CommitPlan,
    pub(crate) ordered_segments: Vec<StagedSegmentIdentity>,
    pub(crate) package_statistics: Option<BatchStats>,
}

impl VerifiedFinalBinding {
    pub fn attempt_id(&self) -> &LoadAttemptId {
        &self.attempt_id
    }

    pub fn execution_plan_id(&self) -> &PlanId {
        &self.execution_plan_id
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

    pub fn output_arrow_schema_hash(&self) -> &SchemaHash {
        &self.output_arrow_schema_hash
    }

    pub fn merge_keys(&self) -> &[String] {
        &self.merge_keys
    }

    pub fn ordered_segments(&self) -> &[StagedSegmentIdentity] {
        &self.ordered_segments
    }

    pub fn package_statistics(&self) -> Option<&BatchStats> {
        self.package_statistics.as_ref()
    }

    pub fn from_verified_package(
        attempt_id: LoadAttemptId,
        package: &dyn VerifiedPackageAccess,
        plan: CommitPlan,
    ) -> Result<Self> {
        let execution_plan_id = package.recorded_scan_plan()?.plan_id;
        Self::from_verified_package_with_execution_authority(
            attempt_id,
            execution_plan_id,
            package,
            plan,
        )
    }

    pub fn from_verified_package_with_execution_authority(
        attempt_id: LoadAttemptId,
        execution_plan_id: PlanId,
        package: &dyn VerifiedPackageAccess,
        plan: CommitPlan,
    ) -> Result<Self> {
        let recorded_execution_plan_id = package.recorded_scan_plan()?.plan_id;
        if execution_plan_id != recorded_execution_plan_id {
            return Err(CdfError::contract(format!(
                "staged execution plan {execution_plan_id} does not match recorded package execution plan {recorded_execution_plan_id}",
            )));
        }
        let inputs = package.replay_inputs()?;
        if plan.target != inputs.destination_commit.target
            || plan.disposition != inputs.destination_commit.disposition
        {
            return Err(CdfError::contract(
                "final package binding target/disposition does not match its commit plan",
            ));
        }
        let output_schema = package.runtime_arrow_schema()?;
        let package_statistics = package.verified_package_statistics()?;
        let output_arrow_schema_hash =
            cdf_kernel::canonical_arrow_schema_hash(output_schema.as_ref())?;
        let schema_hash = inputs.schema_hash.clone();
        let mut seen = BTreeSet::new();
        let mut ordered_segments = Vec::new();
        package.for_each_identity_segment(&mut |entry| {
            let ordinal = ordered_segments.len();
            let segment = (|| {
                if !seen.insert(entry.segment_id.clone()) {
                    return Err(CdfError::data(format!(
                        "final package repeats segment {}",
                        entry.segment_id
                    )));
                }
                let ordinal = u32::try_from(ordinal)
                    .map_err(|_| CdfError::data("final package has too many segments"))?;
                StagedSegmentIdentity::from_manifest_entry(&entry, schema_hash.clone(), ordinal)
            })()?;
            ordered_segments.push(segment);
            Ok(())
        })?;
        let package_hash = PackageHash::new(package.package_hash())?;
        let commit = inputs.destination_commit;
        if commit.package_hash != package_hash
            || commit.idempotency_token.as_str() != package_hash.as_str()
        {
            return Err(CdfError::contract(
                "verified package replay inputs do not bind the final package token",
            ));
        }
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
            execution_plan_id,
            commit,
            schema_hash,
            output_arrow_schema_hash,
            merge_keys: inputs.merge_keys,
            plan,
            ordered_segments,
            package_statistics,
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
    fn stage_stream(&mut self, stream: &mut dyn StagedSegmentStream) -> Result<()>;
    fn snapshot(&self) -> Result<StagingSnapshot>;
    fn bind_final(
        self: Box<Self>,
        binding: VerifiedFinalBinding,
    ) -> Result<crate::DestinationCommitOutcome>;
    fn abort(self: Box<Self>) -> Result<()>;
}
