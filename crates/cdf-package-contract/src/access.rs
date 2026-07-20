use std::sync::Arc;

use arrow_schema::SchemaRef;
use cdf_kernel::{Result, ScanPlan};

use crate::{PackageReplayInputs, QuarantineRecord, SegmentEntry};

/// Read-only access to facts from one package whose identity has already been
/// verified by the package implementation.
///
/// The contract intentionally exposes no path, file handle, codec, or
/// verification mechanism. Neutral orchestration and destination adapters can
/// consume verified facts without depending on package persistence.
pub trait VerifiedPackageAccess: Send + Sync {
    fn package_hash(&self) -> &str;
    fn identity_segments(&self) -> &[SegmentEntry];
    fn recorded_scan_plan(&self) -> Result<ScanPlan>;
    fn replay_inputs(&self) -> Result<PackageReplayInputs>;
    fn runtime_arrow_schema(&self) -> Result<SchemaRef>;
    fn for_each_quarantine_record(
        &self,
        visitor: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
    ) -> Result<()>;
}

pub type SharedVerifiedPackageAccess = Arc<dyn VerifiedPackageAccess>;
