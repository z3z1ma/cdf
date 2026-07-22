use std::sync::Arc;

use arrow_schema::SchemaRef;
use cdf_kernel::{BatchStats, Result, ScanPlan};

use crate::{PackageReplayInputs, QuarantineRecord, SegmentEntry};

/// Read-only access to facts from one package whose identity has already been
/// verified by the package implementation.
///
/// The contract intentionally exposes no path, file handle, codec, or
/// verification mechanism. Neutral orchestration and destination adapters can
/// consume verified facts without depending on package persistence.
pub trait VerifiedPackageAccess: Send + Sync {
    fn package_hash(&self) -> &str;
    fn for_each_identity_segment(
        &self,
        visitor: &mut dyn FnMut(SegmentEntry) -> Result<()>,
    ) -> Result<()>;
    fn recorded_scan_plan(&self) -> Result<ScanPlan>;
    fn replay_inputs(&self) -> Result<PackageReplayInputs>;
    fn runtime_arrow_schema(&self) -> Result<SchemaRef>;
    /// Returns complete package-grain statistics only when a manifest-bound profile artifact was
    /// emitted and fully verified. Absence is conservative: consumers must retain every field.
    fn verified_package_statistics(&self) -> Result<Option<BatchStats>> {
        Ok(None)
    }
    fn for_each_quarantine_record(
        &self,
        visitor: &mut dyn FnMut(QuarantineRecord) -> Result<()>,
    ) -> Result<()>;
}

pub type SharedVerifiedPackageAccess = Arc<dyn VerifiedPackageAccess>;
