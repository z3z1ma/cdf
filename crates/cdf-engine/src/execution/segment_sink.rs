//! Canonical segment encoding and package sink ownership.

use arrow_array::RecordBatch;
use cdf_kernel::Result;
use cdf_memory::MemoryLease;
use cdf_package_contract::SegmentEntry;

pub type DurableSegmentHook<'a> =
    dyn FnMut(&SegmentEntry, DurableSegmentPayload) -> Result<()> + 'a;

/// An owned, accounted handoff from durable package publication to staged ingress.
///
/// The record batches and their existing memory leases move together so a destination queue does
/// not reserve the same Arrow allocations a second time. Dropping the payload releases ownership.
pub struct DurableSegmentPayload {
    pub(super) durable_file: cdf_package::DurableSegmentFile,
    pub(super) batches: Vec<RecordBatch>,
    pub(super) memory_leases: Vec<MemoryLease>,
}

impl DurableSegmentPayload {
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn into_parts(
        self,
    ) -> (
        cdf_package::DurableSegmentFile,
        Vec<RecordBatch>,
        Vec<MemoryLease>,
    ) {
        (self.durable_file, self.batches, self.memory_leases)
    }
}

pub(super) struct DurableSegmentObserver<'a> {
    pub(super) hook: Option<&'a mut DurableSegmentHook<'a>>,
}

impl DurableSegmentObserver<'_> {
    pub(super) fn observe(
        &mut self,
        segment: &SegmentEntry,
        payload: DurableSegmentPayload,
    ) -> Result<()> {
        match self.hook.as_deref_mut() {
            Some(hook) => hook(segment, payload),
            None => Ok(()),
        }
    }
}
