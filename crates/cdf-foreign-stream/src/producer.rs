use std::fmt;

use cdf_kernel::{InvocationTermination, PartitionId, ResourceId, Result};

use crate::{
    cancellation::ForeignCancellation, descriptor::ForeignProducerDescriptor,
    events::ForeignEventStream,
};

#[derive(Clone, Debug)]
pub struct ForeignStreamOpenRequest {
    pub resource_id: ResourceId,
    pub partition_id: PartitionId,
    pub cancellation: ForeignCancellation,
}

pub struct ForeignStreamOpen {
    pub descriptor: ForeignProducerDescriptor,
    pub events: ForeignEventStream,
    /// Invocation-wide cancellation and join authority retained independently of stream polling.
    pub termination: InvocationTermination,
}

impl fmt::Debug for ForeignStreamOpen {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ForeignStreamOpen")
            .field("descriptor", &self.descriptor)
            .field("events", &"<foreign event stream>")
            .field("termination", &"<foreign invocation termination>")
            .finish()
    }
}

pub trait ForeignProducer: Send + Sync {
    fn descriptor(&self) -> &ForeignProducerDescriptor;
    fn open(
        &self,
        request: ForeignStreamOpenRequest,
    ) -> cdf_kernel::BoxFuture<'_, Result<ForeignStreamOpen>>;
}
