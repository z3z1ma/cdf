#![doc = "Engine-neutral runtime contracts and extension registries for cdf."]

mod bounded_format;
mod bulk;
mod canonical_frontier;
mod capabilities;
mod content_reclamation;
mod context;
mod controlled_byte_source;
mod destination;
mod execution_host;
mod format;
mod graph;
mod observed_byte_source;
mod registry;
mod retry;
mod scheduler;
mod source;
mod source_add;
mod source_frontier;
mod source_registry;
mod spill;
mod staging;
mod staging_lease;
mod transformed_byte_source;
mod utilities;

pub use bounded_format::*;
pub use bulk::*;
pub use canonical_frontier::*;
pub use capabilities::*;
pub use content_reclamation::*;
pub use context::*;
pub use controlled_byte_source::*;
pub use destination::*;
pub use execution_host::*;
pub use format::*;
pub use graph::*;
pub use observed_byte_source::*;
pub use registry::*;
pub use retry::*;
pub use scheduler::*;
pub use source::*;
pub use source_add::*;
pub use source_frontier::*;
pub use source_registry::*;
pub use spill::*;
pub use staging::*;
pub use staging_lease::*;
pub use transformed_byte_source::*;
pub use utilities::*;

pub mod foreign {
    pub use cdf_foreign_stream::*;
}

pub type RuntimeSecretProvider =
    dyn cdf_http::SecretProvider + Send + Sync + std::panic::RefUnwindSafe;

mod prelude {
    pub(crate) use std::{
        any::Any,
        path::{Path, PathBuf},
    };

    pub(crate) use arrow_schema::Schema;
    pub(crate) use cdf_kernel::{
        CapabilitySupport, CdfError, CommitPlan, CommitSession, DestinationCommitRequest,
        DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest, DestinationId,
        DestinationProtocol, DestinationSheet, DestinationSheetArtifact, Receipt,
        ReceiptVerification, ResourceStream, Result, SchemaHash, StateDelta, TargetName,
        WriteDisposition,
    };
    pub(crate) use cdf_package_contract::{PackageReplayInputs, SharedVerifiedPackageAccess};
    pub(crate) use serde::{Deserialize, Serialize};

    pub(crate) use crate::RuntimeSecretProvider;
    pub(crate) use crate::bulk::*;
    pub(crate) use crate::capabilities::*;
    pub(crate) use crate::context::*;
    pub(crate) use crate::destination::*;
    pub(crate) use crate::staging::*;
    pub(crate) use crate::utilities::*;
}

#[cfg(test)]
mod tests;
