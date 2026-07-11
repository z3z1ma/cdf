#![doc = "Engine-neutral runtime contracts and extension registries for cdf."]

mod capabilities;
mod context;
mod destination;
mod execution_host;
mod registry;
mod staging;
mod utilities;

pub use capabilities::*;
pub use context::*;
pub use destination::*;
pub use execution_host::*;
pub use registry::*;
pub use staging::*;
pub use utilities::*;

pub type ReceiptVerifiedHook<'a> = &'a dyn Fn(&cdf_kernel::Receipt) -> cdf_kernel::Result<()>;
pub type RuntimeSecretProvider =
    dyn cdf_http::SecretProvider + Send + Sync + std::panic::RefUnwindSafe;

mod prelude {
    pub(crate) use std::{
        any::Any,
        path::{Path, PathBuf},
    };

    pub(crate) use arrow_schema::{Schema, SchemaRef};
    pub(crate) use cdf_contract::{IdentifierPolicy, identifier_policy_from_destination_rules};
    pub(crate) use cdf_kernel::{
        CapabilitySupport, CdfError, CommitPlan, DestinationCommitRequest,
        DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest, DestinationId,
        DestinationProtocol, DestinationSheet, DestinationSheetArtifact, Receipt,
        ReceiptVerification, ResourceStream, Result, SchemaHash, StateDelta, TargetName,
        WriteDisposition,
    };
    pub(crate) use cdf_package::{PackageReader, PackageReplayInputs};
    pub(crate) use serde::{Deserialize, Serialize};

    pub(crate) use crate::capabilities::*;
    pub(crate) use crate::context::*;
    pub(crate) use crate::destination::*;
    pub(crate) use crate::staging::*;
    pub(crate) use crate::utilities::*;
    pub(crate) use crate::{ReceiptVerifiedHook, RuntimeSecretProvider};
}

#[cfg(test)]
mod tests;
