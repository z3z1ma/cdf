#![doc = "Bounded content-addressed task-set artifacts for cdf planners."]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "foundational production code must propagate recoverable failures"
    )
)]

mod canonical;
mod encoded;
mod limits;
mod publication;
mod sqlite_capacity;
mod store;
mod typed;

pub use canonical::CanonicalTaskSetBuilder;
pub use encoded::{
    ExternalTaskRecord, ExternalTaskSetArtifact, ExternalTaskSetReader, ExternalTaskSetWriter,
};
pub use limits::{CanonicalTaskSetLimits, ExternalTaskWorkspaceLimits, TaskSetLimits};
pub use store::{AccountedExternalTaskWorkspace, ExternalTaskStore, ExternalTaskWorkspace};
pub use typed::{
    ExternalTaskParseAdmission, ExternalTaskParseMemory, ExternalTaskPlanningCodec,
    ExternalTaskSetCodec, RetainedExternalTask, RetainedExternalTaskAuthority,
    TypedCanonicalTaskSetBuilder, TypedExternalTaskSetBuilder, TypedExternalTaskSetReader,
    TypedExternalTaskSetReaderConfig,
};

#[cfg(test)]
mod tests;
