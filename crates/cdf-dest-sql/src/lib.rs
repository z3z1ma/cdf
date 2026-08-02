#![doc = "Typed transactional SQL destination mirror lifecycle for cdf."]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "destination commons must propagate recoverable failures"
    )
)]

mod identifier;
mod manager;
mod model;

pub use identifier::ValidatedSqlIdentifier;
pub use manager::{MirrorApplyOutcome, TransactionalMirrorBackend, TransactionalMirrorManager};
pub use model::{
    LoadMirrorKey, LoadMirrorMutation, LoadMirrorRow, MirrorCommit, MirrorInsertOutcome,
    MirrorReadIntent, QuarantineMirrorKey, QuarantineMirrorMutation, QuarantineMirrorRow,
    SegmentMirrorMutation, SegmentMirrorPolicy, SegmentMirrorRow, SegmentRowRange, StateMirrorKey,
    StateMirrorMutation, StateMirrorRow,
};
