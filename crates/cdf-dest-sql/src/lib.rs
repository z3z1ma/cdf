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

pub use identifier::*;
pub use manager::*;
pub use model::*;
