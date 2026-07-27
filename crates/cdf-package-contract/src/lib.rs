#![doc = "Canonical package artifacts and verified-access contracts for cdf."]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "foundational production code must propagate recoverable failures"
    )
)]

mod access;
mod artifacts;
mod late_data;
mod model;
mod provenance;
mod quarantine;
mod receipt;

pub use access::*;
pub use artifacts::*;
pub use late_data::*;
pub use model::*;
pub use provenance::*;
pub use quarantine::*;
pub use receipt::*;
