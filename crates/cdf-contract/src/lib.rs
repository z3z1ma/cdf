#![doc = "Schema contract compilation boundary for cdf."]

mod aggregation;
mod compiler;
mod correction;
mod dedup_key;
mod evaluator;
mod fingerprint;
mod lattice;
mod normalization;
mod policy;
mod program;
mod reconciliation;
mod residual;
mod schema;
mod transforms;
mod vector;

pub use aggregation::*;
pub use compiler::*;
pub use correction::*;
pub use evaluator::*;
pub use fingerprint::*;
pub use lattice::*;
pub use normalization::*;
pub use policy::*;
pub use program::*;
pub use reconciliation::*;
pub use residual::*;
pub use schema::*;
pub use vector::*;

#[cfg(test)]
mod tests;
