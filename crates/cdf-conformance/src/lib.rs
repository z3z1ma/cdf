#![doc = "Conformance harness boundary for cdf."]

pub mod checkpoint_store;
pub mod destination;
pub mod golden_package;
pub mod live_run;
#[cfg(test)]
mod mvp_acceptance_demo;
pub mod package_replay;
#[cfg(test)]
pub mod property_fuzz;
pub mod resource;
pub mod run_matrix;
pub mod runtime_chaos;
