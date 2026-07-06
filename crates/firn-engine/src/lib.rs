#![doc = "Planning and execution boundary for firn."]

mod execution;
mod planning;
mod predicates;
#[cfg(test)]
mod tests;
mod types;

pub use execution::{execute_to_package, execute_to_package_with_run_id};
pub use planning::{
    DATAFUSION_TABLE_PROVIDER_KIND, Planner, datafusion_filter_pushdown, negotiate_scan_plan,
};
pub use types::*;
