#![doc = "Planning and execution boundary for cdf."]

mod execution;
mod planning;
mod predicates;
mod table_provider;
#[cfg(test)]
mod tests;
mod types;

pub use execution::{
    PackagePreFinalizeHook, execute_to_package, execute_to_package_with_run_id,
    execute_to_package_with_segment_positions,
    execute_to_package_with_segment_positions_and_pre_finalize,
};
pub use planning::{
    CDF_NATIVE_RESOURCE_ADAPTER_KIND, Planner, datafusion_filter_pushdown, negotiate_scan_plan,
};
pub use table_provider::{QueryableResourceTableProvider, queryable_resource_table_provider};
pub use types::*;
