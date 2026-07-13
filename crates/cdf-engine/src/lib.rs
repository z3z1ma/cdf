#![doc = "Planning and execution boundary for cdf."]

mod dedup_spill;
mod execution;
mod expression;
mod graph_plan;
mod memory;
mod output_schema;
mod planning;
mod predicates;
mod residual_spill;
mod segmentation;
mod standalone_host;
mod table_provider;
#[cfg(test)]
mod tests;
mod types;
mod variant_capture;

pub use execution::{
    DurableSegmentHook, PackagePreFinalizeHook, StreamingFinalizeHook, execute_to_package,
    execute_to_package_with_run_id, execute_to_package_with_segment_positions,
    execute_to_package_with_segment_positions_and_pre_finalize,
    execute_to_package_with_streaming_hooks, normalize_record_batch,
    preview_partition_selector_candidate, preview_resource,
};
pub use graph_plan::compile_operator_graph;
pub use memory::DataFusionMemoryCoordinator;
pub use planning::{
    CDF_NATIVE_RESOURCE_ADAPTER_KIND, Planner, datafusion_filter_pushdown, negotiate_scan_plan,
    validate_plan_schema_authority,
};
pub use segmentation::{
    AdaptiveMicrobatchController, CanonicalSegment, CanonicalSegmentAssembler,
    CanonicalSegmentationPolicy, PositionJoin, join_positions,
};
pub use standalone_host::StandaloneExecutionHost;
pub use table_provider::{QueryableResourceTableProvider, queryable_resource_table_provider};
pub use types::*;
