mod finalization;
mod measurements;
mod orchestration;
mod partition_lifecycle;
mod retry_frontier;
mod schema_admission;
mod segment_sink;

pub use finalization::{PackagePreFinalizeHook, StreamingFinalizeHook};
pub use orchestration::{
    DrainEpochExecution, LateDataCarryoverInput, assemble_isolated_worker_package,
    execute_drain_epoch_with_hooks, execute_to_package, execute_to_package_with_progress_hook,
    execute_to_package_with_run_id, execute_to_package_with_segment_positions,
    execute_to_package_with_segment_positions_and_pre_finalize,
    execute_to_package_with_streaming_hooks, normalize_record_batch, planned_empty_package_content,
    preview_partition_selector_candidate, preview_resource,
};
pub use segment_sink::{DurableSegmentHook, DurableSegmentPayload, PackageSegmentProgressHook};

#[cfg(test)]
pub(crate) use orchestration::{
    canonical_construction_reservation_bytes, resolve_pipeline_concurrency_from_bounds,
    statistics_computation_reservation_bytes,
};
#[cfg(test)]
pub(crate) use retry_frontier::partition_open_jobs;
