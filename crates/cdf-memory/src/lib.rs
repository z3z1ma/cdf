#![doc = "Runtime-neutral memory accounting, admission, and payload ownership."]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::unwrap_used,
        reason = "foundational production code must propagate recoverable failures"
    )
)]

mod accounting;
mod budget;
mod cgroup;
mod coordinator;
mod spill;

pub use accounting::{
    AccountedBatch, AccountedBytes, BudgetTag, ConsumerKey, ConsumerMemorySnapshot, LeaseAccount,
    MemoryClass, MemoryEvent, MemoryLease, MemorySnapshot, OperatorMemoryProfile, PressureStrategy,
    ReservationRequest, record_batch_retained_bytes,
};
pub use budget::{
    DEFAULT_PROCESS_BUDGET_BYTES, DEFAULT_SPILL_BUDGET_BYTES, HEADROOM_POLICY_VERSION,
    MINIMUM_NATIVE_HEADROOM_BYTES, MemoryBudgetResolution, NATIVE_HEADROOM_PERCENT,
    resolve_memory_budget, resolve_unenforced_memory_budget,
};
pub use cgroup::{
    CGROUP_V2_MEMORY_PROVIDER_VERSION, CgroupV2MemoryReport, current_cgroup_v2_memory_report,
    current_cgroup_v2_memory_report_from,
};
pub use coordinator::{
    DeterministicMemoryCoordinator, MemoryCoordinator, MemoryWaiterSet, ReserveFuture, reserve,
    reserve_blocking,
};
pub use spill::{FixedSpillBudget, SpillBudgetCoordinator, SpillBudgetSnapshot, SpillReservation};

#[cfg(test)]
use cgroup::{cgroup_v2_memory_report_from_root, parse_cgroup_v2_relative_path};

#[cfg(test)]
mod tests;
