Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Runtime native headroom policy v1

## Context

The user-facing memory budget is a process RSS ceiling, while Arrow/DataFusion/CDF-managed buffers are only part of process memory. A2 therefore needs a deterministic initial subtraction for allocator, runtime, Arrow metadata, database/native-library, thread-stack, and other non-ledger allocations. The P3 pre-optimization baseline observed a maximum median startup-path RSS of 45.16 MiB on the baseline host; this is fixed-cost evidence, not a safe enterprise ceiling. The P3 default process budget is 4 GiB.

## Decision

Policy `native-headroom-v1` reserves the greater of 512 MiB or 15% of the resolved process budget before creating the managed pool. An unspecified process budget resolves to the smaller of 4 GiB and 80% of effective host/container authority. An explicit budget above effective authority is rejected. Any resolution that cannot fit the declared minimum working set after headroom is rejected before execution with remediation.

The 512 MiB floor is more than eleven times the largest median fixed RSS observed in the baseline and leaves 3.4 GiB managed capacity under the ordinary 4 GiB default. The percentage term scales conservatively for larger native-library and concurrency footprints. RSS/process-tree stress remains the independent falsification mechanism; ledger balance cannot prove this headroom sufficient.

## Alternatives considered

- Subtract only the observed 45.16 MiB: rejected because the baseline uses tiny fixtures and does not exercise enterprise concurrency or large native destination state.
- Fixed 1 GiB: rejected because it needlessly starves smaller containers and does not scale with very large budgets.
- Percentage only: rejected because small-container fixed costs do not scale to zero.
- Treat the full process budget as managed: rejected because it guarantees RSS overshoot from native/untracked allocations.

## Consequences

Plans and reports can record a versioned, reproducible process/headroom/managed split. Operators still must account every CDF-owned data buffer, while WS-F can revise the policy only through a superseding decision backed by RSS evidence.
