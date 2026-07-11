Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Process-tree constant-memory proof

## Context

CDF's managed pool is necessary but insufficient proof of constant memory. Native libraries, allocators, file mappings/cache, runtime stacks, and foreign children can escape it. The P3 100 GB/2 GiB and 1 TB laws need an enforceable, reproducible meaning.

## Decision

The configured memory budget is the maximum CDF process-tree resident working set CDF intends to consume for a run. Budget resolution records requested/default 4 GiB, effective container/host authority, calibrated native/runtime/allocator/thread/child headroom, resulting managed pool, and separate spill disk. A default resolves downward against container limits; explicit unsafe/impossible budgets fail before extraction.

Standalone Linux slow-tier execution uses a dedicated cgroup v2 subtree where available. It records/enforces aggregate `memory.max`/events and records `memory.peak`, while separately recording parent/child RSS high-water (`VmHWM`/`ru_maxrss`/equivalent) and managed-pool peaks. The acceptance ceiling is process-tree RSS within the configured budget; cgroup aggregate/file-cache observations are reported separately and must not be mislabeled RSS. The cgroup hard limit and any measured safety margin are calibrated/versioned by F1 and may not be silently relaxed.

Portable/macOS hosts run the same workload and record process-tree/high-water evidence but label enforcement unavailable. They do not substitute for the Linux enforced law. Container/Kubernetes deployments use effective cgroup limits even when host RAM is larger.

Every live allocation class is one of:

- managed CDF/DataFusion memory with exact RAII accounting;
- measured native/runtime/allocator/thread headroom with an owning component/version;
- external durable staging with bounded CDF buffers and byte evidence;
- OS/file-backed mapping/cache reported separately and constrained by the process/cgroup law;
- child-process memory inside the same aggregate authority or an enforceable child sub-budget.

Unclassified allocations are a closure failure. Headroom is not a dumping ground: F1 records component baselines/peaks under idle and representative load, versioned by host/runtime/library tuple. Persistent drift above calibration fails regression and opens the owner.

The normative stress suite includes deterministic generated inputs with bounded generator memory:

- 100 GB stream/file under a 2 GiB process budget, successful completion and observed spill;
- 1 TB synthetic glob under default budget in scheduled/manual tier;
- wide/nested, compressed high-ratio, million-file/segment metadata, all-unique/high-skew dedup, quarantine/residual-heavy, slow destination, remote readahead, and Python/subprocess child cases;
- repeated-run soak to expose leaks/fragmentation;
- a budget below one legal working set that fails `Data` with required/available/largest consumers/remediation before allocation/OOM.

Stress output proves input-size invariance by comparing steady-state/high-water across geometrically increasing inputs, not one passing size. The test generator and fixture preparation run outside the measured CDF child or report their memory separately.

OOM kill is never a clean failure. cgroup OOM events, signal death, allocator abort, or OS pressure termination fail the law even when a wrapper reports nonzero. Spill disk exhaustion is a separate clean `Data` case.

## Alternatives considered

- Assert only managed-pool peak: rejected because native/child/RSS escape remains.
- Use RSS polling as allocator admission: rejected because it is reactive and racy; the ledger controls admission while RSS falsifies completeness.
- Use cgroup `memory.current` as RSS: rejected because page cache/kernel accounting differs.
- Run only a 1 GB proxy: rejected because high cardinality/spill/fragmentation may appear later.
- Increase headroom until tests pass: rejected because it hides owners and makes the budget meaningless.
- Adopt jemalloc/mimalloc immediately: rejected pending L5/F1 evidence and supply-chain review.

## Consequences

F1 builds host measurement/enforcement and calibrates headroom. Every runtime component exposes memory ownership. F2 removes/unifies remaining materializations. F3/F4 execute/close the laws. Doctor and run reports distinguish process budget, managed pool, native headroom, spill, process RSS, and cgroup aggregate.
