Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md, commits 664625ad..604a8b36
Verdict: pass

# P3 A4 execution-host implementation review

## Target

Neutral host/services contracts, standalone executor implementation, composition propagation, private-runtime removal, memory/CPU/lane admission, and first-party adapter confinement.

## Findings

No critical or significant finding remains.

- The first review pass found that a panicking typed blocking operation could return after its result channel disconnected but before the worker signaled CPU-slot release. The bridge now waits for the release signal on success, error, and panic; a permanent panic-then-success test proves the lane remains usable.
- The first review pass found `peak_cpu_slots` represented the largest single task cost rather than aggregate concurrent admission. Per-scope atomic usage tracking now records the aggregate peak, with a two-worker barrier test proving a peak of two.
- Runtime construction and blocking executors are centralized in the standalone host. A permanent source gate excludes test/benchmark code and rejects production regressions.
- Object-store sources and Parquet use host I/O. DuckDB uses a pinned lane; Postgres owns transaction state across its shared lane; Python selects a GIL or free-threaded lane from interpreter/resource evidence. Registry/orchestration code has no adapter-name branch.
- Cooperative-only drivers cannot be unsafely interrupted. Synchronous calls join before returning, while run scopes cancel, join/abort, and release leases/slots.

## Verdict

Pass. A4 acceptance is supported by the linked executor comparison, embedding tests, static gate, adapter tests, live DuckDB/Postgres paths, Python product-spine test, and strict Clippy evidence.

## Residual risk

The current engine has not yet moved decode/validate/segment operators onto run scopes, and DataFusion CPU work therefore does not yet exercise jobs-level host scheduling. This is the explicit A5/C1/C3 graph-and-scheduler scope, not hidden A4 runtime ownership. Postgres CSV COPY and Python read materialization are owned by D3/H2.
