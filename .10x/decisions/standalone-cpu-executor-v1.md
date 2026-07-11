Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Standalone CPU executor v1

## Context

The standalone host needs CPU work isolated from Tokio I/O and from blocking/FFI lanes, with explicit slot costs and no oversubscription from native internal parallelism. Candidate implementations were the already-present Tokio blocking pool, a new Rayon dependency, or a small standard-library fixed pool behind the neutral host contract.

## Decision

Use CDF's bounded standard-library fixed worker pool for CPU tasks. All CPU and blocking-lane workers acquire from one shared logical CPU-slot semaphore using the greater of declared task cost and native internal parallelism. Blocking lanes additionally enforce their own worker concurrency and affinity shape. Tokio remains the control/I/O runtime only.

The release comparison in `.10x/evidence/2026-07-11-p3-a4-cpu-executor-comparison.md` measured the fixed pool within 1.28% of Tokio `spawn_blocking` on 1,152 SHA-256 tasks. That difference does not justify surrendering explicit admission or adding another dependency. Rayon is not added without evidence that Arrow kernels need its work-stealing behavior.

## Alternatives considered

- Tokio `spawn_blocking` for CPU and FFI: rejected because its pool is not CDF's CPU/lane policy and mixes unrelated blocking work.
- Rayon: deferred because no measured advantage currently pays for a new global-pool dependency and embedding obligation.
- One OS thread per task: rejected as unbounded and incompatible with enterprise load.

## Consequences

The standalone host has predictable thread/slot ownership and embedders remain free to implement the neutral contract differently. The lab must continue comparing real decode/validation workloads; a superseding decision is required to change the default.
