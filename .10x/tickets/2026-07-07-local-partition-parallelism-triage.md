Status: open
Created: 2026-07-07
Updated: 2026-07-07
Parent: .10x/tickets/2026-07-07-performance-investigation-backlog.md

# Triage local partition parallelism

## Scope

Investigate whether CDF's local execution should process independent scan partitions concurrently, and if so under what resource, package, destination, ordering, and checkpoint constraints.

This is a triage ticket. It does not authorize changing `cdf-engine`, adding async runtimes, introducing worker pools, changing package segment ordering, or widening destination concurrency semantics.

## Current hypothesis

Current engine execution is shaped around iterating planned partitions and streaming batches into package segments. That is simple and deterministic, but it may underuse CPU and IO when a resource can safely open multiple independent partitions. Parallel partition execution could improve file-source, REST-window, and future database-source throughput, but it may also complicate package determinism, segment ordering, memory bounds, rate limiting, checkpoint positions, and destination guarantees.

## Investigation questions

- Which resource kinds can honestly declare parallel-safe partition opens under current specs?
- Does `ResourceCapabilities` already express enough backpressure/concurrency information, or is a spec/API extension required?
- Can package segment identity remain deterministic if partitions execute concurrently but segments are emitted in planned order?
- How should per-resource rate limits, HTTP retry budgets, and auth refresh interact with concurrent partition opens?
- How should memory be bounded when multiple partitions produce Arrow batches at once?
- Does partition parallelism belong in `cdf-engine`, source-specific runtimes, a future distributed scheduler, or both local and distributed layers?
- How does local partition parallelism interact with `.10x/tickets/2026-07-05-distributed-execution-and-remote-state.md`?

## Candidate measurement scenarios

- Multiple independent local files with equal schema, each becoming one or more batches.
- Native Parquet file source with several files or row groups, if current source exposes partitions.
- Deterministic REST window partitions served by an in-memory transport with controllable latency.
- CPU-bound residual filtering or contract validation on several partitions.
- IO-bound package segment writes with many partitions.

## Acceptance criteria

- Classify local partition parallelism as `not needed yet`, `needs benchmark harness first`, `needs spec/API change`, or `ready for a bounded implementation ticket`.
- Identify the exact ordering and determinism invariant that any implementation must preserve.
- Identify required capability fields or destination/resource sheet constraints, if current types are insufficient.
- Identify memory and backpressure limits required before parallel opens are safe.
- Identify whether parallel execution should preserve byte-for-byte package hashes for fixed inputs; if not, name the spec/decision that must change before implementation.
- If implementation is recommended, open a separate child ticket scoped to one resource class and one execution mode.

## Evidence expectations

- Source inspection of `crates/cdf-engine/src/execution.rs`, resource partition plans, and package builder segment behavior.
- A small timing or profiling sketch if an existing deterministic fixture can be used without code changes.
- A risk list covering determinism, rate limits, memory, cancellation, error propagation, and source-position aggregation.

## Explicit exclusions

No engine rewrite, no Tokio runtime adoption, no worker pool, no distributed scheduler work, no destination concurrency changes, no package hash semantic change, no REST live-network test, and no alteration of conformance suites.

## References

- `.10x/tickets/2026-07-07-performance-investigation-backlog.md`
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/package-lifecycle-determinism.md`
- `.10x/tickets/2026-07-05-distributed-execution-and-remote-state.md`
- `crates/cdf-engine/src/execution.rs`
- `crates/cdf-kernel/src/resource.rs`

## Progress and notes

- 2026-07-07: Opened from performance discussion. The concern is a likely throughput ceiling from sequential local partition execution, but the first step is validating whether current workloads are actually partition-rich and whether deterministic packages can stay stable.
- 2026-07-11: P3 audit confirmed sequential execution and ratified canonical-frontier scheduling. C1–C4 own admission, fan-out/reorder, engine/FFI integration, and jobs/scaling invariance; WX1/C5 preserve the future distributed worker seam without adding a remote scheduler. This triage owns no implementation and remains open until C4/C5 record scaling and byte-identity evidence.

## Blockers

None for investigation. Implementation is blocked on triage evidence and explicit determinism/backpressure acceptance criteria.
