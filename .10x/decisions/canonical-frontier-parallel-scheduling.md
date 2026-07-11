Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Canonical-frontier parallel scheduling

## Context

CDF needs full CPU/I/O utilization, but package rows, limits, positions, evidence, and segments currently derive from sequential encounter order. Parallel tasks finishing in scheduler order would violate determinism; buffering all later results would violate constant memory.

## Decision

Every scan plan assigns immutable canonical partition ordinals. Format drivers may assign nested decode-unit ordinals inside one logical partition. Ordinals, source identity, scope, retry granularity, and canonical order are plan semantics. Worker/task/attempt ids and completion times are runtime evidence only.

The standalone scheduler is hierarchical and work-conserving under one execution host:

- a run-level fair admission queue shares CPU, memory, I/O, rate, and blocking lanes across resource transitions;
- a transition scheduler admits logical partitions up to configured `--jobs`/auto and capability limits;
- a partition scheduler admits decode units where the source/format declares safe concurrency;
- CPU/native work consumes declared shared slots, preventing nested oversubscription;
- destination writers and checkpoint scopes impose downstream serialization without forcing upstream single-threading where safe.

`--jobs` is a global logical-partition ceiling. Auto is the default and resolves from effective CPU/container quota, memory working sets, source/rate limits, reorder budget, transport connections, destination writers/staging, and native internal parallelism. Resolved concurrency/tuning is observable nonidentity evidence. `--jobs 1` remains the semantic reference, not a different execution implementation.

Parallel outputs carry `(partition ordinal, unit ordinal, local sequence)` into an accounted reorder boundary. A canonical commit frontier releases complete outcome bundles only in plan order. Lookahead is bounded by bytes, items, and ordinal distance; a stalled frontier backpressures/halts later admission rather than spilling unboundedly. Spill is allowed only through typed deterministic operator policy.

Global `LIMIT N` preserves the compiled jobs=1 operator position: it selects the first N candidate rows after residual filters and before projection/contract evaluation in canonical order. Later quarantine may therefore yield fewer than N accepted output rows. Speculative later work may execute only when source capability permits it, but rows/evidence/positions beyond that limit prefix are discarded as nonprocessed and cannot enter package identity or checkpoint state. Non-idempotent/nonreopenable sources disable speculative lookahead. Exact source limit pushdown must prove the same prefix and remains illegal across inexact filters.

Partition/unit retry uses the same plan identity and a new runtime attempt. Retry eligibility/budget/backoff comes from source capability/policy and injected timers; jitter/timing does not affect output. A retry reattests immutable/snapshot identity. Terminal identity change fails and requires replan. No successful checkpoint/file position advances until every required canonical unit/partition outcome contributing to it is complete.

On terminal failure, structured cancellation stops admission and joins all work; no package/receipt/checkpoint succeeds. Diagnostics record all observed terminal/join failures in canonical order with one primary trigger, but arrival order never becomes artifact semantics.

Transitions sharing a checkpoint scope/head acquire a scope lease before deriving final state and keep commit order serialized. Independent scopes may overlap extraction/package/staging under global budgets. Source rate/quota state is shared per resolved driver/source authority, not recreated per partition.

Python/free-threaded/GIL, DataFusion, DuckDB, compression, and other native libraries use generic host/lane/slot declarations. Scheduler code contains no source/destination/library-name branch.

## Alternatives considered

- Deterministic static worker assignment: rejected because skew leaves hardware idle; tagging/frontier gives determinism with work stealing.
- Emit completion order and sort only manifest entries: rejected because row order, limit, dedup, lineage, and positions already changed.
- Unbounded reorder buffer: rejected because one slow first partition makes memory input-dependent.
- Disable all speculation: rejected because safe idempotent file/row-group sources would underutilize hardware.
- Per-driver Tokio/Rayon pools/retry loops: rejected because global CPU/memory/rate control becomes impossible.
- Make resolved jobs part of plan/package identity: rejected because hardware/container tuning would churn identical data evidence.

## Consequences

Partition/unit ordinals and capabilities become explicit. A5's reorder/assembler implements the frontier. SX1/FX1 drivers expose scheduler needs. The lab measures utilization, queue/frontier stalls, speculative discard, retries, scaling, and oversubscription. Jobs-invariance becomes a permanent law.
