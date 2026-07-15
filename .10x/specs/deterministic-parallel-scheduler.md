Status: active
Created: 2026-07-11
Updated: 2026-07-14

# Deterministic parallel scheduler

## Purpose and scope

This specification governs partition/unit ordinals, admission, effective jobs, canonical frontier/reordering, global limits, retries/rate limits, scope serialization, nested CPU control, cancellation, and jobs-invariance.

## Plan and capabilities

Every executable partition and nested unit MUST have unique contiguous canonical ordinals within its plan parent, immutable source/snapshot identity, scope/completion authority, estimated/minimum/maximum working sets, executor class, retry/speculation declarations, and output ordering. Missing unsafe declarations fail plan.

Drivers MAY expose lower useful concurrency than partition count. Source rate/quota, connection, snapshot, transaction, and ordering constraints are first-class capabilities. Plans MUST state whether a partition/unit is idempotent, reopenable, independently retryable, and safe to execute ahead of the canonical frontier.

## Admission and execution

The scheduler MUST enforce global configured jobs, CPU slots, memory minimums, I/O/connection permits, shared source rate/quota, blocking lanes, destination limits, and checkpoint-scope leases. It MUST NOT create parallelism outside the injected host or memory ledger.

Configured jobs counts concurrently active leaf work across nested partition/unit execution. Parent orchestration MUST NOT retain a jobs permit while awaiting children that require the same pool. A run MAY provision the host/container ceiling before source resolution, but it MUST tighten that same shared run-scoped admission object to the final joined source/destination/memory resolution before payload execution begins; the ceiling cannot increase or change after leaf work starts. Format/source/destination implementations consume this neutral permit and MUST NOT create private jobs semaphores.

Auto defaults MUST use effective container CPU/memory rather than physical-host totals. Internal native-library threads consume declared CPU slots or are configured down. Admission must remain work-conserving among eligible independent work and boundedly fair across resource transitions so one large glob cannot starve all others.

## Canonical frontier

Every outcome bundle is tagged before concurrency. The reorder structure MUST be accounted by payload and metadata and bounded by configured byte/item/ordinal lookahead. Only a fully successful canonical next outcome may advance the frontier. Quarantine/residual/verdict/lineage/position facts advance with their data outcome.

Slow/missing earlier work backpressures later producers and eventually stops new lookahead admission. Scheduling, queue pressure, spill, destination speed, and jobs cannot change released outcome order or canonical segment assembly.

## Limit and speculative work

Limit applies at the same compiled operator position as jobs=1: after residual filters and before projection/contract evaluation. It selects a canonical candidate-row prefix, so later quarantine may reduce accepted output below the requested limit. A batch crossing the limit is sliced only with exact row/source-position authority; otherwise the driver/assembler uses its conservative boundary rule. Exact pushdown remains forbidden when inexact residual filters could change the prefix.

Speculative outcomes beyond the satisfied prefix MUST release all resources, MUST NOT write canonical package/evidence/quarantine, and MUST NOT advance state/manifest completion. Side-effecting reads may speculate only when the source declares them safe and idempotent. Metrics disclose speculative bytes/requests discarded.

## Retry, identity, and errors

Retry classification is typed, not string matching. Budgets/backoff/deadlines are policy plus source capability; injected host timers make cancellation immediate. Attempt ids/timing are redacted runtime evidence.

Before accepting a retried outcome, runtime MUST validate its planned immutable/snapshot identity and schema attestation. Mixing object generations/pages/snapshots fails. Duplicate successful attempts are detected by planned unit identity; exactly one may reach the frontier.

First terminal failure cancels scope admission and joins tasks. Observed errors are rendered in canonical unit order with typed causes/retry history; no scheduling-dependent package artifact is emitted.

## Scope/state and destination

A logical file completes only after all selected units complete; its `FileManifest` entry is atomic at file level. A resource/checkpoint scope transition cannot commit concurrently with another transition derived from the same head. Lease acquisition/revalidation prevents stale-head commits.

Single-writer destinations serialize only their writer lane. Upstream decode/validation/package persistence may remain parallel until backpressure joins at the bounded graph. Final receipt/checkpoint order follows run orchestration.

## Conformance and performance

Permanent tests MUST run jobs 1/2/N with randomized delays/completion/failures, skew, limits at every boundary, retries/identity changes, slow first/last partition, small reorder budgets, source rate/quota, file subunits, scope conflicts, single/multiwriter destinations, cancellation, Python GIL/free-threaded modes, and native internal thread counts.

For fixed inputs/plans, package hash, segments, rows, positions, verdicts, quarantine, lineage, state preimages, and receipt package/segment identities MUST match jobs=1. Runtime attempt/timing/transaction details may differ truthfully.

Lab evidence MUST show scaling until the actual CPU/device/network/destination roofline and record CPU utilization, runnable/blocked time, queue/frontier wait, permits, context switches, speculative waste, retries, and nested oversubscription.

## Explicit exclusions

This spec does not define distributed workers, remote leases, unbounded-stream scheduling, or require speculation for unsafe sources.
