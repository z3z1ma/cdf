Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Stream epochs, drain execution, and watermarks

## Purpose and scope

This specification governs kernel execution extent artifacts, unbounded policy compilation, finite epoch closure, package rotation, checkpoint cadence, typed watermarks, late-data verdicts, drain termination, recovery, observability, and conformance.

## Artifact model

Every logical and physical plan node MUST carry the kernel-owned execution extent. An unbounded extent MUST carry a complete versioned `StreamEpochPolicy`; missing cadence, rotation, watermark/late-data, aggregation/idleness, or safe-frontier policy is a plan-time contract error naming the field and remediation.

Policy enums MUST be closed/versioned kernel values, not free-form strings or runtime types. Plans, lockfiles, package evidence, and checkpoint proposals record the effective policy and epoch/frontier identity. Migration preserves existing bounded plan/package fixtures and maps legacy drain values only when semantics are unambiguous; ambiguous legacy live values fail explicitly.

## Epoch execution

The ordinary fused graph executes both bounded partitions and drain epochs. A closure trigger requests a barrier. Admission stops at a canonical safe frontier; already admitted work at or below it drains in canonical order; work above it is retained/spilled for the next epoch or deterministically re-read. No row may be lost or committed twice because of the barrier.

Each nonempty epoch finalizes and verifies its package, settles destinations, verifies receipts, and commits its scope before the next epoch may publish a later checkpoint. Empty epochs do not create empty packages unless an explicit evidence-heartbeat policy exists in a later specification. A no-data drain reports a verified no-op.

Timer triggers are monotonic control observations. The selected frontier, trigger reason/time, overshoot, input low/high positions, and next-epoch carryover are evidence. Wall-clock timing MUST NOT participate in canonical package identity except where an existing artifact specification explicitly requires a recorded event timestamp outside identity.

Drain termination MUST be explicit and finite: quiescence as defined by a source capability, maximum duration, maximum admitted records/bytes, or a source frontier. Cancellation is not successful drain termination. On termination, the current legal epoch closes and gates unless the user requested abort; exact CLI behavior is governed separately.

## Watermarks and late data

A watermark claim MUST name event-time field/domain, typed position, partition, provenance/authority, policy version, and observation context. Claims MUST be monotone within their scope. Regressions are source contract events and cannot reduce a committed watermark.

Global watermark is the minimum across eligible active partition claims. New partitions, missing claims, idleness, and resumption after idleness follow explicit plan policy. A resumed partition producing data behind the effective watermark is late data and receives the configured total verdict.

Each operator/codec/source descriptor MUST declare watermark behavior: preserves, transforms with a named monotone mapping, or invalidates/drops. Filters preserve; projections preserve only while retaining required authority; arbitrary code defaults to invalidating unless it declares and passes conformance. Watermarks are control metadata accounted in bounded sinks, not an unbounded side channel.

Late data actions are:

- `recapture_next_epoch`: preserve the row and its late annotation for the next eligible epoch/package;
- `quarantine`: persist the row and named late-data rule in quarantine evidence;
- `admit_with_annotation`: include it in the current open epoch with physical/event-time lateness metadata when the epoch is not finalized.

No action may rewrite a finalized package or committed destination implicitly. Backfill/correction of previously committed event-time ranges compiles as an ordinary explicit plan.

## Recovery and performance

Epoch package/checkpoint crash windows are the existing crash matrix repeated. Resume starts from the last receipt-verified committed epoch head, verifies any finalized/settled in-flight epoch, and never contacts/replays positions already proven committed unless source semantics require a recorded overlap.

Epoch barriers, watermark aggregation, and control evidence MUST be O(active partitions plus bounded frontier window), ledger-accounted, and rate-limited. Million-partition metadata uses append/spill-backed state. Slow destinations propagate backpressure; non-pausable sources use the ratified spill policy.

## Conformance

Conformance MUST use mock pausable and non-pausable unbounded resources and cover all trigger/termination/late-data modes, watermark regression, idle/new/resumed partitions, operator invalidation, empty drain, barrier overshoot, crash at every package/receipt/checkpoint boundary, memory pressure/spill, jobs invariance for fixed captured intervals, replay, and serialized migration/golden stability.

The same captured interval at `--jobs 1` and `--jobs N` MUST produce identical epoch frontiers, segments, manifests, receipts (excluding permitted destination metadata), and checkpoints. Different live captures need not choose identical time-triggered frontiers; each must remain internally evidenced and replayable.

## Explicit exclusions

No resident daemon lifecycle, distributed leases, concrete log protocol, `cdc_apply`, general aggregation/window/trigger/retraction engine, processing-time-as-event-time inference, or finalized-package mutation is specified here.
