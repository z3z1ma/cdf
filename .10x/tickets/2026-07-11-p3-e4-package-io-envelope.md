Status: active
Created: 2026-07-11
Updated: 2026-07-14
Parent: .10x/tickets/2026-07-10-p3-ws-e-hashing-package-io.md
Depends-On: .10x/tickets/2026-07-11-p3-e3-streaming-verification-replay-io.md

# P3 E4: package I/O envelope and triage closeout

## Scope

Run small/large/many-segment/high-cardinality package build/verify/replay cases, publish roofline/hash/sync breakdowns, prove crash/golden/memory laws, and close the original package-I/O triage into measured evidence.

## Acceptance criteria

- Package build reaches ≥70% write roofline and hashing ≤5% wall on named hosts.
- Production construction reports zero redundant content reread bytes.
- High-cardinality build/verify memory is bounded.
- Triage hypotheses are each closed with before/after or measured no-action rationale.

## Evidence expectations

Host reports/profiles/syscall counts, crash/golden suite, memory stress, triage reconciliation, and adversarial filesystem review.

## Explicit exclusions

No hash/artifact semantic change.

## Blockers

Depends on E1-E3.

## References

- `.10x/tickets/2026-07-07-package-io-hashing-overhead-triage.md`
- `.10x/specs/package-io-hashing-durability.md`

## Journal

- 2026-07-14: Corrected the FineWeb critical-path attribution before tuning package I/O. A release run spent 4.118 seconds in the package interval, but sampling showed its main thread blocked on staged-destination backpressure while DuckDB flushed/checkpointed each of 115 segments. Raw warm fsynced write was 6.87 GB/s and SHA-256 was 2.96 GiB/s with 2.01% measured write overhead. The destination regression is owned by `.10x/tickets/2026-07-14-p3-d7-persistent-staged-ingress-stream.md`; E4 will remeasure the package-only roofline after D7 removes the confounder.
- 2026-07-14: Activated with a measured package critical-path improvement. Removing a four-worker encode cap initially failed because completed encoder output and staged destination input independently reserved the same Arrow allocations. Canonical pressure relief plus an owned batch-and-lease handoff completed the 2.147 GB FineWeb-to-DuckDB fixture and reduced package execution from 5.008 to 4.168 seconds (16.8%). Evidence: `.10x/evidence/2026-07-14-p3-f2-accounted-staged-payload-handoff.md`.

## Evidence

- Current critical-path measurement and accounted handoff: `.10x/evidence/2026-07-14-p3-f2-accounted-staged-payload-handoff.md`.
- The roofline, hash-share, high-cardinality, and redundant-reread acceptance criteria remain open.

## Review

Verdict: pass for the owned-handoff milestone; E4 remains active.

The change removes a false resource collision and an arbitrary concurrency cap without altering package bytes, ordering, hashing, or the destination capability boundary. It does not yet substantiate the package roofline acceptance criteria.

## Retrospective

Fixed worker caps can conceal broken resource ownership. Concurrency should be bounded by measured CPU, memory, disk, and destination authorities; when widening it fails, first test whether the same physical allocation is being counted at multiple pipeline stages.
