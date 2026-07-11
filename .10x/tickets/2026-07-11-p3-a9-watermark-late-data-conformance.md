Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
Depends-On: .10x/tickets/2026-07-11-p3-a8-drain-epoch-executor.md

# P3 A9: watermark, late-data, and epoch conformance closeout

## Scope

Build shared conformance for typed watermark claims, partition aggregation/idleness/resumption, operator propagation/invalidation, all late-data verdicts, drain lifecycle, serialization, memory bounds, replay, and performance overhead; update VISION 6.5–6.6 coverage from evidence.

## Acceptance criteria

- Claims cannot regress or become stronger through an undeclared operator.
- New/idle/resumed partitions and each late-data action have fail-closed deterministic tests.
- No accepted/quarantined/recaptured row disappears across epoch barriers or recovery.
- Epoch control overhead remains within the P3 total overhead budget and metadata stays bounded.
- Coverage rows 6.5–6.6 cite implementation, conformance, and evidence rather than the future supervisor alone.

## Evidence expectations

Property/state-machine tests, chaos matrix, package/quarantine/checkpoint inspection, jobs-invariance hashes, memory/performance reports, and adversarial correctness/performance review.

## Explicit exclusions

No concrete CDC source, resident supervisor, or general windowing engine.

## Blockers

Blocked on A8.

## References

- `.10x/specs/stream-epochs-watermarks.md`
