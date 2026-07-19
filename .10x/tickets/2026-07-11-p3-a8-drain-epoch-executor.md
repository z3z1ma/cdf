Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md
Depends-On: .10x/tickets/done/2026-07-11-p3-a7-stream-policy-compilation.md, .10x/tickets/done/2026-07-11-p3-c2-parallel-frontier-execution.md, .10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md

# P3 A8: deterministic drain epoch executor

## Scope

Execute finite drain-mode epochs on the fused runtime graph: closure requests, canonical safe-frontier barriers, carryover/spill, rotation, per-epoch package/settlement/checkpoint gating, termination, resume, and bounded telemetry.

## Acceptance criteria

- All cadence/rotation/termination variants close only at recorded canonical safe frontiers.
- Every epoch independently passes package verification, destination receipt verification, and checkpoint gate before later progress publishes.
- Crash/resume repeats only existing lifecycle states and never advances past receipts.
- Pausable and non-pausable sources remain within the memory/spill contract; million-epoch metadata does not accumulate in memory.
- Non-pausable replay retention uses a byte/time-bounded rolling spool whose low watermark advances only with the committed checkpoint frontier; eviction/recovery cannot lose or duplicate an admitted position.
- Unbounded sources never use a finite-object spool, and exhaustion pauses/backpressures where supported or fails cleanly before memory/disk bounds are exceeded.
- Fixed captured intervals are jobs-invariant.

## Evidence expectations

Mock-stream integration, crash/chaos matrix, segment/manifest/checkpoint hashes at jobs 1/N, memory/spill traces, slow-destination/backpressure tests, and before/after lab overhead.

## Explicit exclusions

No resident daemon, concrete CDC connector, `cdc_apply`, or arbitrary event-time aggregation.

## Blockers

Blocked on A7, C2, and staged ingress.

## References

- `.10x/decisions/kernel-owned-stream-epoch-policy.md`
- `.10x/specs/stream-epochs-watermarks.md`
