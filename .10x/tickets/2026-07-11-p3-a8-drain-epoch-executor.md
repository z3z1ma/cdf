Status: active
Created: 2026-07-11
Updated: 2026-07-19
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

None. A7, C2, and A1 staged ingress are done.

## Journal

- 2026-07-19: Activated after A7 closure. The implementation lane is confined to kernel/runtime/engine/project drain-epoch authority and mock-stream conformance; the concurrent Iceberg/object-access/dependency lane remains out of scope. The first slice will replace the explicit drain execution rejection with one reusable finite-epoch state machine, then integrate package/receipt/checkpoint gating without source- or destination-specific orchestration.
- 2026-07-19: Ratified the missing closure-cause vocabulary while installing the neutral controller. `EpochClosureEvidence` now distinguishes checkpoint cadence, package rotation, drain termination, and source exhaustion; termination cannot be falsely serialized as a cadence trigger. If rotation and cadence first become observable at the same canonical frontier, package rotation wins deterministically because it is the physical package ceiling. The controller consumes only drained canonical source positions, records exact overshoot, and enters `AwaitingSettlement`; it cannot observe later progress until the caller acknowledges that exact frontier as committed. This is the generic package/receipt/checkpoint gate, not a source or destination hook.
- 2026-07-19: Focused evidence: `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel execution_extent::tests --locked` passed 7/7 and `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime drain_epoch::tests --locked` passed 5/5. These tests cover typed closure causes, serialization validation, cadence overshoot, deterministic simultaneous-trigger precedence, exact settlement fencing, terminal settlement, source-frontier overshoot, and empty-drain no-op behavior. They do not yet prove engine package rotation or project destination/checkpoint integration.

## References

- `.10x/decisions/kernel-owned-stream-epoch-policy.md`
- `.10x/specs/stream-epochs-watermarks.md`
