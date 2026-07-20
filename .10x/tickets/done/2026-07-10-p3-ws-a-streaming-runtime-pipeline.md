Status: done
Created: 2026-07-10
Updated: 2026-07-19
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/specs/architecture-layering-runtime.md, .10x/tickets/done/2026-07-07-streaming-package-to-destination-commit-triage.md, .10x/tickets/done/2026-07-07-batch-sizing-segment-coalescing-triage.md

# P3 WS-A: streaming runtime pipeline

## Scope

Implement the Chapter 6 runtime spine: Tokio I/O runtime, CPU and bounded blocking pools, byte-bounded stage channels, shared memory ledger, recorded adaptive batching, spill/escalation, and segment-durable streaming destination sessions. Remove whole-run/package materialization from the ordinary bounded path while preserving the existing crash matrix and receipt-gated checkpoint commit.

This workstream is a plan and requires bounded executable children for runtime ownership, channels, ledger/spill, adaptive batching, streaming commit integration, and chaos/conformance.

## Activated children

- `.10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md` — complete
- `.10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md` — complete
- `.10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md`
- `.10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md`
- `.10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md`
- `.10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md` (child of A5)
- `.10x/tickets/done/2026-07-11-p3-a7-stream-policy-compilation.md`
- `.10x/tickets/done/2026-07-11-p3-a8-drain-epoch-executor.md`
- `.10x/tickets/done/2026-07-11-p3-a9-watermark-late-data-conformance.md`
- `.10x/tickets/done/2026-07-11-p3-a12-byte-first-segments-shared-arrow-accounting.md`

## Acceptance criteria

- Decode, validate/normalize, segment encode, persist/hash, and destination write can overlap without unbounded queues.
- Every buffered byte is ledger-owned; exhaustion follows flush, backpressure, spill, clean failure.
- Replay uses recorded batches; streaming commit issues no receipt before final manifest verification.
- Pre-finalization destination work is staged ingress under non-identity attempt authority; only final verified package binding may publish target state and return a receipt.
- Peak commit memory is bounded by segments and configured queues, not package size.
- P1 progress and all crash-matrix rows remain green.
- Drain-mode unbounded resources rotate deterministic frontier-closed packages, enforce typed watermarks/late-data verdicts, and gate every epoch independently.

## Explicit exclusions

No distributed scheduler, resident supervisor, general windowing engine, semantic package bypass, or source/destination-specific orchestration branch.

## Blockers

None. WS-L and every WS-A executable child are done.

## Progress and notes

- 2026-07-11: A1 completed the destination-neutral staged-ingress/final-binding contract without provisional package identity or destination-specific runtime branches.
- 2026-07-11: A2 completed the neutral memory ledger, shared finite DataFusion coordinator, weighted discovery execution, working-set conformance, and versioned headroom policy.
- 2026-07-11: A3 completed plan-versioned row/byte canonical segmentation across the full Arrow vocabulary, adaptive nonidentity microbatches, typed position joins, source-rechunking invariance, and the fixed package golden.
- 2026-07-19: Reprioritized the remaining chain as program-critical. A7 and WX1 proceed as independent prerequisites; A8 follows A7, then C5 can close deterministic isolated-worker equivalence against both completed authorities.
- 2026-07-19: A8 closed with deterministic finite drain epochs, persisted partition-local restart authority, generic incomplete-construction recovery, explicit no-op outcomes, bounded replay retention, timer-independent closure, and minimum-partition watermark aggregation. A9 is now the sole remaining child and owns late-data/idle-resume/chaos conformance rather than another runtime architecture rewrite.
- 2026-07-19: A9 closed the runtime-spine conformance tail: source-authored partition idleness, monotone receipt-gated watermarks, exact late-data disposition and carryover, real crash recovery, jobs invariance, bounded/grouped evidence, and strict affected-crate Clippy. A12 had already completed byte-first canonical segments and shared Arrow accounting. The exclusive WS-A runtime-spine window is released.

## Evidence

- A1 proves destination-neutral staged ingress and verified final package binding.
- A2 and A6 prove one memory authority, pressure propagation, spill, and clean failure.
- A3 and A12 prove recorded adaptive microbatches with deterministic byte-first canonical segments.
- A4 and A5 prove injected execution ownership and bounded fused streaming operators without whole-package materialization on the ordinary path.
- A7-A9 prove compiled unbounded policy, deterministic frontier-closed epochs, receipt-gated checkpoints, typed watermarks and late-data outcomes, crash recovery, and jobs invariance. A9's final evidence is `.10x/evidence/2026-07-19-p3-a9-watermark-late-data-conformance.md`.
- Each child ticket contains its focused verification, performance evidence, review, and retrospective; all child paths above are terminal.

## Review

The child sequence was independently reviewed at its high-risk seams: memory ownership, deterministic segmentation, destination publication, runtime ownership, drain lifecycle, and watermark/late-data state. A9's initial fail review found persistence, idleness, evidence, and crash-window gaps; the final repair tranche closed them with focused conformance and strict Clippy. No destination/source identity branch was added to generic orchestration. Residual resident supervision, external worker high-cardinality storage, and host-class whole-product overhead remain explicitly owned by the supervisor, C5, and Z1 tickets rather than hidden in this closure. Verdict: pass.

## Retrospective

The runtime spine became tractable only after separating identity-bearing canonical segments from adaptive in-flight batching and separating destination ingress capability from destination identity. The late-data tail reinforced that receipt-gated state must be enforced in kernel and persistence authorities, not only the happy-path controller. Future distributed and resident execution should marshal these completed authorities rather than introduce parallel semantics. Performance changes were retained only with measurements; a plausible allocation optimization was deleted when it regressed the hot classifier.
