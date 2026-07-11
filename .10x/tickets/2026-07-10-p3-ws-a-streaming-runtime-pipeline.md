Status: open
Created: 2026-07-10
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/2026-07-11-p0-dx1-neutral-runtime-crate.md, .10x/specs/architecture-layering-runtime.md, .10x/tickets/2026-07-07-streaming-package-to-destination-commit-triage.md, .10x/tickets/2026-07-07-batch-sizing-segment-coalescing-triage.md

# P3 WS-A: streaming runtime pipeline

## Scope

Implement the Chapter 6 runtime spine: Tokio I/O runtime, CPU and bounded blocking pools, byte-bounded stage channels, shared memory ledger, recorded adaptive batching, spill/escalation, and segment-durable streaming destination sessions. Remove whole-run/package materialization from the ordinary bounded path while preserving the existing crash matrix and receipt-gated checkpoint commit.

This workstream is a plan and requires bounded executable children for runtime ownership, channels, ledger/spill, adaptive batching, streaming commit integration, and chaos/conformance.

## Activated children

- `.10x/tickets/2026-07-11-p3-a1-staged-ingress-final-binding.md`
- `.10x/tickets/2026-07-11-p3-a2-unified-memory-ledger.md`
- `.10x/tickets/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md`
- `.10x/tickets/2026-07-11-p3-a4-injected-execution-host.md`
- `.10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md`
- `.10x/tickets/2026-07-11-p3-a6-spillable-package-dedup.md` (child of A5)
- `.10x/tickets/2026-07-11-p3-a7-stream-policy-compilation.md`
- `.10x/tickets/2026-07-11-p3-a8-drain-epoch-executor.md`
- `.10x/tickets/2026-07-11-p3-a9-watermark-late-data-conformance.md`

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

Blocked until WS-L baseline evidence exists. This lane has an exclusive freeze on shared runtime-spine files while active.
