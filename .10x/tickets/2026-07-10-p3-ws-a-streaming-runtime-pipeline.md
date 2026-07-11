Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/2026-07-10-p3-ws-l-performance-lab.md, .10x/specs/architecture-layering-runtime.md, .10x/tickets/2026-07-07-streaming-package-to-destination-commit-triage.md, .10x/tickets/2026-07-07-batch-sizing-segment-coalescing-triage.md

# P3 WS-A: streaming runtime pipeline

## Scope

Implement the Chapter 6 runtime spine: Tokio I/O runtime, CPU and bounded blocking pools, byte-bounded stage channels, shared memory ledger, recorded adaptive batching, spill/escalation, and segment-durable streaming destination sessions. Remove whole-run/package materialization from the ordinary bounded path while preserving the existing crash matrix and receipt-gated checkpoint commit.

This workstream is a plan and requires bounded executable children for runtime ownership, channels, ledger/spill, adaptive batching, streaming commit integration, and chaos/conformance.

## Acceptance criteria

- Decode, validate/normalize, segment encode, persist/hash, and destination write can overlap without unbounded queues.
- Every buffered byte is ledger-owned; exhaustion follows flush, backpressure, spill, clean failure.
- Replay uses recorded batches; streaming commit issues no receipt before final manifest verification.
- Peak commit memory is bounded by segments and configured queues, not package size.
- P1 progress and all crash-matrix rows remain green.

## Explicit exclusions

No distributed scheduler, semantic package change, or source/destination-specific orchestration branch.

## Blockers

Blocked until WS-L baseline evidence exists. This lane has an exclusive freeze on shared runtime-spine files while active.
