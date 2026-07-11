Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-11-p3-e1-hashing-artifact-sink.md
Verdict: pass

# P3 E1 hashing artifact sink review

## Target

The E1 implementation at commits `426a08b5` plus the closure delta: typed hash-while-write receipts, atomic publication ordering, receipt-backed finalization for migrated writers, hardware SHA activation, and boundary failure tests.

## Findings

No critical or significant E1-scoped defect remains.

- Exact-byte risk: `HashingWriter::write` hashes only the prefix the underlying writer reports as written, and `write_all` retains standard short-write semantics. Explicit reread hash/count conformance passes.
- Premature-authority risk: the receipt is constructed only after encoder finish, flush, file sync, rename, and directory sync. Injected failures at each boundary return no receipt.
- Cleanup risk: error, cancellation, and panic tests prove temporary siblings are removed. A directory-sync failure may leave a complete renamed draft but never a receipt or manifest reference, matching the ratified crash state.
- Redundant-read risk: registered artifacts are selected from receipts after metadata size reconciliation. The unreadable-content finalization test would fail if content were reopened.
- Compatibility risk: all package tests and the fixed v1 fixture hash pass; intentional same-path metadata rewrites replace the prior receipt rather than being misclassified as duplicates.
- Supply-chain risk: the added `sha2-asm 0.6.4` fixed-buffer FFI/assembly dependency is covered by a recorded cargo-vet `safe-to-deploy` audit and improves measured SHA rate 5.61x.

## Verdict

Pass for E1. Its acceptance criteria are supported by `.10x/evidence/2026-07-11-p3-e1-hash-while-write-milestone.md`.

## Residual risk

The in-memory receipt map, unregistered trace writer, metadata directory-barrier coalescing, million-entry bounded finalization, unexpected filesystem entry reconciliation, and production elimination of all compatibility hashing are explicitly owned by E2. Streaming verification/replay read fusion remains E3. These are downstream scoped work, not hidden E1 exceptions.
