Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/evidence/2026-07-11-p0-fx1-accounted-batch-retention.md
Verdict: pass

# Review: accounted batch retention

## Assumptions tested

- Lifetime retention can cross the kernel boundary without importing `cdf-memory`.
- Source and destination retention are the same architectural concern.
- Cloning a kernel batch must clone ownership, and dropping the final batch owner must release accounting.
- Referenced payloads must not accept an in-memory retention owner.

## Findings

No critical or significant finding remains. `PayloadRetention` is an opaque owner plus byte count in the kernel; the concrete owner remains a runtime `MemoryLease`. The native envelope consumes rather than clones its lease when entering the kernel. Existing destination retention was replaced, not aliased or shimmed. Referenced batches reject the operation.

The public `Batch::record_batch` borrow does not itself carry a second token. Consumers that retain Arrow arrays beyond the batch lifetime must transfer them under their own admitted working-set lease; engine transformation already follows that rule. A future consumer bypassing that handoff would violate the runtime accounting contract and belongs in shared conformance.

## Verdict

Pass. This removes the ownership blocker for production native-driver composition while keeping the kernel neutral.

## Residual risk

Full graph conformance must prove no operator retains a cloned Arrow batch past the input batch lifetime without an output lease. FX1 remains open until composition and that conformance land.
