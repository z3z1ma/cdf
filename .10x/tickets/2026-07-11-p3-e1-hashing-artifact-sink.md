Status: active
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-ws-e-hashing-package-io.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/specs/package-io-hashing-durability.md

# P3 E1: hashing artifact sink and writer receipts

## Scope

Implement the streaming hashing/atomic sink, typed writer receipts/durability states, IPC/small-artifact migration, hardware SHA measurement, and failure injection. Remove immediate post-write content rereads.

## Acceptance criteria

- IPC and identity artifact receipts match explicit reread hashes/bytes in conformance.
- No migrated writer reopens its output solely for manifest metadata.
- Segment publish receipt exists only after file/directory durability.
- Error/cancel/panic paths leave no registered partial file/receipt.
- SHA rate/features and hashing wall fraction are measured; golden bytes remain identical.

## Evidence expectations

Hash cross-check/property tests, failpoint matrix, syscall/read-byte profile, hardware/feature/dependency evidence, goldens, and before/after package benchmarks.

## Explicit exclusions

No streaming manifest/draft index, hash algorithm change, or mmap.

## Blockers

Depends on L5 baseline.

## References

- `.10x/decisions/hash-while-write-and-durability-barriers.md`
- `.10x/research/2026-07-11-package-io-durability-audit.md`
- `.10x/specs/package-io-hashing-durability.md`

## Progress and notes

- 2026-07-11: Began the typed atomic artifact sink and writer-receipt migration for small identity bytes and Arrow IPC segments, preserving segment-publish durability while removing immediate post-write hash rereads.
- 2026-07-11: Migrated IPC and small identity artifacts to hash-while-write receipts, added receipt-backed finalization for migrated artifacts, preserved intentional metadata rewrites, and added exact-hash plus failed/cancelled publication tests. Enabled and cargo-vet audited `sha2/asm`; measured 3.035 GiB/s versus 0.541 GiB/s software (5.61x). Evidence: `.10x/evidence/2026-07-11-p3-e1-hash-while-write-milestone.md`. E1 remains active for the complete boundary failpoint matrix and isolated large-payload hashing wall fraction.
