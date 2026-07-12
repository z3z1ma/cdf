Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a6-spillable-package-dedup.md

# Bounded external dedup winner index

## What was observed

Production execution with injected services now routes package dedup key state through a collision-safe external merge index. It spools exact Arrow row-encoding bytes plus canonical ordinals, creates fixed-memory sorted runs, merges with fan-in capped at 32 files, certifies one first/last/fail winner per exact key without retaining skewed groups, externally re-sorts decisions by package row ordinal, and joins those decisions back to canonical batches.

Normalized final-output payloads and per-batch partition/source-position authority now spool alongside the key index through Arrow IPC plus a bounded sequential metadata stream. After uniqueness certification, payload batches stream back one at a time and join the ordinal decision stream before canonical segment assembly. The injected path no longer populates the engine's package-wide `pending_dedup_batches` vector.

Every scratch write grows the shared spill reservation before issuing the write. Owner-only scratch directories contain no source values in path names and are removed by idempotent drop cleanup on success or error. `keep=fail` returns before any decision reaches segment persistence.

The index records the largest observed exact key and derives merge fan-in from a shared-ledger working-set reservation. Ordinary keys use the 8 MiB sort target and at most 32 merge heads; a wide composite/nested key raises the minimum working set and reduces fan-in so heap memory cannot silently multiply key width by 32. Exhausted memory fails before run allocation with explicit remediation.

## Procedure

- `cargo test -p cdf-engine dedup_spill::tests -- --nocapture` — 3 passed: forced multi-run first/last equivalence, fail-before-output, and clean disk exhaustion/cleanup.
- `cargo test -p cdf-engine append_exact_row_dedup_compiles_and_drops_only_complete_duplicates -- --nocapture` — passed both the simple reference evaluator and forced production spill path, with byte-identical manifest identity/package hash and balanced spill and memory ledgers after cleanup.
- `cargo check -p cdf-engine` — passed.

## What this supports

Dedup payload, winner/key/decision state no longer requires package-sized resident batches or a package-cardinality hash map on the ordinary injected production path. Uniform, all-identical/skew, all-unique, and collision cases share exact byte equality and bounded group state.

## Limits

Inline dropped-row provenance is still retained in a resident vector. A6 remains active until v2 sharded provenance, memory-accounted sort buffers, crossover measurement, and stress evidence land.
