Status: active
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-11-p3-a5-streaming-operator-graph.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/tickets/done/2026-07-11-p3-a3-canonical-segmentation-adaptive-batching.md, .10x/specs/spillable-package-dedup.md

# P3 A6: spillable package-order dedup barrier

## Scope

Implement final-output-row dedup placement, versioned typed key equality, accounted in-memory fast path, measured bounded external winner algorithm, canonical payload/key/decision spools, ordinal rejoin, v2 sharded provenance, legacy reader compatibility, and constant-memory conformance.

## Acceptance criteria

- Exact-row identity includes residual/variant and every normalized final output field; keyed fields resolve unambiguously.
- First/last/fail semantics match the simple reference evaluator for all supported Arrow types and random rechunking.
- Forced memory-to-spill transition at every boundary produces byte-identical package/evidence to the in-memory path.
- Adversarial high-cardinality/skew/collision inputs remain within budget and do not lose or merge unequal keys.
- No output/staged segment is durable before `fail` uniqueness certification.
- V2 provenance is deterministically sharded and bounded in memory; v1 packages remain inspectable/replayable.
- Scratch disk exhaustion/cancellation/crash fails cleanly and cleanup is idempotent.
- The 100 GB dedup stress completes within the configured RSS ceiling; algorithm selection has before/after/crossover evidence.

## Evidence expectations

Algorithm comparison, artifact-version decision/goldens, generated Arrow equality/reference properties, forced collisions, in-memory/spill/jobs invariance, RSS/disk stress, cleanup/crash tests, replay compatibility, security permission checks, and adversarial correctness/performance review.

## Explicit exclusions

No destination-state dedup, approximate filters as authority, cross-package retention window, distributed shuffle, or destination-specific implementation.

## Blockers

Depends on L5, unified accounting, and canonical order/segmentation. The v2 writer must not land without explicit package artifact migration evidence.

## References

- `.10x/decisions/spillable-package-order-dedup.md`
- `.10x/research/2026-07-11-package-dedup-spill-audit.md`
- `.10x/specs/spillable-package-dedup.md`
- `.10x/decisions/keyless-exact-row-deduplication.md`
- `.10x/decisions/contract-live-verdict-execution-semantics.md`

## Progress and notes

- 2026-07-11: Began with the correctness gate: move package dedup after residual/variant materialization, identifier normalization, effective-schema canonicalization, and compiled-output conformance so exact-row identity observes the actual package row rather than a pre-output intermediate.
- 2026-07-11: Landed the final-output placement gate. Exact-row compilation includes `_cdf_variant` when emitted; the evaluator resolves final-only fields; engine pending rows are normalized/conformed once before the barrier and no longer carry a detached variant vector. Focused contract and package tests prove rows differing only in captured residual values remain distinct.
- 2026-07-11: Added a runtime-neutral shared spill-budget coordinator to the injected execution host. Atomic reservations/growth enforce the accepted 8 GiB default globally, record current/peak/failure telemetry, and release by RAII; embedders can inject another authority. The dedup key encoder is now a public typed contract primitive returning exact Arrow row bytes per bounded batch, enabling the external barrier without duplicating equality semantics in the engine.
- 2026-07-11: Implemented and integrated the bounded external winner index for injected production execution: fixed-memory exact-key runs, 32-file merge fan-in, skew-safe winner certification, ordinal decision re-sort/join, owner-only opaque scratch, pre-write shared disk admission, fail-before-output, and idempotent cleanup. The forced spill path produces the exact reference manifest hash for existing exact-row conformance.
- 2026-07-11: Removed injected-path payload retention. Normalized final rows stream into a budgeted Arrow IPC spool with sequential partition/source-position metadata, then stream back one batch at a time against the certified ordinal decision stream. The production path no longer appends to `pending_dedup_batches`; simple non-injected execution remains only as the reference evaluator for conformance.
- 2026-07-11: Activated dedup evidence artifact v2. Dropped provenance streams into deterministic 65,536-row Parquet shards with UInt64 ordinal pairs; the bounded summary records format/version/target and shard identities without inline cardinality. Reader compatibility covers legacy v1 and validates v2 paths, manifest membership, schema, nulls, and ordering. The full 70-test engine suite is green.
- 2026-07-11: Accounted external sort/key-heap memory in the unified ledger. The index derives its minimum working set and merge fan-in from the largest exact encoded key; wide keys reduce fan-in rather than multiplying memory by 32. Forced production spill proves both memory and disk reservations peak nonzero and return to zero.
- 2026-07-11: Added the accounted in-memory winner fast path with lossless pressure/cap transition to the same external index. Complete state may stay resident only within a 64 MiB grant; decisions remain memory-resident and disk spill is one reservation byte. Generated skew/chunking laws match the reference across first/last, and a forced midstream memory-pressure transition balances the ledger. Release crossover evidence selects fast-when-complete-fits, external otherwise, with measured time/spill ratios across unique, duplicate, skew, identical, and 1 KiB-wide workloads.
