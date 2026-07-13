Status: done
Created: 2026-07-11
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-11-p3-ws-v-vectorized-validation.md
Depends-On: .10x/tickets/done/2026-07-11-p3-v1-vector-kernel-plan.md, .10x/tickets/done/2026-07-11-p3-a5-streaming-operator-graph.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md

# P3 V2: validation masks and selected evidence in the graph

## Scope

Integrate vector masks/aggregates with normalize/filter/package/quarantine/residual paths, fuse only where measured, remove duplicate scalar evaluation, ledger-account selected-row evidence, and preserve package/verdict bytes.

## Acceptance criteria

- Ordinary production validation uses V1 kernels once per batch.
- Accepted rows remain vectorized; only required selected rows materialize evidence.
- High-failure evidence spills/bounds correctly and no row/verdict disappears.
- Existing and new golden packages/jobs invariance/crash recovery pass.

## Evidence expectations

Integration/golden/chaos/memory tests, duplicate-evaluation static/profile proof, phase telemetry, before/after macro profiles, and adversarial evidence review.

## Explicit exclusions

No semantic rule additions or target closeout.

## Blockers

Blocked on V1, A5, and A2.

## References

- `.10x/specs/vectorized-contract-validation.md`

## Progress and notes

- 2026-07-11: Production preview and run contract execution now use a run-lived `VectorValidationEvaluator`; it binds once per observed Arrow schema and automatically rebinds when physical provenance metadata changes. Both fused no-residual and residual-present paths use the same vector evaluator. The scalar function remains only as the `cdf-contract` oracle/property surface, and an engine architecture test forbids calling it from production execution. All 90 non-ignored engine tests passed. The existing 64k fused-transform benchmark improved from the recorded 3.912 GiB/s baseline to 15.658 GiB/s at the same 200 iterations; unfused measured 2.183 GiB/s and fused/unfused outputs remain golden-identical. V2 remains open for exact ledger accounting/spill of high-failure selected evidence and macro package/TLC profiles.
- 2026-07-11: Removed batch-wide quarantine candidate and Parquet-byte materialization from production. The vector evaluator now emits selected candidates through a callback; the engine converts them directly into an 8,192-record bounded accumulator shared by native-rule, residual, and pre-contract evidence; and `cdf-package` writes successive Arrow batches through an owned streaming Parquet writer into the existing atomic hash-while-write artifact sink. The old quarantine records↔Parquet `Vec<u8>` APIs were deleted. A 20,000-record multi-chunk round trip, all engine contract/residual/package laws, and strict Clippy pass. Fused throughput remains 15.496 GiB/s. V2 stays open only for explicit memory-ledger reservation/measurement of the bounded evidence/Parquet working set and macro TLC/package profiles. Evidence: `.10x/evidence/2026-07-11-p3-v2-streaming-quarantine-evidence.md`.
- 2026-07-11: The bounded quarantine accumulator now owns a named `quarantine-evidence` lease in the shared coordinator. It starts at one byte only when evidence is possible, grows before retaining each record, reserves a conservative 3x exact record/string/source-position estimate for simultaneous records+Arrow+Parquet buffers, flushes and retries on pressure, forces the Parquet row group durable before releasing the chunk, and drops Vec capacity before shrinking the lease. A 1 KiB deterministic budget rejects a 4 KiB value before artifact creation and releases the ledger to zero. V2 now remains open for the macro TLC/package profile and final golden/RSS closeout rather than an unowned evidence buffer.
- 2026-07-11: Repaired the performance-lab package fixture to publish the runtime Arrow schema required by the current package contract, then completed the smoke macro suite. The engine package/filter/project cell measured 128.62–133.44 ms (131.11 ms median), a statistically significant 7.78% median improvement against the local stored baseline. NDJSON, archive, REST, and DuckDB replay smoke samples were variance-bound and are not claimed as improvements. V2 remains open because the final golden-package closeout is currently prevented by the active DX2/DX3 destination-composition migration: `cdf-conformance` still calls removed concrete `ResolvedProjectDestination::{duckdb,parquet_filesystem,postgres}` constructors. The owning P0 tickets are `.10x/tickets/done/2026-07-11-p0-dx2-driver-owned-adapters-composition.md` and `.10x/tickets/2026-07-11-p0-dx3-generic-lock-doctor-replay.md`. Evidence: `.10x/evidence/2026-07-11-p3-v2-smoke-macro-profile.md`.
- 2026-07-11: DX4 replaced those stale destination constructors with registry-backed conformance resolution; strict compilation and the package golden suite are green. The live golden now stops at the next independent architecture migration (`compiled declarations are not executable; resolve their typed source driver`), owned by `.10x/tickets/2026-07-11-p0-sx1-source-extension-boundary.md`. V2 remains open for the live golden/RSS criteria after SX1 migrates its fixtures.
- 2026-07-11: SX1 migrated the local-file fixture to typed source resolution and promoted its current deterministic segment golden. The canonical DuckDB live law now passes across 100 rebuilds through source and destination registries. V2 remains open only for its explicit RSS/high-failure closeout rather than functional/golden integration.
- 2026-07-11: Closed V2 after the 100%-quarantine stress exposed and removed a long-lived Parquet-writer metadata leak. One writer previously retained row-group footer metadata across every bounded flush: 25k rows peaked at 20.82 MiB RSS and 250k at 30.79 MiB. Quarantine chunks now finalize independent atomic Parquet parts; the same probes peak at 19.87 MiB and 20.20 MiB respectively (0.33 MiB delta for 10x rows), while the shared evidence lease remains capped at 512 KiB and returns to zero. A permanent 25k-row law reads every rotated part back, proving exact count/order/no loss. All 93 non-ignored engine tests and strict Clippy pass. Evidence: `.10x/evidence/2026-07-11-p3-v2-constant-memory-quarantine-closeout.md`; review: `.10x/reviews/2026-07-11-p3-v2-closeout-review.md`.
