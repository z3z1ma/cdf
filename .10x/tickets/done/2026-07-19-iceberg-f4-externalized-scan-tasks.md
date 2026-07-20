Status: done
Created: 2026-07-19
Updated: 2026-07-19
Parent: .10x/tickets/done/2026-07-19-iceberg-glue-source-program.md
Depends-On: .10x/tickets/done/2026-07-11-p0-wx1-portable-partition-task-protocol.md, .10x/tickets/2026-07-11-p3-f2-materialization-closure-audit.md

# Iceberg F4: externalized canonical scan-task sets

## Scope

Implement source-neutral content-addressed, bounded/spill-backed planned partition/task sets and specialize a safe canonical Iceberg task payload without serializing upstream Iceberg task structs.

## Non-goals

No remote scheduler/RPC, scan reader, catalog implementation, secret material, or unbounded inline `Vec` fallback.

## Acceptance Criteria

- Million-task synthetic planning holds the configured metadata budget and deterministic order.
- Task artifacts are canonical, tamper-detecting, generation-bound, stream-readable, and portable-capsule compatible.
- Iceberg task payload contains complete data/delete/schema/spec/name-map/predicate authority and no credentials/plaintext key material.
- Jobs/timing/spill location cannot change task or final package identity.

## References

- `.10x/specs/iceberg-source.md`
- `.10x/specs/portable-partition-task-protocol.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/specs/deterministic-parallel-scheduler.md`

## Assumptions

- User-ratified 2026-07-19: externalized source-neutral task authority is required; no Iceberg-only task store.

## Journal

- 2026-07-19: Activated after WX1 closure. The broader P3 F2 audit remains active, but this ticket explicitly owns its bounded metadata-cardinality integration slice: one source-neutral external task-set artifact consumed through the existing portable worker artifact reference. No remaining F2 decision is required for this bounded implementation.
- 2026-07-19: Added `cdf-task-store`, a source-neutral canonical task-set writer/reader with hash-while-write, atomic content-addressed install, spill accounting, fixed managed-memory reservations, per-record hashes, whole-artifact generation verification, canonical ordinal enforcement, and clean budget failure. Normal publication does not reread the just-written artifact; only an existing content-address collision is verified.
- 2026-07-19: Extended `ScanPlan` with an exclusive external task authority and added the planned-task-set artifact kind to the portable worker protocol. Inline and external partition authorities cannot coexist; high-cardinality plans have no unbounded inline fallback.
- 2026-07-19: Added the CDF-owned Iceberg task vocabulary without serializing upstream task types. One bounded, canonical shared-authority header carries the pinned snapshot, schemas, specs, name mapping, projection/predicate program, and reader capabilities; each file task binds its hash and carries only immutable file/range/generation, typed partition, schema/spec ID, and ordered delete facts. Credentials, signed URLs, plaintext keys, handles, and coordinator paths are structurally absent.
- 2026-07-19: Corrected Arrow-58 portable worker goldens missed by the earlier dependency alignment; the replacements are derived canonical task/attempt/result hashes, not compatibility fixtures.

## Blockers

None. The implementation must avoid the unrelated active runtime files owned by the concurrent worker.

## Evidence

- **Million-task boundedness and deterministic order:** `CARGO_BUILD_JOBS=10 cargo test -p cdf-task-store million_tasks_hold_the_configured_metadata_budget -- --ignored --nocapture` passed in 3.02 seconds. The generated one-million-record artifact held the configured 64 KiB managed-memory ceiling and 256 MiB spill ceiling. This synthetic test proves the artifact authority, not yet Iceberg manifest-planning throughput (owned by I1).
- **Task-store and Iceberg authority behavior:** `CARGO_BUILD_JOBS=10 cargo test -p cdf-task-store -p cdf-source-iceberg` passed 12 focused tests (one slow test ignored in the ordinary suite), covering deterministic cross-root identity, canonical ordering, shared-authority binding, typed Iceberg schema/spec/partition/delete validation, tamper detection, portable artifact conversion, forbidden secret-bearing shapes, and memory/spill failure.
- **Static quality:** `CARGO_BUILD_JOBS=10 cargo clippy -p cdf-task-store -p cdf-source-iceberg --all-targets -- -D warnings` passed.
- **Integration compile:** `CARGO_BUILD_JOBS=10 cargo check -p cdf-kernel -p cdf-runtime -p cdf-task-store -p cdf-source-iceberg -p cdf-engine -p cdf-source-files -p cdf-source-postgres -p cdf-source-rest -p cdf-python -p cdf-conformance -p cdf-dest-parquet -p cdf-dest-duckdb --all-targets` passed. This covers every changed `ScanPlan` constructor and the source/runtime extension graph; it is not a whole-workspace behavioral test.
- **Kernel and capsule regression:** `CARGO_BUILD_JOBS=10 cargo test -p cdf-kernel -p cdf-runtime --lib` passed 191 tests with two explicit performance tests ignored. It includes external-vs-inline scan authority, tampered references, portable fixtures, isolated worker reconstruction, and worker artifact validation.

## Review

Verdict: pass.

Fresh-hat review first falsified three drafts rather than preserving them: arbitrary `Serialize` input could admit nondeterministic maps, normal finalize reread all artifact bytes, and repeating table-level schema/spec/predicate authority in every Iceberg task amplified metadata at file cardinality. The final boundary accepts only a caller-owned canonical writer, hashes while writing, and stores shared authority once. A fourth review finding added writer poisoning after partial filesystem writes and strictly ordered equality-delete IDs. No critical or significant finding remains.

Residual risk: I1 must feed tasks in canonical order despite parallel manifest completion and must prove that property at jobs 1/N; F4 deliberately rejects out-of-order input rather than hiding an unbounded reorder buffer in the store. Remote content-provider publication/resolution remains the portable scheduler/provider concern; this ticket proves the typed artifact reference and local provider implementation, not an RPC transport.

## Retrospective

The central lesson is that reconstructability does not require repeating authority. A task can remain independently verifiable by binding one immutable shared authority hash, saving multiplicative metadata while strengthening semantic cohesion. Accepting arbitrary serializable values at an identity boundary was also too weak: deterministic bytes must be produced by the source-owned canonical encoder, while the generic store owns only framing, budgets, and hashes. Hash-while-write and explicit ownership transfer to the persistent content store preserved both performance and constant-memory semantics.
