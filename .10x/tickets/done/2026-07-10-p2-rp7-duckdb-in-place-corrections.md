Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/done/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/done/2026-07-10-p2-rp2-residual-verdict-runtime-package.md, .10x/tickets/done/2026-07-10-p2-rp3-correction-capabilities-row-provenance.md

# P2 RP7 DuckDB row provenance and in-place corrections

## Scope

Persist the canonical provenance tuple on DuckDB target rows and implement atomic addressed corrections with nullable-column migration, residual-path removal, package-token idempotency, and verifiable receipts.

## Acceptance criteria

- DuckDB append/replace/merge target rows persist `_cdf_load`, `_cdf_segment`, `_cdf_row` consistently with package segments/ordinals.
- Legacy targets receive an explicit migration/backfill or fail with exact remediation; no fake address is synthesized.
- Correction planning and transaction semantics match declared sheet capabilities.
- Missing/duplicate addresses, unsupported migrations, and partial failures roll back.
- Correction replay is a no-op and existing run/replay golden paths remain green.
- User identifiers cannot collide with reserved provenance columns.

## Evidence expectations

DuckDB live target inspection, legacy migration cases, addressed update/residual preservation, rollback/idempotency, receipt verification, and conformance.

## Explicit exclusions

No generic orchestrator, Postgres/Parquet behavior, lock publication, or destination readback beyond declared scope.

## Progress and notes

- 2026-07-10: Opened because DuckDB currently lacks target-row provenance even though it is the canonical local happy-path destination.
- 2026-07-10: Activated after RP2/RP3 closure and assigned to `/root/impl_d5`. Generic correction request/plan/receipt semantics remain kernel-owned; DuckDB-specific row persistence and transactions stay inside the adapter.
- 2026-07-10: Implemented canonical target-row provenance at the DuckDB adapter boundary. Every append/replace/merge row now carries `_cdf_load = original package hash`, `_cdf_segment = canonical segment id`, and segment-local zero-based `_cdf_row`; target DDL makes the tuple `NOT NULL` and `UNIQUE`. Merge dedup compares governed user columns while retaining the deterministic first package-row address. Multi-segment, append, replace, and merge inspections prove exact package/segment/ordinal values, and a direct duplicate-address insert is rejected.
- 2026-07-10: Added fail-closed legacy handling and reserved-name protection. Append/merge refuse absent or partial provenance, nullable system columns, wrong physical types, or missing tuple uniqueness with exact rebuild/backfill remediation; replace remains the explicit verified-package rebuild path. Package and schema-plan boundaries reject user `_cdf_*` impostors while allowing only the shared exact framework `_cdf_variant` classifier.
- 2026-07-10: Integrated DuckDB with the shared kernel correction protocol/session seam and canonical `Receipt` gate. The adapter now declares persisted/targetable provenance, exact residual readback, and `in_place_update` with `AtomicPackage`/`PackageToken` guarantees. Plan and begin both run the shared whole-request validator, consume compiler-produced one-field `residual-json-v1` Arrow authority, and never parse legacy `promoted_value_json` or infer destination names/types.
- 2026-07-10: Implemented read-only dry correction planning, nullable promoted-column DDL, exact address lookup, canonical residual-path removal, and parameterized row updates inside one DuckDB transaction. Multiple paths preserve unrelated canonical residual entries and the last removal stores SQL NULL. Missing rows/residuals, unsupported type migrations, stale residuals, and DDL/update failures fail without a receipt; a finalize-time stale-residual regression proves both DDL and prior-row updates roll back. Applying then aborting before finalize is a no-op.
- 2026-07-10: Implemented durable package-token correction idempotency and verification. Correction settlement writes the ordinary `_cdf_loads`/`_cdf_state` mirrors, canonical segment acknowledgements/counts, new schema hash, migrations, and closed typed correction evidence into the ordinary receipt. Reopen verification succeeds; replay returns the exact stored receipt without rereading already-removed residuals or duplicating target/mirror effects. Residual readback coverage reproduces canonical `_cdf_variant` paths plus the exact row address before correction.
- 2026-07-10: Added the shared canonical `remove_residual_json_v1_path` codec operation with preservation, last-path, absent-path, and noncanonical-input coverage; RP6 consumes the same helper. Focused verification passed: residual removal 2/2; DuckDB adapter 21/21 including five correction lifecycle cases; correction-only rerun 5/5; DuckDB all-target/all-feature strict Clippy; CLI DuckDB normalization/provenance shape, local run, source-free replay, and duplicate replay; project DuckDB replay/checkpoint and duplicate settlement; the committed DuckDB live-run golden across 100 runs plus duplicate live replay; and DuckDB semver 196/196 with no update required. No full workspace suite was run by this child.
- 2026-07-10: Implemented the shared `DestinationProtocol::read_correction_residual` hook behind DuckDB's advertised readback capability. The adapter performs an exact read-only provenance-tuple lookup, validates target provenance and canonical residual bytes, and returns the addressed optional envelope. The live correction test now proves byte-for-byte envelope/address reproduction before correction, unrelated-path preservation after partial promotion, and `NULL` readback after the final path is removed. Final rerun passed DuckDB 21/21, all-target/all-feature check, strict Clippy, and scoped diff validation.
- 2026-07-10: Parent integration verification passed 913/913 all-feature workspace tests, strict workspace Clippy, formatting, and diff checks. Evidence: `.10x/evidence/2026-07-10-p2-a10g-rp6-rp7-integration.md`. Adversarial review passed after read-only planning, exact provenance, rollback, capability/readback, and shared-protocol repairs: `.10x/reviews/2026-07-10-p2-a10g-rp6-rp7-integration-review.md`. All RP7 acceptance criteria are supported.

## Blockers

None.
