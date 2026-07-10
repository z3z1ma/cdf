Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/2026-07-10-p2-rp2-residual-verdict-runtime-package.md, .10x/tickets/2026-07-10-p2-rp3-correction-capabilities-row-provenance.md

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

## Blockers

Depends on RP2/RP3.
