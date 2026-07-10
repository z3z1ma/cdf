Status: open
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-10-p2-residual-schema-promotion-program.md
Depends-On: .10x/tickets/2026-07-10-p2-rp2-residual-verdict-runtime-package.md, .10x/tickets/done/2026-07-10-p2-rp3-correction-capabilities-row-provenance.md

# P2 RP6 Postgres addressed in-place corrections

## Scope

Make Postgres conformance-testable for atomic addressed correction: enforce/verify uniqueness of `_cdf_load`, `_cdf_segment`, `_cdf_row`; plan nullable column migrations; stage correction rows; update only exact provenance tuples; return verifiable package receipts and idempotent counts.

## Acceptance criteria

- Existing and newly loaded targets have a safe unique/validated provenance address without changing user merge keys.
- Correction DDL/DML is dry-runnable and destination-sheet-consistent.
- One transaction applies migrations, addressed updates, `_cdf_variant` residual removal only for promoted paths, and receipt mirror.
- Missing/duplicate addresses fail before partial mutation; unrelated residual paths remain intact.
- Replaying a correction package is a package-token no-op with a verifiable receipt.
- Append still requires no semantic key.

## Evidence expectations

Live Postgres migrations/updates, duplicate/missing address negatives, partial residual preservation, receipt/checkpoint-ready output, rollback/failpoint tests, and destination conformance.

## Explicit exclusions

No generic promotion orchestrator, DuckDB/Parquet behavior, lock publication, or source rediscovery.

## Progress and notes

- 2026-07-10: Opened around the existing Postgres provenance columns and transactional reference role.

## Blockers

Depends on RP2/RP3.
