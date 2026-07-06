Status: open
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/2026-07-05-package-builder-reader.md, .10x/tickets/2026-07-05-contract-compiler-normalization.md

# Implement Postgres destination

## Scope

Implement `firn-dest-postgres`: destination sheet, type mappings, identifier rules, transactional DDL, append/replace/merge with `ON CONFLICT`, xid-bearing receipts, `_firn_loads` and `_firn_state`, receipt verification, and Postgres source-side exercise hooks where appropriate. Owns `crates/firn-dest-postgres/**`.

## Acceptance criteria

- Append, transactional replace, and merge work with deterministic dedup.
- DDL migration plans are dry-runnable and included in commit plans.
- Receipts include transaction metadata, counts, schema hash, migrations, and verification query.
- Ledger/mirror drift can be detected by project/doctor code.
- Type mappings handle exact/widening/lossy/unsupported fidelity per sheet.

## Evidence expectations

Record integration tests against Postgres or a test container, including dispositions, DDL, idempotency/replay, receipt verification, and mirror state.

## Explicit exclusions

No warehouse destinations in this ticket.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.

