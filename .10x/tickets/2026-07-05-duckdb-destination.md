Status: open
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/done/2026-07-05-kernel-core-types.md, .10x/tickets/2026-07-05-package-builder-reader.md, .10x/tickets/2026-07-05-contract-compiler-normalization.md

# Implement DuckDB destination

## Scope

Implement `firn-dest-duckdb`: destination sheet, dry-run commit planning, DDL migration, append/replace/merge, Arrow appender/Parquet replay paths, idempotency tokens, `_firn_loads` and `_firn_state` mirrors, receipt verification, single-writer locking, and ICU doctor probe. Owns `crates/firn-dest-duckdb/**`.

## Acceptance criteria

- Destination sheet declares type mappings, identifier rules, dispositions, transactions, idempotency, bulk paths, migration support, and single-writer constraint.
- Append, atomic replace, and deterministic merge work against package segments.
- Replaying the same package returns duplicate/no-op behavior where package-token idempotency applies.
- Receipt verify query can confirm durable commit after process restart.
- ICU/timezone support is detectable.

## Evidence expectations

Record DuckDB integration tests for dispositions, DDL, idempotency, crash-recovery receipt verification, mirrors, and ICU probe.

## Explicit exclusions

No CLI-specific doctor UI; expose probes for project/CLI ticket.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.

