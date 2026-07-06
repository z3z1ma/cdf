Status: open
Created: 2026-07-05
Updated: 2026-07-05
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: .10x/tickets/2026-07-05-cli-surface.md, .10x/tickets/done/2026-07-05-duckdb-destination.md, .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md

# Implement observability, doctor, status, and system SQL

## Scope

Implement tracing fields, optional OTLP wiring where practical, system-history SQL mounting, `doctor` probes, `inspect run` story assembly, and `status` freshness evaluation. Owns observability modules across project/CLI/engine crates as coordinated by this ticket.

## Acceptance criteria

- Spans include run, resource, partition, and package identifiers.
- `firn sql` can query ledger/package/load metadata in supported local configurations.
- `doctor` checks environment health, secrets, Python interpreter/free-threaded status, DuckDB ICU, and ledger/destination drift where fixtures support them.
- `status` exits nonzero on serving-resource freshness breach.
- Inspect run presents plan, verdicts, receipts, and transitions without leaking secrets.

## Evidence expectations

Record integration tests for SQL mounting, doctor probes, status exit codes, inspect redaction, and tracing field presence.

## Explicit exclusions

No dashboard or UI.

## Progress and notes

- 2026-07-05: Opened from book and specs.

## Blockers

None.
