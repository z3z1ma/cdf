Status: active
Created: 2026-07-05
Updated: 2026-07-06
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
- 2026-07-06: Split first executable child `.10x/tickets/done/2026-07-06-local-system-sql.md` for read-only local `firn sql` over checkpoint/package metadata.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-local-system-sql.md`; `firn sql` now supports read-only local system-history queries over checkpoint rows and package manifest/receipt metadata.
- 2026-07-06: Split and closed child `.10x/tickets/done/2026-07-06-duckdb-ledger-mirror-doctor-drift.md` for the first concrete local DuckDB ledger/mirror drift doctor probe. `firn doctor` now reports local DuckDB ledger/mirror drift as skipped, passed, or failed with read-only probes.
- 2026-07-06: Split child `.10x/tickets/done/2026-07-06-python-doctor-interpreter-probe.md` for the next bounded doctor slice: configured Python interpreter health, version, GIL/free-threaded status, and `python.require_free_threaded` compatibility. This keeps the observability work moving without crossing into blocked run/preview/status runtime orchestration.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-python-doctor-interpreter-probe.md`; `firn doctor` now runs a fixed process-based Python interpreter probe, validates version/GIL/free-threaded metadata, enforces `python.require_free_threaded`, fails configured Python resources without an interpreter, and avoids executing project Python resource code.
- 2026-07-06: Split child `.10x/tickets/done/2026-07-06-doctor-secrets-duckdb-icu-health.md` for the next bounded doctor slice: closure-grade environment details, secret resolvability/redaction behavior, and DuckDB ICU reporting.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-doctor-secrets-duckdb-icu-health.md`; `firn doctor` now reports structured project/environment details, redacted resolved secret references including env/file/declarative coverage, missing/unavailable secret failures without value leakage, and read-only DuckDB ICU details for missing and existing databases.
- 2026-07-06: Split child `.10x/tickets/done/2026-07-06-status-freshness-local-ledger.md` for local read-only `firn status` freshness evaluation. The child explicitly avoids inventing a pipeline default by evaluating only unambiguous committed local ledger heads.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-status-freshness-local-ledger.md`; `firn status` now evaluates serving freshness SLOs from existing committed SQLite checkpoint heads, reports fresh/stale/non-evaluable states with structured details, keeps missing or ambiguous local state non-evaluable, and preserves concise scheduler-friendly human output.

## Blockers

None.
