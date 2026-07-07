Status: active
Created: 2026-07-05
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/2026-07-05-cli-surface.md, .10x/tickets/done/2026-07-05-duckdb-destination.md, .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md

# Implement observability, doctor, status, and system SQL

## Scope

Implement tracing fields, optional OTLP wiring where practical, system-history SQL mounting, `doctor` probes, `inspect run` story assembly, and `status` freshness evaluation. Owns observability modules across project/CLI/engine crates as coordinated by this ticket.

## Acceptance criteria

- Spans include run, resource, partition, and package identifiers.
- `cdf sql` can query ledger/package/load metadata in supported local configurations.
- `doctor` checks environment health, secrets, Python interpreter/free-threaded status, DuckDB ICU, and ledger/destination drift where fixtures support them.
- `status` exits nonzero on serving-resource freshness breach.
- Inspect run presents plan, verdicts, receipts, and transitions without leaking secrets.

## Evidence expectations

Record integration tests for SQL mounting, doctor probes, status exit codes, inspect redaction, and tracing field presence.

## Explicit exclusions

No dashboard or UI.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-06: Split first executable child `.10x/tickets/done/2026-07-06-local-system-sql.md` for read-only local `cdf sql` over checkpoint/package metadata.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-local-system-sql.md`; `cdf sql` now supports read-only local system-history queries over checkpoint rows and package manifest/receipt metadata.
- 2026-07-06: Split and closed child `.10x/tickets/done/2026-07-06-duckdb-ledger-mirror-doctor-drift.md` for the first concrete local DuckDB ledger/mirror drift doctor probe. `cdf doctor` now reports local DuckDB ledger/mirror drift as skipped, passed, or failed with read-only probes.
- 2026-07-06: Split child `.10x/tickets/done/2026-07-06-python-doctor-interpreter-probe.md` for the next bounded doctor slice: configured Python interpreter health, version, GIL/free-threaded status, and `python.require_free_threaded` compatibility. This keeps the observability work moving without crossing into blocked run/preview/status runtime orchestration.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-python-doctor-interpreter-probe.md`; `cdf doctor` now runs a fixed process-based Python interpreter probe, validates version/GIL/free-threaded metadata, enforces `python.require_free_threaded`, fails configured Python resources without an interpreter, and avoids executing project Python resource code.
- 2026-07-06: Split child `.10x/tickets/done/2026-07-06-doctor-secrets-duckdb-icu-health.md` for the next bounded doctor slice: closure-grade environment details, secret resolvability/redaction behavior, and DuckDB ICU reporting.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-doctor-secrets-duckdb-icu-health.md`; `cdf doctor` now reports structured project/environment details, redacted resolved secret references including env/file/declarative coverage, missing/unavailable secret failures without value leakage, and read-only DuckDB ICU details for missing and existing databases.
- 2026-07-06: Split child `.10x/tickets/done/2026-07-06-status-freshness-local-ledger.md` for local read-only `cdf status` freshness evaluation. The child explicitly avoids inventing a pipeline default by evaluating only unambiguous committed local ledger heads.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-status-freshness-local-ledger.md`; `cdf status` now evaluates serving freshness SLOs from existing committed SQLite checkpoint heads, reports fresh/stale/non-evaluable states with structured details, keeps missing or ambiguous local state non-evaluable, and preserves concise scheduler-friendly human output.
- 2026-07-06: Opened child `.10x/tickets/done/2026-07-06-engine-execution-tracing-spans.md` for the next bounded observability slice: engine package-execution spans with caller-supplied `RunId`, resource, partition, and package identifiers. Read-only exploration found `inspect run` is blocked until run-ledger and run-to-artifact semantics are ratified.
- 2026-07-06: Closed `.10x/tickets/done/2026-07-06-engine-execution-tracing-spans.md`; `cdf-engine` now exposes additive `execute_to_package_with_run_id`, emits exact package and partition tracing fields, preserves the existing untraced API and package identity, and has mutation-clean execution tests with full relevant `QUALITY.md` evidence.
- 2026-07-07: Run-ledger and inspect-run semantics were ratified in `.10x/decisions/run-ledger-commit-session-spine.md` and `.10x/specs/run-orchestration-ledger.md`. Implementation is now blocked on the run ledger store, general orchestrator, and CLI inspect-run child tickets rather than semantic ratification.

## Blockers

`inspect run` story assembly semantics are ratified, and the run-ledger store has landed in `.10x/tickets/done/2026-07-07-run-ledger-store.md`; implementation remains blocked until `.10x/tickets/2026-07-07-general-run-orchestrator.md` and `.10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md` land.
