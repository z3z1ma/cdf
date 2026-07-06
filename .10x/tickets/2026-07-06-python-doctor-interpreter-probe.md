Status: open
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-observability-doctor-status-sql.md
Depends-On: .10x/tickets/done/2026-07-05-project-format-lockfile-secrets.md, .10x/tickets/done/2026-07-05-python-sdk-bridge.md

# Implement Python interpreter doctor probe

## Scope

Make `firn doctor` genuinely check configured Python interpreter health, version, GIL/free-threaded status, and `python.require_free_threaded` compatibility using only the configured interpreter process. Owns the Python doctor probe implementation in `crates/firn-cli/**` and its focused tests.

## Acceptance Criteria

- `firn doctor --json` reports a `python` check with structured details when `python.interpreter` is configured: resolved executable path, version, implementation, `gil_enabled`, `free_threaded_build`, `can_parallelize_python`, and `require_free_threaded`.
- A configured interpreter path that is missing, not executable, exits unsuccessfully, emits invalid JSON, or reports a Python version older than 3.12 makes the `python` check fail and makes `doctor` exit nonzero.
- If `python.require_free_threaded = true`, `doctor` fails unless the interpreter reports a free-threaded build with the GIL disabled.
- If no interpreter is configured and the project has no Python resources, `doctor` skips the `python` check.
- If a Python resource is configured but `python.interpreter` is absent, `doctor` fails with a Python interpreter issue rather than silently skipping.
- The probe MUST NOT import or execute project Python resource code. It may execute only a fixed inline interpreter-inspection snippet through the configured interpreter.
- The probe MUST NOT leak resolved secret values in stdout, stderr, JSON details, or error messages.
- Existing doctor drift behavior remains read-only and does not create missing SQLite or DuckDB databases.

## Evidence Expectations

Record targeted `firn-cli` tests for skipped, missing, invalid-output, GIL-enabled pass, and `require_free_threaded` failure cases; include a regression test proving `doctor` still does not create missing state or DuckDB files. Before closure, run focused fmt/test/clippy for `firn-cli`, relevant workspace checks, security scans required by `QUALITY.md`, and update the observability parent with the child outcome.

## Explicit Exclusions

Do not implement Python resource execution, run orchestration, preview source opening, status freshness evaluation, OTLP export, or `inspect run`. Do not add a `firn-python` dependency to `firn-cli` unless implementation evidence shows the fixed process probe is insufficient.

## Progress and Notes

- 2026-07-06: Opened from `.10x/specs/project-cli-observability-security.md`, the observability parent ticket, and the existing `firn-python` interpreter/free-threaded semantics. Current `firn doctor` only checks that a configured interpreter path exists; this child turns that placeholder into a real doctor probe without crossing into blocked runtime orchestration.

## Blockers

None.
