Status: done
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
- 2026-07-06: Implemented the `firn-cli` process-based Python doctor probe with a fixed inline interpreter inspection script and no `firn-python` dependency. Local worker checks passed `cargo fmt --all -- --check`, `cargo test -p firn-cli --locked --no-fail-fast`, and `cargo clippy -p firn-cli --all-targets --locked -- -D warnings`; parent verification/evidence/review remain outside this worker scope.
- 2026-07-06: Added focused parent-review coverage proving `python.require_free_threaded = true` passes when the probe reports `free_threaded_build = true`, `gil_enabled = false`, and `can_parallelize_python = true`. Re-ran minimum worker checks after the test addition.
- 2026-07-06: Added final acceptance-hardening coverage proving a project with a `python://src/events.py#raw_events` resource still invokes the configured interpreter only as `-I -c <fixed inspection snippet>` with probe markers present and resource URI/code markers absent. Re-ran minimum worker checks after the test addition.
- 2026-07-06: Patched actionable mutation-test misses in `firn-cli` tests: exact missing-interpreter branch wording, probe/setup failure details, inconsistent parseable probe JSON metadata, and free-threaded-build-with-GIL-enabled rejection. Re-ran minimum worker checks after the test additions.
- 2026-07-06: Refactored the fake Python probe JSON test helper to use a named-field local struct instead of an eight-argument helper after final clippy review. Re-ran fmt, focused tests, and clippy after the refactor.
- 2026-07-06: Closed with evidence `.10x/evidence/2026-07-06-python-doctor-interpreter-probe.md` and review `.10x/reviews/2026-07-06-python-doctor-interpreter-probe-review.md`. Final checks covered workspace tests, doctests, nextest, clippy, docs, cargo-hack feature powerset, semver against `HEAD`, dependency hygiene, cargo-audit, deny advisories, OSV, Semgrep, Gitleaks, CodeQL using reusable `target/quality/codeql-db-rust`, direct unsafe search, and mutation testing. Remaining limits are external supply-chain policy gates and a non-Unix-only mutation survivor.

## Blockers

None.
